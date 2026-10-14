import http.client
import importlib.util
import json
import socket
import struct
import subprocess
import sys
import threading
import time
from email.utils import formatdate
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace

import pytest


MODULE = Path(__file__).with_name("vox_relay.py")


def load_relay():
    assert MODULE.exists(), "fixed-upstream relay is missing"
    spec = importlib.util.spec_from_file_location("vox_relay", MODULE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def upstream():
    state = SimpleNamespace(
        condition=threading.Condition(),
        starts=[],
        ends=[],
        requests=[],
        active=0,
        maximum=0,
        release=threading.Event(),
        disconnected=threading.Event(),
        status=200,
        retry_after=None,
    )

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_POST(self):
            self.do_GET()

        def do_GET(self):
            with state.condition:
                index = len(state.starts)
                state.starts.append(time.monotonic())
                state.active += 1
                state.maximum = max(state.maximum, state.active)
                body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
                state.requests.append(
                    (self.command, self.path, dict(self.headers), body)
                )
                state.condition.notify_all()
            try:
                status = state.status if index == 0 else 200
                self.send_response(status)
                self.send_header("Content-Type", "application/x-ndjson")
                if state.retry_after:
                    self.send_header("Retry-After", state.retry_after)
                if status == 302:
                    self.send_header("Location", "https://untrusted.invalid/")
                if "query=truncated" in self.path:
                    self.send_header("Content-Length", "1000")
                self.end_headers()
                if status != 200:
                    self.wfile.write(b'{"error":"synthetic"}\n')
                elif "query=hold" in self.path:
                    self.wfile.write(b'{"point":0}\n')
                    self.wfile.flush()
                    state.release.wait(5)
                    self.wfile.write(b'{"messages":[]}\n')
                elif "query=error200" in self.path:
                    self.wfile.write(b'{"point":0}\n')
                    self.wfile.flush()
                    time.sleep(0.03)
                    self.wfile.write(b'{"error":"synthetic backend failure"}\n')
                elif "query=silent" in self.path:
                    self.wfile.write(b'{"point":0}\n')
                    self.wfile.flush()
                    self.connection.settimeout(3)
                    if self.connection.recv(1) == b"":
                        state.disconnected.set()
                elif "query=cancel" in self.path:
                    while not state.release.wait(0.015):
                        self.wfile.write(b'{"point":0}\n')
                        self.wfile.flush()
                else:
                    self.wfile.write(b'{"messages":[]}\n')
            except (BrokenPipeError, ConnectionResetError):
                state.disconnected.set()
            finally:
                with state.condition:
                    state.active -= 1
                    state.ends.append(time.monotonic())
                    state.condition.notify_all()

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    state.address = server.server_address
    yield state
    state.release.set()
    server.shutdown()
    server.server_close()
    thread.join(2)


@pytest.fixture
def relays(tmp_path, upstream):
    module = load_relay()
    key = tmp_path / "key"
    key.write_text("synthetic-relay-owned-key\n")
    servers = []

    class HTTPTransport(module.HTTPSUpstream):
        def __init__(self, timeout):
            self.connection = http.client.HTTPConnection(
                *upstream.address, timeout=timeout
            )

    def start(**overrides):
        options = dict(
            spacing=0.08,
            cooldown=0.12,
            backoff_cap=1.0,
            queue_wait=0.6,
            request_deadline=3,
            socket_idle=2,
            client_timeout=0.3,
            max_clients=16,
            max_inflight=1,
        )
        options.update(overrides)
        server = module.RelayServer(
            ("127.0.0.1", 0),
            key=key.read_text(),
            state_dir=tmp_path / "state",
            transport_factory=HTTPTransport,
            **options,
        )
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        servers.append((server, thread))
        return server

    yield start
    upstream.release.set()
    for server, thread in servers:
        server.shutdown()
        server.server_close()
        thread.join(2)


def request(
    server,
    path="/search/text?query=ordinary",
    method="GET",
    body=None,
    headers=None,
    timeout=5,
):
    connection = http.client.HTTPConnection(*server.server_address, timeout=timeout)
    connection.request(method, path, body=body, headers=headers or {})
    response = connection.getresponse()
    result = (response.status, dict(response.getheaders()), response.read())
    connection.close()
    return result


def await_starts(upstream, count):
    with upstream.condition:
        assert upstream.condition.wait_for(lambda: len(upstream.starts) >= count, 3)


def test_defaults_are_deployment_limits():
    module = load_relay()
    assert module.DEFAULTS == dict(
        spacing=0.25,
        cooldown=60.0,
        backoff_cap=900.0,
        queue_wait=360.0,
        request_deadline=2100.0,
        socket_idle=1890.0,
        client_timeout=30.0,
        max_clients=16,
        max_inflight=5,
    )
    assert module.UPSTREAM == "api.vox-lab.com"


def test_raw_parallel_clients_hold_slot_until_stream_eof(relays, upstream):
    server = relays()
    first = http.client.HTTPConnection(*server.server_address, timeout=3)
    first.request("GET", "/search/text?query=hold&stream=true")
    response = first.getresponse()
    assert response.readline() == b'{"point":0}\n'
    results = []
    threads = [
        threading.Thread(target=lambda: results.append(request(server)))
        for _ in range(3)
    ]
    for thread in threads:
        thread.start()
    time.sleep(0.18)
    assert len(upstream.starts) == 1
    upstream.release.set()
    assert response.read() == b'{"messages":[]}\n'
    first.close()
    for thread in threads:
        thread.join(3)
    assert len(results) == 3 and all(result[0] == 200 for result in results)
    assert upstream.maximum == 1
    assert all(b - a >= 0.075 for a, b in zip(upstream.starts, upstream.starts[1:]))


def test_relay_owns_key_and_does_not_forward_caller_headers(relays, upstream):
    server = relays()
    status, headers, _ = request(
        server, headers={"Authorization": "Bearer caller-key", "Accept": "*/*"}
    )
    assert status == 200 and headers["Connection"] == "close"
    forwarded = upstream.requests[0][2]
    assert forwarded["Authorization"] == "Bearer synthetic-relay-owned-key"
    assert forwarded["Accept"] == "application/json"
    assert forwarded["Accept-Encoding"] == "identity"


@pytest.mark.parametrize(
    "path,method,headers,body",
    [
        ("https://evil.invalid/search/text", "GET", {}, None),
        ("//evil.invalid/search/text", "GET", {}, None),
        ("api.vox-lab.com:443", "CONNECT", {}, None),
        ("/search/text", "DELETE", {}, None),
        ("/reporter/parse", "GET", {}, None),
        ("/reporter/user/1", "GET", {}, None),
        ("/groups/-1001/../messages", "GET", {}, None),
        ("/groups/%2d1001", "GET", {}, None),
        ("/search/text?query=a&query=b", "GET", {}, None),
        ("/search/text?destination=https://evil.invalid", "GET", {}, None),
        ("/search/text?timeout=601", "GET", {}, None),
        ("/search/text?limit=10001", "GET", {}, None),
        ("/search/text?stream=maybe", "GET", {}, None),
        ("/search/text?query=%0a", "GET", {}, None),
        ("/search/text?query=%zz", "GET", {}, None),
        ("/search/text", "GET", {"X-Upstream": "evil.invalid"}, None),
        ("/search/text", "GET", {"Transfer-Encoding": "chunked"}, None),
        ("/search/text", "GET", {}, b"x"),
        ("/search/text", "POST", {}, b"{}"),
        ("/search/text?query=" + "x" * 8192, "GET", {}, None),
        ("/search/text", "GET", {"Accept": "x" * 17000}, None),
    ],
)
def test_denials_never_open_upstream(relays, upstream, path, method, headers, body):
    status, _, _ = request(relays(), path, method, body, headers)
    assert status in {400, 403, 405, 413, 414, 431}
    assert not upstream.starts


@pytest.mark.parametrize(
    "path",
    [
        "/search/ai/raw/GROUP?query=test&limit=10&offset=0",
        "/groups/-1001",
        "/groups/-1001/messages?messages_limit=10&messages_min=1&members_min=0",
        "/ai_analytics/GROUP/-1001",
        "/ai_analytics/custom/GROUP/-1001?custom_prompt=test",
        "/user_tracker/123/panels/groups",
        "/user_tracker/123/panels/top_groups?limit=10",
        "/user_tracker/123/panels/messages?limit=10",
    ],
)
def test_cli_get_endpoints_are_allowed(relays, path):
    assert request(relays(), path)[0] == 200


@pytest.mark.parametrize(
    "body",
    [
        {
            "groups_search_query": "test",
            "run_parser": False,
            "find_new_users": False,
            "new_users_limit": 0,
        },
    ],
)
def test_safe_post_body_is_allowed(relays, upstream, body):
    assert (
        request(
            relays(),
            "/reporter/parse",
            "POST",
            json.dumps(body),
            {"Content-Type": "application/json"},
        )[0]
        == 200
    )
    assert json.loads(upstream.requests[0][3]) == body


@pytest.mark.parametrize(
    "body",
    [
        {},
        {"run_parser": True, "find_new_users": False, "new_users_limit": 0},
        {"run_parser": False, "find_new_users": True, "new_users_limit": 0},
        {"run_parser": False, "find_new_users": False, "new_users_limit": 1},
        {"run_parser": "false", "find_new_users": False, "new_users_limit": 0},
        {"run_parser": False, "find_new_users": False, "new_users_limit": False},
        {
            "run_parser": False,
            "find_new_users": False,
            "new_users_limit": 0,
            "destination": "evil",
        },
    ],
)
def test_unsafe_post_never_opens_upstream(relays, upstream, body):
    assert (
        request(
            relays(),
            "/reporter/parse",
            "POST",
            json.dumps(body),
            {"Content-Type": "application/json"},
        )[0]
        == 400
    )
    assert not upstream.starts


@pytest.mark.parametrize("retry_after", ["1", "date", "invalid"])
def test_429_persists_cooldown_across_relay_restart(relays, upstream, retry_after):
    upstream.status = 429
    upstream.retry_after = (
        formatdate(time.time() + 2, usegmt=True)
        if retry_after == "date"
        else retry_after
    )
    server = relays(queue_wait=2.5)
    before = time.time()
    assert request(server)[0] == 429
    state = json.loads((server.state_dir / "state.json").read_text())
    expected = 1 if retry_after == "1" else 2 if retry_after == "invalid" else 0.12
    assert state["cooldown_until"] >= before + expected
    assert state["failures"] == 0
    assert state["gap"] == 0.16
    server.shutdown()
    server.server_close()
    next_server = relays(queue_wait=2.5)
    assert request(next_server)[0] == 200
    assert upstream.starts[1] - upstream.starts[0] >= expected - 0.01
    assert "synthetic" not in json.dumps(state)


def test_process_restart_preserves_retry_after(tmp_path, upstream):
    load_relay()
    key = tmp_path / "key"
    key.write_text("synthetic-relay-owned-key")
    state_dir = tmp_path / "process-state"
    code = (
        "import http.client,importlib.util;"
        f"s=importlib.util.spec_from_file_location('relay',{str(MODULE)!r});"
        "m=importlib.util.module_from_spec(s);s.loader.exec_module(m);"
        "T=type('T',(m.HTTPSUpstream,),{'__init__':lambda self,timeout:"
        f"setattr(self,'connection',http.client.HTTPConnection({upstream.address[0]!r},{upstream.address[1]},timeout=timeout))}});"
        f"v=m.RelayServer(('127.0.0.1',0),key={key.read_text()!r},state_dir={str(state_dir)!r},"
        "transport_factory=T,spacing=.05,cooldown=.12,queue_wait=2.5,request_deadline=3,socket_idle=2);"
        "print(v.server_port,flush=True);v.serve_forever()"
    )
    processes = []

    def start():
        process = subprocess.Popen(
            [sys.executable, "-c", code],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        processes.append(process)
        port = int(process.stdout.readline())
        return SimpleNamespace(server_address=("127.0.0.1", port))

    try:
        upstream.status, upstream.retry_after = 429, "1"
        assert request(start())[0] == 429
        processes[0].terminate()
        processes[0].communicate(timeout=3)
        assert request(start())[0] == 200
        assert upstream.starts[1] - upstream.starts[0] >= 0.99
    finally:
        for process in processes:
            if process.poll() is None:
                process.terminate()
            _, err = process.communicate(timeout=3)
            assert "synthetic-relay-owned-key" not in err


def test_queued_request_returns_bounded_busy(relays, upstream):
    server = relays(queue_wait=0.12)
    first = http.client.HTTPConnection(*server.server_address, timeout=3)
    first.request("GET", "/search/text?query=hold")
    response = first.getresponse()
    await_starts(upstream, 1)
    started = time.monotonic()
    status, headers, _ = request(server)
    assert status == 503 and int(headers["Retry-After"]) >= 1
    assert time.monotonic() - started < 0.5
    assert len(upstream.starts) == 1
    upstream.release.set()
    response.read()
    first.close()


def test_client_connection_count_is_bounded(relays, upstream):
    server = relays(max_clients=2, client_timeout=1)
    sockets = [
        socket.create_connection(server.server_address, timeout=2) for _ in range(2)
    ]
    try:
        time.sleep(0.05)
        assert request(server)[0] == 503
        assert not upstream.starts
    finally:
        for connection in sockets:
            connection.close()


def test_disconnect_closes_upstream_before_next_admission(relays, upstream):
    server = relays(cooldown=0.05)
    connection = socket.create_connection(server.server_address, timeout=2)
    connection.sendall(b"GET /search/text?query=cancel HTTP/1.1\r\nHost: relay\r\n\r\n")
    assert b"200" in connection.recv(4096)
    connection.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
    connection.close()
    assert upstream.disconnected.wait(2)
    assert request(server)[0] == 200
    assert upstream.maximum == 1


def test_total_deadline_closes_progress_stream(relays, upstream):
    server = relays(request_deadline=0.15, cooldown=0.05)
    status, _, body = request(server, "/search/text?query=cancel")
    assert status == 200 and b'"point"' in body
    assert upstream.disconnected.wait(2)
    assert request(server)[0] == 200
    assert upstream.maximum == 1


def test_silent_client_disconnect_aborts_upstream(relays, upstream):
    server = relays(cooldown=0.05, queue_wait=0.2)
    connection = socket.create_connection(server.server_address, timeout=2)
    connection.sendall(b"GET /search/text?query=silent HTTP/1.1\r\nHost: relay\r\n\r\n")
    assert b"200" in connection.recv(4096)
    connection.close()
    assert upstream.disconnected.wait(0.4)
    assert request(server)[0] == 200


def test_deadline_aborts_silent_upstream_before_unlock(relays, upstream):
    server = relays(request_deadline=0.15, cooldown=0.05, queue_wait=0.2)
    assert request(server, "/search/text?query=silent")[0] == 200
    assert upstream.disconnected.wait(0.4)
    state = json.loads((server.state_dir / "state.json").read_text())
    assert state["failures"] == 1
    assert request(server)[0] == 200
    assert upstream.maximum == 1


def test_health_and_status_do_not_contact_upstream(relays, upstream):
    server = relays()
    assert json.loads(request(server, "/health")[2]) == {"ok": True}
    state = json.loads(request(server, "/status")[2])
    assert state["requests"] == 0
    assert "key" not in json.dumps(state)
    assert not upstream.starts


def test_legacy_query_only_safe_post_is_allowed(relays, upstream):
    path = "/reporter/parse?groups_search_query=test&run_parser=false&find_new_users=false&new_users_limit=0"
    assert request(relays(), path, "POST")[0] == 200
    assert upstream.requests[0][1] == path


def test_redirect_is_not_followed_or_forwarded(relays, upstream):
    upstream.status = 302
    assert request(relays())[0] == 502
    assert len(upstream.starts) == 1


def test_http_200_backend_error_sets_cooldown(relays, upstream):
    server = relays()
    status, _, body = request(server, "/search/text?query=error200")
    assert status == 200 and b"synthetic backend failure" in body
    state = json.loads((server.state_dir / "state.json").read_text())
    assert state["failures"] == 1 and state["cooldown_until"] > time.time()


def test_truncated_body_sets_cooldown(relays):
    server = relays()
    assert request(server, "/search/text?query=truncated")[0] == 200
    assert json.loads((server.state_dir / "state.json").read_text())["failures"] == 1


def test_real_vox_cli_uses_relay_without_upstream_key(relays, tmp_path, upstream):
    server = relays()
    script = MODULE.parents[1] / "skills/public/telegram-research/scripts/vox.py"
    import os

    env = dict(
        os.environ,
        HOME=str(tmp_path),
        VOX_RELAY_URL=f"http://127.0.0.1:{server.server_port}",
    )
    env.pop("VOX_API_KEY", None)
    result = subprocess.run(
        [
            sys.executable,
            str(script),
            "search-page",
            "ordinary",
            "--limit",
            "10",
            "--from",
            "2026-01-01",
            "--to",
            "2026-02-01",
            "--no-forwards",
            "--dedup",
            "--type",
            "post",
            "--exclude-users",
            "123",
            "--output",
            str(tmp_path / "receipt.json"),
        ],
        cwd=tmp_path,
        env=env,
        text=True,
        capture_output=True,
        timeout=5,
    )
    assert result.returncode == 0, result.stderr
    assert json.loads((tmp_path / "receipt.json").read_text())["messages"] == []
    assert (
        upstream.requests[0][2]["Authorization"] == "Bearer synthetic-relay-owned-key"
    )


def test_retry_after_never_shortens_default_cooldown(tmp_path):
    module = load_relay()
    gate = module.Gate(tmp_path / "cooldown", 5, 60, 900)
    fd = gate.acquire(1)
    try:
        now = time.time()
        gate.finish(True, "1")
        assert gate.read()["cooldown_until"] >= now + 60
        gate.finish(True, "invalid")
        assert gate.read()["cooldown_until"] >= now + 120
    finally:
        gate.release(fd)


def test_logs_do_not_expose_request_data(relays, capsys):
    server = relays()
    assert (
        request(
            server,
            "/search/text?query=private-topic",
            headers={"Authorization": "Bearer private-caller-key"},
        )[0]
        == 200
    )
    out, err = capsys.readouterr()
    assert err == ""
    assert "private-topic" not in out
    assert "private-caller-key" not in out
    assert "Authorization" not in out
    events = [json.loads(line) for line in out.splitlines()]
    assert events
    assert all("request_id" in event and "event" in event for event in events)


def test_state_missing_after_initialization_fails_closed(relays, upstream):
    server = relays()
    (server.state_dir / "state.json").unlink()
    assert request(server)[0] == 503
    assert not upstream.starts


def test_corrupt_state_fails_closed(relays, upstream):
    server = relays()
    (server.state_dir / "state.json").write_text("{}")
    assert request(server)[0] == 503
    assert not upstream.starts
