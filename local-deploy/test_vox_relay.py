import http.client
import importlib.util
import json
import subprocess
import sys
import threading
import time
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
                    self.wfile.write(b'{"messages":[],"stop_reason":"finished"}\n')
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
                    self.wfile.write(b'{"messages":[],"stop_reason":"finished"}\n')
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
        server = module.RelayServer(
            ("127.0.0.1", 0),
            key=key.read_text(),
            state_dir=tmp_path / "state",
            transport_factory=HTTPTransport,
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


def test_only_search_and_whale_messages_reach_vox(relays, upstream):
    server = relays()
    assert request(server, "/search/text?query=test")[0] == 200
    assert request(server, "/user_tracker/123/panels/messages?limit=100000")[0] == 200
    for path in (
        "/search/ai/raw/GROUP?query=test&limit=10",
        "/groups/-1001",
        "/groups/-1001/messages?messages_limit=10",
        "/ai_analytics/GROUP/-1001",
        "/user_tracker/123/panels/groups",
        "/user_tracker/123/panels/top_groups?limit=10",
        "/user_tracker/123/panels/messages?limit=100001",
    ):
        assert request(server, path)[0] in {400, 403}, path
    assert request(server, "/reporter/parse", "POST", b"{}", {"Content-Type": "application/json"})[0] == 405
    assert len(upstream.starts) == 2


def test_health_and_status_do_not_contact_upstream(relays, upstream):
    server = relays()
    assert json.loads(request(server, "/health")[2]) == {"ok": True}
    state = json.loads(request(server, "/status")[2])
    assert state["active"] == 0 and state["queued"] == 0
    assert "key" not in json.dumps(state)
    assert not upstream.starts


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
            "search",
            "ordinary",
            "--since",
            "2026-01-01",
            "--until",
            "2026-02-01",
        ],
        cwd=tmp_path,
        env=env,
        text=True,
        capture_output=True,
        timeout=5,
    )
    assert result.returncode == 0, result.stderr
    assert json.loads((tmp_path / "messages_raw.json").read_text())["messages"] == []
    assert (
        upstream.requests[0][2]["Authorization"] == "Bearer synthetic-relay-owned-key"
    )
