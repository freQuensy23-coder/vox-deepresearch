"""Loopback-only integration tests; no real Vox calls or deployment."""

import http.client
import json
import os
import subprocess
import sys
import urllib.parse
from pathlib import Path
import socket
import struct
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace

import pytest

import vox_adaptive_relay as relay


FINAL = b'{"stop_reason":"finished","messages":[]}\n'


@pytest.mark.parametrize(
    "body,expected",
    [
        (FINAL, "success"),
        (b'{"point":100}\n', "failure"),
        (b'{"messages":[]}\n', "failure"),
        (b'{"stop_reason":"timeout","messages":[]}\n', "failure"),
        (b'{"stop_reason":"finished","messages":[],"partial":true}\n', "failure"),
        (FINAL + b'{"error":"oops"}\n', "failure"),
        (b"{}", "failure"),
        (b'{\n"stop_reason": "finished",\n"messages": []\n}', "success"),
    ],
)
def test_only_complete_search_is_healthy(body, expected):
    probe = relay.CompletionProbe(True)
    for pos in range(0, len(body), 7):
        probe.feed(body[pos : pos + 7])
    assert probe.finish() == expected


def test_large_valid_frame_is_healthy_with_bounded_buffers():
    probe = relay.CompletionProbe(True)
    probe.feed(b'{"messages":[{"text":"')
    for _ in range(100):
        probe.feed(b"x" * 65536)
        assert probe.buffered_bytes <= 259
    probe.feed(b'"}],"stop_reason":"finished"}')
    assert probe.finish() == "success"


def test_only_advancing_semantic_progress_counts():
    seen = []
    probe = relay.CompletionProbe(True, notify=lambda: seen.append(True))
    for body in [
        b'{"point":0}\n',
        b'{"point":10}\n',
        b'{"point":10}\n',
        b"\n",
        b'{"point":5}\n',
    ]:
        probe.feed(body)
    assert len(seen) == 1
    assert probe.finish() == "failure"


@pytest.fixture
def rig(tmp_path):
    state = SimpleNamespace(
        condition=threading.Condition(),
        requests=[],
        active=0,
        maximum=0,
        release=threading.Event(),
        closed=threading.Event(),
        events=[],
        servers=[],
    )

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_POST(self):
            self.do_GET()

        def do_GET(self):
            with state.condition:
                state.requests.append(
                    (
                        self.command,
                        self.path,
                        dict(self.headers),
                        self.rfile.read(int(self.headers.get("Content-Length", 0))),
                    )
                )
                state.active += 1
                state.maximum = max(state.maximum, state.active)
                state.condition.notify_all()
            try:
                mode = self.path.split("query=")[-1].split("&")[0]
                status = (
                    429 if mode == "limited" else 400 if mode == "bad_request" else 200
                )
                self.send_response(status)
                self.send_header("Content-Type", "application/x-ndjson")
                if mode == "limited":
                    self.send_header("Retry-After", "1")
                self.end_headers()
                if mode in {"hold", "cancel", "limited"}:
                    self.wfile.write(b'{"point":1}\n')
                    self.wfile.flush()
                    if mode == "hold":
                        state.release.wait(3)
                    else:
                        while not state.release.wait(0.015):
                            self.wfile.write(b'{"point":1}\n')
                            self.wfile.flush()
                if mode == "warm":
                    time.sleep(0.5)
                if mode == "incomplete":
                    self.wfile.write(b'{"point":100}\n')
                elif mode in {"error", "bad_request"}:
                    self.wfile.write(b'{"error":"failed"}\n')
                elif mode == "cli":
                    self.wfile.write(state.cli_body)
                elif mode == "malformed":
                    self.wfile.write(FINAL + b'{"broken":')
                elif mode == "progress":
                    for i in range(1, 4):
                        self.wfile.write(json.dumps({"point": i * 10}).encode() + b"\n")
                        self.wfile.flush()
                        time.sleep(0.01)
                    self.wfile.write(FINAL)
                else:
                    self.wfile.write(FINAL)
            except (BrokenPipeError, ConnectionResetError):
                state.closed.set()
            finally:
                with state.condition:
                    state.active -= 1
                    state.condition.notify_all()

    class LocalUpstreamServer(ThreadingHTTPServer):
        request_queue_size = 64

    upstream = LocalUpstreamServer(("127.0.0.1", 0), Handler)
    worker = threading.Thread(target=upstream.serve_forever, daemon=True)
    worker.start()

    class Transport(relay.HTTPSUpstream):
        def __init__(self, timeout):
            self.connection = http.client.HTTPConnection(
                *upstream.server_address, timeout=timeout
            )

        def close(self):
            super().close()
            state.events.append("upstream_closed")

    key = tmp_path / "key"
    key.write_text("local-test-key", encoding="utf-8")

    def start(**kwargs):
        options = dict(
            queue_wait=2, socket_idle=2, request_deadline=3, client_timeout=0.3
        )
        options.update(kwargs)
        server = relay.AdaptiveRelayServer(
            ("127.0.0.1", 0),
            key=key.read_text(),
            state_dir=tmp_path / str(len(state.servers)),
            transport_factory=Transport,
            **options,
        )
        original = server.gate.finish

        def finish(*args, **kw):
            state.events.append(("finish", kw))
            return original(*args, **kw)

        server.gate.finish = finish
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        state.servers.append((server, thread))
        return server

    state.start = start
    yield state
    state.release.set()
    for server, thread in state.servers:
        server.shutdown()
        server.server_close()
        thread.join(3)
    upstream.shutdown()
    upstream.server_close()
    worker.join(3)


def request(
    server, path="/search/text?query=ordinary", method="GET", body=None, headers=None
):
    connection = http.client.HTTPConnection(*server.server_address, timeout=5)
    connection.request(method, path, body, headers or {})
    response = connection.getresponse()
    result = response.status, dict(response.getheaders()), response.read()
    connection.close()
    return result


def wait_finished(rig, count=1):
    until = time.monotonic() + 2
    while (
        sum(isinstance(e, tuple) for e in rig.events) < count
        and time.monotonic() < until
    ):
        time.sleep(0.01)
    assert sum(isinstance(e, tuple) for e in rig.events) >= count


def test_stream_is_identical_and_lease_finishes_after_close(rig):
    server = rig.start()
    status, _, body = request(server, "/search/text?query=progress")
    assert status == 200
    assert body == b'{"point": 10}\n{"point": 20}\n{"point": 30}\n' + FINAL
    wait_finished(rig)
    assert rig.events == [
        "upstream_closed",
        ("finish", {"outcome": "success", "transport_closed": True}),
    ]


@pytest.mark.parametrize("mode", ["incomplete", "error"])
def test_http200_does_not_train_on_incomplete_or_error(rig, mode):
    server = rig.start()
    assert request(server, "/search/text?query=" + mode)[0] == 200
    wait_finished(rig)
    assert rig.events[-1][1]["outcome"] == "failure"


def test_authorization_routes_and_post_policy_unchanged(rig):
    server = rig.start()
    assert request(server, "/billing/admin/keys")[0] == 403
    assert not rig.requests
    body = json.dumps(
        {"run_parser": False, "find_new_users": False, "new_users_limit": 0}
    )
    assert (
        request(
            server,
            "/reporter/parse",
            "POST",
            body,
            {"Content-Type": "application/json", "Authorization": "Bearer caller-key"},
        )[0]
        == 200
    )
    assert rig.requests[0][2]["Authorization"] == "Bearer local-test-key"
    assert rig.requests[0][3] == body.encode()


def test_search_and_metadata_have_separate_capacity(rig):
    server = rig.start(
        gate_options={"initial_search": 1, "search_cap": 1, "metadata_cap": 1}
    )
    connection = http.client.HTTPConnection(*server.server_address, timeout=3)
    connection.request("GET", "/search/text?query=hold")
    response = connection.getresponse()
    assert response.readline() == b'{"point":1}\n'
    assert request(server, "/groups/-1001")[0] == 200
    assert rig.maximum == 2
    rig.release.set()
    response.read()
    connection.close()


def test_accepts32clients_when_gate_has_learned_capacity(rig):
    server = rig.start(gate_options={"initial_search": 32, "search_cap": 32})
    # Learn this request shape before a burst; cold shapes intentionally probe once.
    assert request(server, "/search/text?query=warm")[0] == 200
    wait_finished(rig)
    # Explicit learned-state fixture: this checks the HTTP client's capacity,
    # not whether a cold controller learns 32 permits on this workload.
    cohort = relay.request_class("search_text", "/search/text?query=warm")
    with server.gate._transaction() as state:
        state["baselines"][cohort]["control"].update(limit=32, safe=32, probe=False)
    with ThreadPoolExecutor(max_workers=32) as pool:
        futures = [
            pool.submit(request, server, "/search/text?query=hold") for _ in range(32)
        ]
        with rig.condition:
            assert rig.condition.wait_for(lambda: len(rig.requests) == 33, 3)
        rig.release.set()
        assert all(f.result()[0] == 200 for f in futures)
    assert rig.maximum == 32


def test_downstream_cancel_is_neutral_and_timeout_is_failure(rig):
    server = rig.start()
    connection = socket.create_connection(server.server_address, timeout=3)
    connection.sendall(b"GET /search/text?query=cancel HTTP/1.1\r\nHost: local\r\n\r\n")
    assert b"200" in connection.recv(4096)
    connection.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
    connection.close()
    wait_finished(rig)
    assert rig.events[-1][1]["outcome"] == "cancelled"
    next_server = rig.start(request_deadline=0.15)
    assert request(next_server, "/search/text?query=cancel")[0] == 200
    wait_finished(rig, 2)
    assert rig.events[-1][1]["outcome"] == "failure"


def test_retry_after_is_recorded_before_cancelled_body(rig):
    server = rig.start()
    observed = []
    original = server.gate.headers

    def headers(*args, **kwargs):
        result = original(*args, **kwargs)
        observed.append(kwargs)
        return result

    server.gate.headers = headers
    connection = socket.create_connection(server.server_address, timeout=3)
    connection.sendall(
        b"GET /search/text?query=limited HTTP/1.1\r\nHost: local\r\n\r\n"
    )
    assert b"429" in connection.recv(4096)
    assert observed == [{"retry_after": "1"}]
    assert server.gate.retry_after() >= 1
    connection.close()
    wait_finished(rig)
    assert rig.events[-1][1]["outcome"] == "failure"


def test_bounded_cohorts_do_not_contain_query_text():
    assert (
        relay.request_class(
            "search_text",
            "/search/text?query=secret&limit=20&include_groups_ids=-1&from_date=2026-01-01&to_date=2026-01-03",
        )
        == "search_text/single/short/small"
    )
    assert (
        relay.request_class("search_text", "/search/text?query=secret&limit=1000")
        == "search_text/all/open/large"
    )
    assert relay.request_class("group", "/groups/-1") == "group"


def test_transport_close_failure_faults_relay_and_never_releases_lease(rig):
    server = rig.start()
    original = server.transport_factory

    class FaultyClose(original):
        def close(self):
            super().close()
            raise OSError("synthetic close failure")

    server.transport_factory = FaultyClose
    assert request(server)[0] == 200
    wait_finished(rig)
    assert rig.events[-1][1]["transport_closed"] is False
    assert request(server, "/health")[0] == 503
    count = len(rig.requests)
    assert request(server)[0] == 503
    assert len(rig.requests) == count


def test_queued_cancel_never_opens_upstream(rig):
    server = rig.start(gate_options={"initial_search": 1, "search_cap": 1})
    first = http.client.HTTPConnection(*server.server_address, timeout=3)
    first.request("GET", "/search/text?query=hold")
    response = first.getresponse()
    assert response.readline()
    connection = socket.create_connection(server.server_address, timeout=2)
    connection.sendall(b"GET /search/text?query=queued HTTP/1.1\r\nHost: local\r\n\r\n")
    time.sleep(0.05)
    connection.close()
    time.sleep(0.05)
    rig.release.set()
    response.read()
    first.close()
    time.sleep(0.05)
    assert len(rig.requests) == 1


def test_invalid_request_is_not_capacity_failure(rig):
    server = rig.start()
    assert request(server, "/search/text?query=bad_request")[0] == 400
    wait_finished(rig)
    assert rig.events[-1][1]["outcome"] == "neutral"


@pytest.mark.parametrize(
    "body",
    [
        FINAL + b'{"broken":',
        FINAL + b"garbage\n",
        b"garbage\n" + FINAL,
    ],
)
def test_malformed_stream_cannot_train_healthy(body):
    probe = relay.CompletionProbe(True)
    for pos in range(0, len(body), 7):
        probe.feed(body[pos : pos + 7])
    assert probe.finish() == "failure"


def test_malformed_tail_forwarded_unchanged_but_not_healthy(rig):
    server = rig.start()
    status, _, body = request(server, "/search/text?query=malformed")
    assert status == 200 and body == FINAL + b'{"broken":'
    wait_finished(rig)
    assert rig.events[-1][1]["outcome"] == "failure"


def test_only_root_of_pretty_json_can_mark_completion():
    probe = relay.CompletionProbe(True)
    probe.feed(b'{"wrapper": [\n{"stop_reason":"finished","messages":[]}\n]}')
    assert probe.finish() == "failure"


@pytest.mark.parametrize(
    "stream,message_count", [("true", 3), ("false", 3), ("true", 1179)]
)
def test_real_vox_cli_search_page_round_trip(rig, tmp_path, stream, message_count):
    """Execute the shipped CLI, with a process-level guard forbidding external sockets."""
    server = rig.start()
    messages = [
        {
            "message_id": index,
            "group_id": -1001 if index < 3 else -1002,
            "group_alias": "synthetic_chat",
            "user_id": index + 100,
            "text": "Сохранить весь текст. " * (1000 if message_count == 3 else 40),
            "is_private": False,
            "url": f"https://t.me/synthetic_chat/{index}",
            "date": "2026-01-14T12:00:00Z",
        }
        for index in range(1, message_count + 1)
    ]
    terminal = {
        "stop_reason": "finished",
        "messages": messages,
        "count": message_count,
        "stopped_at": "2026-01-01T00:00:00Z",
    }
    encoded = json.dumps(terminal, ensure_ascii=False).encode()
    rig.cli_body = (
        (b'{"point":0}\n{"point":100}\n' if stream == "true" else b"") + encoded + b"\n"
    )
    guard = tmp_path / "sitecustomize.py"
    guard.write_text(
        "import socket\n"
        "original_connect = socket.socket.connect\n"
        "def only_loopback(self, address):\n"
        "    if not isinstance(address, tuple) or address[0] not in ('127.0.0.1', '::1'):\n"
        "        raise AssertionError('external network forbidden in local CLI test')\n"
        "    return original_connect(self, address)\n"
        "socket.socket.connect = only_loopback\n",
        encoding="utf-8",
    )
    correlation = {
        "execution_id": "local-cli",
        "tool_call_id": "local-tool",
        "run_id": "local-run",
        "thread_id": "local-thread",
        "child_call_id": None,
        "tool_attempt": 1,
    }
    env = dict(
        os.environ,
        HOME=str(tmp_path),
        PYTHONPATH=str(tmp_path),
        VOX_API_KEY="dummy-caller-key-must-not-be-forwarded",
        VOX_RELAY_URL=f"http://127.0.0.1:{server.server_port}",
        VOX_TOOL_TRACE_CONTEXT=json.dumps(correlation),
    )
    output = tmp_path / "result.json"
    script = (
        Path(__file__).parents[1] / "skills/public/telegram-research/scripts/vox.py"
    )
    result = subprocess.run(
        [
            sys.executable,
            str(script),
            "search-page",
            "cli",
            *(["--limit", "2"] if message_count == 3 else []),
            "--from",
            "2026-01-01T00:00:00Z",
            "--to",
            "2026-02-01T00:00:00Z",
            "--stream",
            stream,
            "--timeout",
            "10",
            "--no-forwards",
            "--dedup",
            "--type",
            "post",
            "--exclude-users",
            "999",
            "--output",
            str(output),
        ],
        cwd=tmp_path,
        env=env,
        text=True,
        capture_output=True,
        timeout=10,
    )
    assert result.returncode == 0, result.stderr
    assert str(output) in result.stdout
    data = json.loads(output.read_text(encoding="utf-8"))
    assert data["messages"] == messages
    assert (
        data["stop_reason"] == "finished"
        and data["stopped_at"] == terminal["stopped_at"]
    )
    assert data["count"] == data["returned_count"] == message_count
    assert [g["message_count"] for g in data["group_summary"]] == sorted(
        [2, message_count - 2], reverse=True
    )
    assert len(rig.requests) == 1
    method, target, headers, body = rig.requests[0]
    assert method == "GET" and not body
    params = dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(target).query))
    assert params == {
        "query": "cli",
        "limit": "2" if message_count == 3 else "2000",
        "from_date": "2026-01-01T00:00:00Z",
        "to_date": "2026-02-01T00:00:00Z",
        "stream": stream,
        "order": "desc",
        "message_type": "post",
        "timeout": "10",
        "exclude_from_users": "999",
        "exclude_forwards": "true",
        "deduplicate": "true",
    }
    assert headers["Authorization"] == "Bearer local-test-key"
    assert "X-Vox-Tool-Trace" not in headers
    wait_finished(rig)
    traces = list((server.state_dir / "traces").glob("*-end.json"))
    assert len(traces) == 1
    trace = json.loads(traces[0].read_text(encoding="utf-8"))
    assert trace["tool_correlation"] == correlation
    assert (
        server.state_dir / "traces" / trace["body_path"]
    ).read_bytes() == rig.cli_body
    assert trace["body_bytes"] == len(rig.cli_body)
    assert rig.events[-1][1]["outcome"] == "success"
    assert "dummy-caller-key" not in (result.stdout + result.stderr)


def test_controller_records_finish_before_downstream_eof(rig):
    server = rig.start()
    entered, release = threading.Event(), threading.Event()
    original = server.gate.finish

    def delayed_finish(*args, **kwargs):
        entered.set()
        assert release.wait(3)
        return original(*args, **kwargs)

    server.gate.finish = delayed_finish
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(request, server)
        try:
            assert entered.wait(2)
            time.sleep(0.05)
            assert not future.done(), (
                "client saw EOF before controller recorded completion"
            )
        finally:
            release.set()
        assert future.result(timeout=3)[0] == 200
