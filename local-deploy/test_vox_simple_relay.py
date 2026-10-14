"""HTTP contract tests: real relay and real synthetic upstream sockets."""

import http.client
import json
import socket
import threading
import time
from collections import Counter
from email.utils import formatdate
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit

import pytest
import vox_relay as relay


@pytest.fixture
def stack(tmp_path):
    state = SimpleNamespace(
        condition=threading.Condition(),
        calls=[],
        active=0,
        maximum=0,
        release=threading.Event(),
        counts=Counter(),
    )

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *args):
            pass

        def do_GET(self):
            values = parse_qs(urlsplit(self.path).query)
            query = values.get("query", ["ordinary"])[0]
            with state.condition:
                state.calls.append(
                    (query, time.monotonic(), self.path, dict(self.headers))
                )
                state.counts[query] += 1
                attempt = state.counts[query]
                state.active += 1
                state.maximum = max(state.maximum, state.active)
                state.condition.notify_all()
            try:
                if query in {"hold", "slow", "slow2"}:
                    self.connection.settimeout(0.02)
                    while not state.release.is_set():
                        try:
                            if self.connection.recv(1, socket.MSG_PEEK) == b"":
                                return
                        except socket.timeout:
                            pass
                if query == "disconnect" and attempt == 1:
                    self.connection.shutdown(socket.SHUT_RDWR)
                    return
                error = query == "permanent" or query == "transient" and attempt == 1
                limited = query.startswith("limited") and attempt == 1
                status = 429 if limited else 503 if error else 200
                payload = (
                    {"error": "PRIVATE VOX FAILURE"}
                    if error or limited
                    else {"messages": [], "stop_reason": "finished"}
                )
                if (
                    query in {"partial", "error200"}
                    and values.get("stream", ["true"])[0] != "false"
                ):
                    payload = (
                        {"progress": 1}
                        if query == "partial"
                        else {"error": "PRIVATE VOX FAILURE"}
                    )
                body = json.dumps(payload).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header(
                    "Content-Length",
                    str(
                        len(body) + 20
                        if query == "truncated" and attempt == 1
                        else len(body)
                    ),
                )
                if limited:
                    self.send_header(
                        "Retry-After",
                        "0.15"
                        if query == "limited"
                        else formatdate(time.time() + 2, usegmt=True)
                        if query == "limited-date"
                        else "60",
                    )
                if error:
                    self.send_header("X-Private-Vox", "hidden")
                self.end_headers()
                self.wfile.write(body)
                if query == "truncated" and attempt == 1:
                    self.wfile.flush()
                    self.connection.shutdown(socket.SHUT_RDWR)
            except (OSError, BrokenPipeError):
                pass
            finally:
                with state.condition:
                    state.active -= 1
                    state.condition.notify_all()

    upstream = ThreadingHTTPServer(("127.0.0.1", 0), Handler)

    class Transport(relay.HTTPSUpstream):
        def __init__(self, timeout):
            self.connection = http.client.HTTPConnection(
                *upstream.server_address, timeout=timeout
            )

    key = tmp_path / "key"
    key.write_text("synthetic-key", encoding="utf-8")
    server = relay.RelayServer(
        ("127.0.0.1", 0),
        key_file=key,
        state_dir=tmp_path / "state",
        transport_factory=Transport,
    )
    threads = [
        threading.Thread(target=s.serve_forever, daemon=True)
        for s in (upstream, server)
    ]
    for thread in threads:
        thread.start()
    state.server = server
    state.upstream_address = upstream.server_address
    yield state
    state.release.set()
    for s in (server, upstream):
        s.shutdown()
        s.server_close()
    for thread in threads:
        thread.join(2)


def fetch(state, query, *, path=None):
    connection = http.client.HTTPConnection(*state.server.server_address, timeout=25)
    connection.request(
        "GET",
        path
        or "/search/text?query="
        + query
        + "&from_date=2026-01-01&to_date=2026-10-14&stream=true",
    )
    response = connection.getresponse()
    result = response.status, dict(response.getheaders()), response.read()
    connection.close()
    return result


def await_calls(state, count):
    with state.condition:
        assert state.condition.wait_for(lambda: len(state.calls) >= count, 3)


def test_fifo_32_callers_one_upstream_and_no_spacing(stack):
    results = []
    workers = []
    first = threading.Thread(target=lambda: results.append(fetch(stack, "hold")))
    first.start()
    await_calls(stack, 1)
    for index in range(31):
        thread = threading.Thread(
            target=lambda i=index: results.append(fetch(stack, str(i)))
        )
        thread.start()
        workers.append(thread)
        with stack.server.gate.condition:
            assert stack.server.gate.condition.wait_for(
                lambda: len(stack.server.gate.waiting) == index + 1, 2
            )
    assert len(stack.calls) == 1
    stack.release.set()
    for thread in [first, *workers]:
        thread.join(5)
        assert not thread.is_alive()
    assert [row[0] for row in stack.calls] == ["hold", *map(str, range(31))]
    assert len(results) == 32 and all(result[0] == 200 for result in results)
    assert stack.maximum == 1
    assert stack.calls[-1][1] - stack.calls[1][1] < 2


@pytest.mark.parametrize(
    "query",
    [
        "transient",
        "limited",
        "limited-date",
        "disconnect",
        "partial",
        "error200",
        "truncated",
    ],
)
def test_upstream_errors_and_partial_stream_never_escape(stack, query):
    result = fetch(stack, query)
    assert result[0] == 200
    assert b"PRIVATE" not in result[2]
    assert json.loads(result[2])["stop_reason"] == "finished"
    assert len(stack.calls) == 2
    if query == "limited":
        assert stack.calls[1][1] - stack.calls[0][1] >= 0.145
    if query == "limited-date":
        assert stack.calls[1][1] - stack.calls[0][1] >= 0.8
    if query in {"partial", "error200"}:
        assert "stream=false" in stack.calls[1][2]
        assert "from_date=2026-01-01" in stack.calls[1][2]
        assert "to_date=2026-10-14" in stack.calls[1][2]
    assert all(
        call[3]["Authorization"] == "Bearer synthetic-key" for call in stack.calls
    )
    assert "Retry-After" not in result[1]


@pytest.mark.parametrize("query", ["permanent", "limited-long"])
def test_retries_share_one_deadline_and_hide_upstream_error(stack, monkeypatch, query):
    monkeypatch.setattr(relay, "REQUEST_DEADLINE", 0.4)
    started = time.monotonic()
    status, headers, body = fetch(stack, query)
    assert status == 504
    assert 0.35 <= time.monotonic() - started < 1
    assert json.loads(body)["error"] == "request_timeout"
    assert "date interval" in json.loads(body)["message"]
    assert "X-Private-Vox" not in headers and b"PRIVATE" not in body
    if query == "limited-long":
        assert len(stack.calls) == 1
    assert fetch(stack, "ordinary")[0] == 200


def test_production_ten_seconds_queue_wait_separate_and_abort_releases_slot(stack):
    assert relay.REQUEST_DEADLINE == 10
    results = []
    started = time.monotonic()
    worker = threading.Thread(target=lambda: results.append(fetch(stack, "slow")))
    worker.start()
    await_calls(stack, 1)
    queued = threading.Thread(target=lambda: results.append(fetch(stack, "ordinary")))
    queued.start()
    worker.join(12)
    queued.join(3)
    assert not worker.is_alive() and not queued.is_alive()
    assert 9.8 <= time.monotonic() - started < 12
    assert sorted(result[0] for result in results) == [200, 504]
    assert stack.calls[1][1] - stack.calls[0][1] >= 9.8
    assert stack.maximum == 1


def test_cancelled_queue_entry_does_not_block_following_request(stack):
    results = []
    worker = threading.Thread(target=lambda: results.append(fetch(stack, "hold")))
    worker.start()
    await_calls(stack, 1)
    cancelled = http.client.HTTPConnection(*stack.server.server_address, timeout=3)
    cancelled.request("GET", "/search/text?query=cancelled")
    with stack.server.gate.condition:
        assert stack.server.gate.condition.wait_for(
            lambda: len(stack.server.gate.waiting) == 1, 2
        )
    cancelled.close()
    with stack.server.gate.condition:
        assert stack.server.gate.condition.wait_for(
            lambda: len(stack.server.gate.waiting) == 0, 2
        )
    stack.release.set()
    worker.join(3)
    assert fetch(stack, "ordinary")[0] == 200
    assert [row[0] for row in stack.calls] == ["hold", "ordinary"]


@pytest.mark.parametrize("command", ["search-page", "search"])
def test_real_cli_receives_proxy_timeout_date_advice(
    stack, monkeypatch, tmp_path, command
):
    import os
    import subprocess
    import sys
    from pathlib import Path

    monkeypatch.setattr(relay, "REQUEST_DEADLINE", 0.2)
    script = (
        Path(__file__).resolve().parents[1]
        / "skills/public/telegram-research/scripts/vox.py"
    )
    result = subprocess.run(
        [
            sys.executable,
            str(script),
            command,
            "permanent",
            "--from",
            "2026-01-01",
            "--to",
            "2026-10-14",
        ],
        env={
            **os.environ,
            "HOME": str(tmp_path),
            "VOX_RELAY_URL": f"http://127.0.0.1:{stack.server.server_port}",
        },
        capture_output=True,
        text=True,
        cwd=tmp_path,
        timeout=3,
    )
    assert result.returncode != 0
    assert "Reduce the date interval" in result.stderr
    assert "PRIVATE VOX FAILURE" not in result.stderr


def test_waiting_longer_than_execution_budget_still_gets_own_budget(stack, monkeypatch):
    monkeypatch.setattr(relay, "REQUEST_DEADLINE", 0.3)
    results = []
    workers = []
    for query in ["slow", "slow2", "ordinary"]:
        worker = threading.Thread(
            target=lambda q=query: results.append((q, fetch(stack, q)))
        )
        worker.start()
        workers.append(worker)
        if query == "slow":
            await_calls(stack, 1)
        else:
            expected = 1 if query == "slow2" else 2
            with stack.server.gate.condition:
                assert stack.server.gate.condition.wait_for(
                    lambda: len(stack.server.gate.waiting) == expected, 2
                )
    for worker in workers:
        worker.join(3)
        assert not worker.is_alive()
    assert {q: r[0] for q, r in results} == {"slow": 504, "slow2": 504, "ordinary": 200}
    assert stack.calls[2][1] - stack.calls[0][1] >= 0.55
    assert stack.maximum == 1


def test_active_disconnect_aborts_upstream_before_next_admission(stack):
    cancelled = http.client.HTTPConnection(*stack.server.server_address, timeout=3)
    cancelled.request("GET", "/search/text?query=hold")
    await_calls(stack, 1)
    results = []
    worker = threading.Thread(target=lambda: results.append(fetch(stack, "ordinary")))
    worker.start()
    with stack.server.gate.condition:
        assert stack.server.gate.condition.wait_for(
            lambda: len(stack.server.gate.waiting) == 1, 2
        )
    cancelled.close()
    worker.join(3)
    assert not worker.is_alive()
    assert results[0][0] == 200
    assert stack.maximum == 1


def test_dns_wait_cannot_extend_total_deadline_or_start_late_vox_request(
    stack, monkeypatch
):
    monkeypatch.setattr(relay, "REQUEST_DEADLINE", 0.2)
    original = socket.getaddrinfo
    pending = threading.Event()
    released = threading.Event()

    def slow_dns(host, port, *args, **kwargs):
        if host == relay.UPSTREAM:
            pending.set()
            released.wait(3)
            raise socket.gaierror("synthetic DNS failure")
        return original(host, port, *args, **kwargs)

    monkeypatch.setattr(socket, "getaddrinfo", slow_dns)
    stack.server.transport_factory = relay.HTTPSUpstream
    try:
        start = time.monotonic()
        assert fetch(stack, "ordinary")[0] == 504
        assert time.monotonic() - start < 0.5
        assert pending.is_set()
        assert not stack.calls
    finally:
        released.set()


def test_tls_handshake_uses_total_deadline_and_closes_before_next_request(
    stack, monkeypatch
):
    monkeypatch.setattr(relay, "REQUEST_DEADLINE", 0.2)
    original = socket.getaddrinfo

    def local_address(host, port, *args, **kwargs):
        if host == relay.UPSTREAM:
            return original(*stack.upstream_address, *args, **kwargs)
        return original(host, port, *args, **kwargs)

    monkeypatch.setattr(socket, "getaddrinfo", local_address)
    stack.server.transport_factory = relay.HTTPSUpstream
    started = time.monotonic()
    assert fetch(stack, "ordinary")[0] == 504
    assert time.monotonic() - started < 0.5
    assert not stack.calls
