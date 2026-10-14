"""Cancellable admission regressions using loopback HTTP and fake CLI transports."""

import http.client
import io
import importlib.util
from pathlib import Path
import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
import adaptive_gate
import vox_adaptive_relay as relay

cli_path = Path(__file__).with_name("vox.py")
if not cli_path.exists():
    cli_path = (
        Path(__file__).parents[1] / "skills/public/telegram-research/scripts/vox.py"
    )
cli_spec = importlib.util.spec_from_file_location("queue_wait_vox_cli", cli_path)
vox = importlib.util.module_from_spec(cli_spec)
cli_spec.loader.exec_module(vox)

FINAL = b'{"stop_reason":"finished","messages":[]}\n'


def test_none_wait_outlives_old_120_and_releases_pending(tmp_path):
    now = [1000.0]
    holder = []

    def sleep(_):
        now[0] += 130
        gate.finish(holder.pop(), "cancelled")

    gate = adaptive_gate.AdaptiveGate(
        tmp_path,
        search_cap=1,
        initial_search=1,
        clock=lambda: now[0],
        monotonic=lambda: now[0],
        sleep=sleep,
    )
    holder.append(gate.acquire(0))
    lease = gate.acquire(None)
    assert now[0] == 1130
    assert gate.read()["lanes"]["search"]["queued"] == 0
    gate.finish(lease, "cancelled")


def test_none_wait_honors_caller_cancel(tmp_path):
    now = [1000.0]
    gate = adaptive_gate.AdaptiveGate(
        tmp_path,
        search_cap=1,
        initial_search=1,
        clock=lambda: now[0],
        monotonic=lambda: now[0],
        sleep=lambda _: now.__setitem__(0, now[0] + 130),
    )
    held = gate.acquire(0)
    with pytest.raises(adaptive_gate.Cancelled):
        gate.acquire(None, cancelled=lambda: now[0] >= 1130)
    assert gate.read()["lanes"]["search"]["queued"] == 0
    assert gate.read()["outcomes"]["failure"] == 0
    gate.finish(held, "cancelled")


def test_explicit_wait_budget_is_still_enforced(tmp_path):
    now = [1000.0]
    gate = adaptive_gate.AdaptiveGate(
        tmp_path,
        search_cap=1,
        initial_search=1,
        clock=lambda: now[0],
        monotonic=lambda: now[0],
        sleep=lambda amount: now.__setitem__(0, now[0] + amount),
    )
    held = gate.acquire(0)
    with pytest.raises(adaptive_gate.Busy):
        gate.acquire(0.05)
    assert gate.read()["lanes"]["search"]["queued"] == 0
    gate.finish(held, "cancelled")


@pytest.mark.parametrize("wait", [-1, True, float("inf"), "none"])
def test_invalid_wait_not_silently_unbounded(tmp_path, wait):
    gate = adaptive_gate.AdaptiveGate(tmp_path)
    with pytest.raises(ValueError):
        gate.acquire(wait)


def test_cli_relay_mode_has_no_idle_cutoff(monkeypatch):
    monkeypatch.setenv("VOX_RELAY_URL", "http://127.0.0.1:12345")
    seen = []

    class FakeOpener:
        def open(self, request, timeout):
            seen.append(timeout)
            return io.BytesIO(FINAL)

    monkeypatch.setattr(vox, "RELAY_OPENER", FakeOpener())
    assert (
        vox.call(
            "/search/text",
            {"query": "local", "timeout": 600, "stream": True},
        )["stop_reason"]
        == "finished"
    )
    assert seen == [None]


def test_cli_requires_relay(monkeypatch):
    monkeypatch.delenv("VOX_RELAY_URL", raising=False)
    with pytest.raises(vox.VoxError, match="VOX_RELAY_URL"):
        vox.call("/groups/1")


@pytest.fixture
def local_server(tmp_path):
    starts = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_GET(self):
            starts.append(time.monotonic())
            self.send_response(200)
            self.send_header("Content-Length", str(len(FINAL)))
            self.end_headers()
            self.wfile.write(FINAL)

    upstream = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=upstream.serve_forever, daemon=True)
    thread.start()

    class Transport(relay.HTTPSUpstream):
        def __init__(self, timeout):
            self.connection = http.client.HTTPConnection(
                *upstream.server_address, timeout=timeout
            )

    key = tmp_path / "key"
    key.write_text("dummy-local-key", encoding="utf-8")
    running = []

    def start(**options):
        server = relay.AdaptiveRelayServer(
            ("127.0.0.1", 0),
            key_file=key,
            state_dir=tmp_path / str(len(running)),
            transport_factory=Transport,
            gate_options={"search_cap": 1, "initial_search": 1},
            **options,
        )
        worker = threading.Thread(target=server.serve_forever, daemon=True)
        worker.start()
        running.append((server, worker))
        return server

    yield start, starts
    for server, worker in running:
        server.shutdown()
        server.server_close()
        worker.join()
    upstream.shutdown()
    upstream.server_close()
    thread.join()


def request(server):
    connection = http.client.HTTPConnection(*server.server_address, timeout=3)
    try:
        connection.request("GET", "/search/text?query=local")
        response = connection.getresponse()
        return response.status, response.read()
    finally:
        connection.close()


def hold(server):
    return server.gate.acquire(
        0, request_class=relay.request_class("search_text", "/search/text?query=local")
    )


def test_queue_can_outlive_upstream_deadline_without_false_503(local_server):
    start, starts = local_server
    server = start(request_deadline=0.12, socket_idle=0.12)
    assert server.options["queue_wait"] is None
    lease = hold(server)
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(request, server)
        time.sleep(0.25)
        assert not future.done() and not starts
        server.gate.finish(lease, "cancelled")
        status, body = future.result(3)
    assert status == 200 and body == FINAL
    assert len(starts) == 1


def test_explicit_http_queue_budget_returns_503_without_upstream(local_server):
    start, starts = local_server
    server = start(queue_wait=0.05)
    lease = hold(server)
    try:
        assert request(server)[0] == 503
        assert not starts
    finally:
        server.gate.finish(lease, "cancelled")


def test_http_unbounded_queue_cancels_on_disconnect(local_server):
    start, starts = local_server
    server = start()
    lease = hold(server)
    connection = socket.create_connection(server.server_address, timeout=2)
    connection.sendall(
        b"GET /search/text?query=local HTTP/1.1\r\nHost: localhost\r\n\r\n"
    )
    until = time.monotonic() + 1
    while (
        server.gate.read()["lanes"]["search"]["queued"] == 0
        and time.monotonic() < until
    ):
        time.sleep(0.01)
    assert server.gate.read()["lanes"]["search"]["queued"] == 1
    connection.close()
    until = time.monotonic() + 1
    while server.gate.read()["lanes"]["search"]["queued"] and time.monotonic() < until:
        time.sleep(0.01)
    assert server.gate.read()["lanes"]["search"]["queued"] == 0
    assert not starts
    assert server.gate.read()["outcomes"]["failure"] == 0
    server.gate.finish(lease, "cancelled")


def test_unbounded_wait_does_not_hang_after_transport_fault(tmp_path):
    gate = adaptive_gate.AdaptiveGate(tmp_path, search_cap=1, initial_search=1)
    held = gate.acquire(0)
    gate.finish(held, transport_closed=False)
    with pytest.raises(adaptive_gate.Busy, match="faulted"):
        gate.acquire(None)
    assert gate.read()["lanes"]["search"]["queued"] == 0


def test_already_cancelled_request_never_acquires_free_slot(tmp_path):
    gate = adaptive_gate.AdaptiveGate(tmp_path)
    with pytest.raises(adaptive_gate.Cancelled):
        gate.acquire(None, cancelled=lambda: True)
    state = gate.read()
    assert state["requests"] == 0
    assert state["lanes"]["search"]["active"] == 0
    assert state["lanes"]["search"]["queued"] == 0
