import importlib.util
import io
import json
import os
import subprocess
import sys
import threading
import time
import urllib.error
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit

import pytest

SCRIPT = (
    Path(__file__).resolve().parents[2]
    / "skills/public/telegram-research/scripts/vox.py"
)
spec = importlib.util.spec_from_file_location("admission_vox", SCRIPT)
vox = importlib.util.module_from_spec(spec)
spec.loader.exec_module(vox)


@pytest.fixture
def local_server():
    state = SimpleNamespace(
        condition=threading.Condition(),
        starts=[],
        ends=[],
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

        def do_GET(self):
            query = parse_qs(urlsplit(self.path).query)
            mode = query.get("query", [""])[0]
            with state.condition:
                index = len(state.starts)
                state.starts.append(time.monotonic())
                state.active += 1
                state.maximum = max(state.maximum, state.active)
                state.condition.notify_all()
            try:
                status = state.status if index == 0 else 200
                self.send_response(status)
                if status != 200 and state.retry_after is not None:
                    self.send_header("Retry-After", state.retry_after)
                self.end_headers()
                if status != 200:
                    self.wfile.write(b"synthetic-private-payload")
                    return
                if mode == "hold":
                    if query.get("stream") == ["true"]:
                        self.wfile.write(b'{"point":0}\n')
                    else:
                        self.wfile.write(b'{"messages":')
                    self.wfile.flush()
                    state.release.wait(20)
                    body = (
                        b'{"messages":[],"count":0}\n'
                        if query.get("stream") == ["true"]
                        else b'[],"count":0}'
                    )
                    self.wfile.write(body)
                elif mode == "cancel":
                    while not state.release.wait(0.03):
                        self.wfile.write(b'{"point":0}\n')
                        self.wfile.flush()
                else:
                    self.wfile.write(b'{"messages":[],"count":0}\n')
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
    state.url = f"http://127.0.0.1:{server.server_port}"
    yield state
    state.release.set()
    server.shutdown()
    server.server_close()
    thread.join(3)


@pytest.fixture
def clock_transport(monkeypatch, tmp_path):
    clock = SimpleNamespace(
        now=1800000000.0, sleeps=[], requests=[], responses=[], opened=[]
    )

    def sleep(seconds):
        clock.sleeps.append(seconds)
        clock.now += seconds

    def open_request(req, timeout):
        clock.requests.append((clock.now, req.full_url))
        response = clock.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        stream = io.BytesIO(response)
        clock.opened.append(stream)
        return stream

    monkeypatch.setenv("VOX_API_KEY", "synthetic-test-key")
    monkeypatch.setattr(
        vox,
        "time",
        SimpleNamespace(
            time=lambda: clock.now, monotonic=lambda: clock.now, sleep=sleep
        ),
    )
    monkeypatch.setattr(vox.OPENER, "open", open_request)
    monkeypatch.setattr(vox.RELAY_OPENER, "open", open_request)
    return clock


def http_error(code, retry_after=None):
    headers = {} if retry_after is None else {"Retry-After": retry_after}
    return urllib.error.HTTPError(
        "https://not-called.invalid",
        code,
        "failure",
        headers,
        io.BytesIO(b"private-payload"),
    )


@pytest.mark.parametrize(
    "base",
    [
        "http://vox-relay:8083",
        "http://localhost:18089",
        "http://127.0.0.1:18089",
        "http://[::1]:18089/",
    ],
)
def test_relay_mode_has_no_key_no_local_admission_and_no_authorization(
    clock_transport, monkeypatch, tmp_path, base
):
    clock = clock_transport
    monkeypatch.setenv("VOX_RELAY_URL", base)
    monkeypatch.delenv("VOX_API_KEY")
    monkeypatch.setattr(vox, "KEY_FILE", tmp_path / "missing-key")
    clock.responses.append(b'{"messages":[]}')
    headers = []
    open_request = vox.OPENER.open

    def capture(req, timeout):
        headers.append(dict(req.header_items()))
        return open_request(req, timeout)

    monkeypatch.setattr(vox.RELAY_OPENER, "open", capture)
    assert vox.call("/search/text", {"stream": True}) == {"messages": []}
    assert clock.requests[0][1] == base.rstrip("/") + "/search/text?stream=true"
    assert all(key.lower() != "authorization" for key in headers[0])
    assert not (tmp_path / "shared").exists()


@pytest.mark.parametrize(
    "base",
    [
        "",
        "https://vox-relay:8083",
        "http://api.vox-lab.com",
        "http://example.com",
        "http://vox-relay.example.com:8083",
        "http://user:secret@vox-relay:8083",
        "http://vox-relay:8083?secret=query",
        "http://vox-relay:8083#fragment",
        "http://vox-relay:8083/path",
        "http://vox-relay:bad",
        "http://vox-relay:65536",
        " http://vox-relay:8083",
        "http://vox-relay\n:8083",
    ],
)
def test_relay_url_rejects_unsafe_bases_without_network_or_secret_echo(
    clock_transport, monkeypatch, base
):
    monkeypatch.setenv("VOX_RELAY_URL", base)
    clock_transport.responses.append(b"{}")
    with pytest.raises(vox.VoxError, match="VOX_RELAY_URL") as error:
        vox.call("/search/text")
    assert "secret" not in str(error.value)
    assert not clock_transport.requests


def test_relay_failure_is_not_locally_retried_or_cooled_down(
    clock_transport, monkeypatch, tmp_path
):
    monkeypatch.setenv("VOX_RELAY_URL", "http://vox-relay:8083")
    clock_transport.responses.extend([http_error(429, "120"), b"{}"])
    with pytest.raises(vox.VoxError, match="429"):
        vox.call("/search/text")
    assert len(clock_transport.requests) == 1
    assert not clock_transport.sleeps
    assert not (tmp_path / "shared").exists()


def test_redirect_error_closes_unreturned_response(clock_transport):
    body = io.BytesIO(b"synthetic-redirect-body")
    request = vox.urllib.request.Request("https://not-called.invalid")
    with pytest.raises(vox.VoxError, match="refusing"):
        vox.NoRedirect().redirect_request(
            request, body, 302, "Found", {}, "https://not-called.invalid/other"
        )
    assert body.closed


def test_real_relay_cli_needs_no_key_and_ignores_external_proxy(local_server, tmp_path):
    env = dict(
        os.environ,
        HOME=str(tmp_path),
        VOX_RELAY_URL=local_server.url,
        HTTP_PROXY="http://127.0.0.1:9",
        http_proxy="http://127.0.0.1:9",
        NO_PROXY="",
        no_proxy="",
    )
    env.pop("VOX_API_KEY", None)
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "search-page", "ordinary"],
        env=env,
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=5,
    )
    assert result.returncode == 0, result.stderr
    path = Path(result.stdout.strip())
    assert path.is_absolute() and json.loads(path.read_text())["messages"] == []
    assert len(local_server.starts) == 1
    assert not (tmp_path / "unused-gate").exists()


def test_direct_calls_can_overlap_without_a_hidden_global_gate(monkeypatch):
    from concurrent.futures import ThreadPoolExecutor

    barrier = threading.Barrier(2)
    monkeypatch.delenv("VOX_RELAY_URL", raising=False)
    monkeypatch.setenv("VOX_API_KEY", "synthetic-test-key")

    def open_request(req, timeout):
        barrier.wait(timeout=3)
        return io.BytesIO(b'{"messages":[]}')

    monkeypatch.setattr(vox.OPENER, "open", open_request)
    with ThreadPoolExecutor(max_workers=2) as workers:
        futures = [workers.submit(vox.call, "/search/text") for _ in range(2)]
        assert [future.result(timeout=5) for future in futures] == [
            {"messages": []},
            {"messages": []},
        ]
