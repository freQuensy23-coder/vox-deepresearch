import fcntl
import importlib.util
import io
import json
import os
import signal
import subprocess
import sys
import threading
import time
import urllib.error
from datetime import datetime, timezone
from email.utils import format_datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "skills/public/telegram-research/scripts/vox.py"
spec = importlib.util.spec_from_file_location("admission_vox", SCRIPT)
vox = importlib.util.module_from_spec(spec)
spec.loader.exec_module(vox)


@pytest.fixture
def local_server():
    state = SimpleNamespace(
        condition=threading.Condition(), starts=[], ends=[], active=0, maximum=0,
        release=threading.Event(), disconnected=threading.Event(), status=200,
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
                    body = b'{"messages":[],"count":0}\n' if query.get("stream") == ["true"] else b'[],"count":0}'
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
def clients(tmp_path, local_server):
    processes = []

    def launch(query="ordinary", attempts=1, stream="true", gate=None):
        home = tmp_path / f"sandbox-{len(processes)}"
        home.mkdir()
        env = dict(os.environ, HOME=str(home), VOX_API_KEY="synthetic-test-key",
                   VOX_ADMISSION_DIR=str(gate if gate is not None else tmp_path / "shared"))
        code = (
            "import importlib.util,sys,urllib.request;"
            f"s=importlib.util.spec_from_file_location('vox_cli',{str(SCRIPT)!r});"
            "v=importlib.util.module_from_spec(s);s.loader.exec_module(v);"
            f"v.BASE={local_server.url!r};"
            "v.OPENER=urllib.request.build_opener(urllib.request.ProxyHandler({}),v.NoRedirect());"
            "v.main(sys.argv[1:])"
        )
        process = subprocess.Popen(
            [sys.executable, "-c", code, "--attempts", str(attempts), "search-page", query, "--stream", stream],
            env=env, cwd=home, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        processes.append(process)
        return process

    yield launch
    for process in processes:
        if process.poll() is None:
            process.kill()
        process.communicate(timeout=5)


def await_starts(server, count, timeout=10):
    with server.condition:
        assert server.condition.wait_for(lambda: len(server.starts) >= count, timeout)


def assert_receipt(process, timeout=15):
    out, err = process.communicate(timeout=timeout)
    assert process.returncode == 0, err
    assert len(out.splitlines()) == 1
    path = Path(out.strip())
    assert path.is_absolute()
    assert json.loads(path.read_text())["messages"] == []
    assert "synthetic-test-key" not in err


@pytest.mark.parametrize("stream", ["true", "false"])
def test_independent_processes_serialize_entire_body_and_space_starts(clients, local_server, stream):
    first = clients("hold", stream=stream)
    await_starts(local_server, 1)
    second = clients(stream=stream)
    third = clients(stream=stream)
    time.sleep(5.3)
    assert len(local_server.starts) == 1
    assert second.poll() is None and third.poll() is None
    local_server.release.set()
    for process in (first, second, third):
        assert_receipt(process)
    assert local_server.maximum == 1
    assert len(local_server.starts) == 3
    assert all(b - a >= 4.95 for a, b in zip(local_server.starts, local_server.starts[1:]))


def test_429_cooldown_is_shared_across_sandbox_homes(clients, local_server, tmp_path):
    local_server.status = 429
    local_server.retry_after = "61"
    first = clients()
    out, err = first.communicate(timeout=5)
    assert first.returncode == 1 and out == "" and "429" in err
    status = json.loads((tmp_path / "shared/status.json").read_text())
    assert status["cooldown_until"] - status["last_start"] >= 61
    second, third = clients(), clients()
    time.sleep(1)
    assert len(local_server.starts) == 1
    assert_receipt(second, timeout=75)
    assert_receipt(third, timeout=75)
    assert local_server.starts[1] - local_server.ends[0] >= 60.9
    assert local_server.starts[2] - local_server.starts[1] >= 4.95
    assert local_server.maximum == 1


@pytest.mark.parametrize("termination", [signal.SIGINT, signal.SIGKILL])
def test_cancel_or_crash_closes_socket_and_releases_gate(clients, local_server, termination):
    first = clients("cancel")
    await_starts(local_server, 1)
    second = clients()
    time.sleep(0.2)
    first.send_signal(termination)
    first.communicate(timeout=5)
    assert local_server.disconnected.wait(3)
    assert_receipt(second)
    assert local_server.maximum == 1
    assert len(local_server.starts) == 2
    assert local_server.starts[1] - local_server.starts[0] >= 4.95


@pytest.mark.parametrize("body", ["", "{", "{}", "null", "[]", "x" * 4097,
    '{"version":1,"last_start":0,"cooldown_until":NaN,"failures":0}',
    '{"version":1,"last_start":0,"cooldown_until":-1,"failures":0}',
    '{"version":1,"last_start":0,"cooldown_until":0,"failures":false}',
    '{"version":1,"last_start":0,"cooldown_until":0,"failures":0,"secret":"x"}',
    '{"version":1,"last_start":0,"cooldown_until":900,"cooldown_until":0,"failures":0}',
])
def test_corrupt_state_fails_closed_before_any_http(clients, local_server, tmp_path, body):
    shared = tmp_path / "shared"
    shared.mkdir()
    (shared / "status.json").write_text(body)
    process = clients()
    out, err = process.communicate(timeout=5)
    assert process.returncode == 1
    assert out == "" and "admission" in err.lower()
    assert not local_server.starts
    assert (shared / "status.json").read_text() == body


def test_missing_previously_initialized_status_fails_closed(clients, local_server, tmp_path):
    shared = tmp_path / "shared"
    shared.mkdir()
    (shared / "status.lock").touch()
    process = clients()
    out, err = process.communicate(timeout=5)
    assert process.returncode == 1
    assert out == "" and "admission" in err.lower()
    assert not local_server.starts


def test_inaccessible_configured_gate_never_falls_back(clients, local_server, tmp_path):
    blocked = tmp_path / "not-directory"
    blocked.write_text("occupied")
    process = clients(gate=blocked / "admission")
    out, err = process.communicate(timeout=5)
    assert process.returncode == 1
    assert out == "" and "admission" in err.lower()
    assert not local_server.starts


@pytest.fixture
def clock_transport(monkeypatch, tmp_path):
    clock = SimpleNamespace(now=1800000000.0, sleeps=[], requests=[], responses=[], opened=[])

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
    monkeypatch.setenv("VOX_ADMISSION_DIR", str(tmp_path / "shared"))
    monkeypatch.setattr(vox, "time", SimpleNamespace(time=lambda: clock.now, monotonic=lambda: clock.now, sleep=sleep))
    monkeypatch.setattr(vox.OPENER, "open", open_request)
    monkeypatch.setattr(vox.RELAY_OPENER, "open", open_request)
    monkeypatch.setattr(vox, "ATTEMPTS", 2)
    return clock


def http_error(code, retry_after=None):
    headers = {} if retry_after is None else {"Retry-After": retry_after}
    return urllib.error.HTTPError("https://not-called.invalid", code, "failure", headers, io.BytesIO(b"private-payload"))


@pytest.mark.parametrize("retry_after,delay", [(None, 60), ("3", 60), ("65", 65), ("1800", 1800),
    (format_datetime(datetime.fromtimestamp(1800000125, timezone.utc), usegmt=True), 125),
    ("invalid", 60)])
def test_429_retry_waits_for_shared_deadline_without_spending_attempts(clock_transport, tmp_path, retry_after, delay):
    clock = clock_transport
    error = http_error(429, retry_after)
    clock.responses.extend([error, b'{"messages":[]}'])
    assert vox.call("/search/text", {"stream": True}) == {"messages": []}
    assert len(clock.requests) == 2
    assert clock.requests[1][0] - clock.requests[0][0] >= delay
    assert clock.requests[0][1] == clock.requests[1][1]
    assert error.closed and all(response.closed for response in clock.opened)
    raw = (tmp_path / "shared/status.json").read_text()
    assert len(raw) < 4096 and "private-payload" not in raw and "synthetic-test-key" not in raw
    assert json.loads(raw)["failures"] == 0


@pytest.mark.parametrize("failure", [429, 502, 503, 504, 524, "disconnect", "incomplete"])
def test_failure_backoff_increases_is_capped_and_survives_exhausted_budget(clock_transport, monkeypatch, tmp_path, failure):
    clock = clock_transport
    monkeypatch.setattr(vox, "ATTEMPTS", 1)
    delays = []
    for _ in range(7):
        response = (http_error(failure) if isinstance(failure, int) else
                    ConnectionResetError() if failure == "disconnect" else b'{"point":0}\n')
        clock.responses.append(response)
        with pytest.raises(vox.VoxError):
            vox.call("/search/text", {"stream": True})
        status = json.loads((tmp_path / "shared/status.json").read_text())
        delays.append(status["cooldown_until"] - clock.now)
    assert delays == [60, 120, 240, 480, 900, 900, 900]
    assert len(clock.requests) == 7
    assert all(response.closed for response in clock.opened)


def test_429_attempt_budget_remains_two(clock_transport):
    clock = clock_transport
    clock.responses.extend([http_error(429), http_error(429), b'{}'])
    with pytest.raises(vox.VoxError, match="429"):
        vox.call("/search/text", {"stream": True})
    assert len(clock.requests) == 2
    assert clock.requests[1][0] - clock.requests[0][0] >= 60


def test_bad_json_releases_gate_after_socket_close(clock_transport, tmp_path):
    clock = clock_transport
    clock.responses.extend([b'not-json', b'{"messages":[]}'])
    with pytest.raises(vox.VoxError, match="Invalid JSON"):
        vox.call("/search/text", {"stream": True})
    assert clock.opened[0].closed
    with (tmp_path / "shared/gate.lock").open("rb") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    assert vox.call("/search/text", {"stream": True}) == {"messages": []}
    assert clock.requests[1][0] - clock.requests[0][0] >= 5


def test_status_publication_failure_fails_closed_before_request(clock_transport, monkeypatch):
    clock = clock_transport
    clock.responses.append(b'{}')

    def fail_replace(*args):
        raise OSError("synthetic filesystem failure")

    monkeypatch.setattr(vox.os, "replace", fail_replace)
    with pytest.raises(vox.VoxError, match="[Aa]dmission"):
        vox.call("/search/text")
    assert not clock.requests


def test_default_path_is_same_user_cache_not_working_directory(clock_transport, monkeypatch, tmp_path):
    monkeypatch.delenv("VOX_ADMISSION_DIR")
    monkeypatch.setenv("HOME", str(tmp_path))
    clock_transport.responses.extend([b'{}', b'{}'])
    vox.call("/search/text")
    monkeypatch.chdir(tmp_path)
    vox.call("/search/text")
    assert (tmp_path / ".cache/vox/admission/status.json").is_file()
    assert clock_transport.requests[1][0] - clock_transport.requests[0][0] >= 5


@pytest.mark.parametrize("base", ["http://vox-relay:8083", "http://localhost:18089", "http://127.0.0.1:18089", "http://[::1]:18089/"])
def test_relay_mode_has_no_key_no_local_admission_and_no_authorization(clock_transport, monkeypatch, tmp_path, base):
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


@pytest.mark.parametrize("base", ["", "https://vox-relay:8083", "http://api.vox-lab.com", "http://example.com",
    "http://vox-relay.example.com:8083", "http://user:secret@vox-relay:8083",
    "http://vox-relay:8083?secret=query", "http://vox-relay:8083#fragment", "http://vox-relay:8083/path",
    "http://vox-relay:bad", "http://vox-relay:65536", " http://vox-relay:8083", "http://vox-relay\n:8083"])
def test_relay_url_rejects_unsafe_bases_without_network_or_secret_echo(clock_transport, monkeypatch, base):
    monkeypatch.setenv("VOX_RELAY_URL", base)
    clock_transport.responses.append(b'{}')
    with pytest.raises(vox.VoxError, match="VOX_RELAY_URL") as error:
        vox.call("/search/text")
    assert "secret" not in str(error.value)
    assert not clock_transport.requests


def test_relay_failure_is_not_locally_retried_or_cooled_down(clock_transport, monkeypatch, tmp_path):
    monkeypatch.setenv("VOX_RELAY_URL", "http://vox-relay:8083")
    clock_transport.responses.extend([http_error(429, "120"), b'{}'])
    with pytest.raises(vox.VoxError, match="429"):
        vox.call("/search/text")
    assert len(clock_transport.requests) == 1
    assert not clock_transport.sleeps
    assert not (tmp_path / "shared").exists()


def test_redirect_error_closes_unreturned_response(clock_transport):
    body = io.BytesIO(b"synthetic-redirect-body")
    request = vox.urllib.request.Request("https://not-called.invalid")
    with pytest.raises(vox.VoxError, match="refusing"):
        vox.NoRedirect().redirect_request(request, body, 302, "Found", {}, "https://not-called.invalid/other")
    assert body.closed


def test_real_relay_cli_needs_no_key_and_ignores_external_proxy(local_server, tmp_path):
    env = dict(os.environ, HOME=str(tmp_path), VOX_RELAY_URL=local_server.url,
               VOX_ADMISSION_DIR=str(tmp_path / "unused-gate"),
               HTTP_PROXY="http://127.0.0.1:9", http_proxy="http://127.0.0.1:9",
               NO_PROXY="", no_proxy="")
    env.pop("VOX_API_KEY", None)
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "search-page", "ordinary"],
        env=env, cwd=tmp_path, capture_output=True, text=True, timeout=5,
    )
    assert result.returncode == 0, result.stderr
    path = Path(result.stdout.strip())
    assert path.is_absolute() and json.loads(path.read_text())["messages"] == []
    assert len(local_server.starts) == 1
    assert not (tmp_path / "unused-gate").exists()
