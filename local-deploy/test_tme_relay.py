import http.client
import importlib.util
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace

import pytest

MODULE = Path(__file__).with_name("tme_relay.py")


def load_relay():
    spec = importlib.util.spec_from_file_location("tme_relay", MODULE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


relay = load_relay()


@pytest.fixture
def upstream():
    """A fake t.me: answers from `script` (status, headers) in order, then 200."""
    state = SimpleNamespace(requests=[], starts=[], script=[], lock=threading.Lock())

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_GET(self):
            with state.lock:
                state.requests.append((self.path, dict(self.headers)))
                state.starts.append(time.monotonic())
                status, headers = state.script.pop(0) if state.script else (200, {})
            body = f"<html>{self.path}</html>".encode()
            self.send_response(status)
            for name, value in headers.items():
                self.send_header(name, value)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    state.address = server.server_address
    yield state
    server.shutdown()
    server.server_close()


@pytest.fixture
def relays(upstream):
    servers = []

    def start(**overrides):
        options = dict(interval=0.2, backoff=0.1, backoff_max=0.4, give_up=5, queue_limit=10, call_timeout=5)
        options.update(overrides)
        server = relay.RelayServer(("127.0.0.1", 0), upstream=upstream.address, tls=False, **options)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        servers.append(server)
        return server

    yield start
    for server in servers:
        server.shutdown()
        server.server_close()


def get(server, path, headers=None):
    connection = http.client.HTTPConnection(*server.server_address, timeout=10)
    connection.request("GET", path, headers=headers or {})
    response = connection.getresponse()
    result = (response.status, dict(response.getheaders()), response.read())
    connection.close()
    return result


@pytest.mark.parametrize(
    "target,upstream_path",
    [
        ("/s/vahta_sever", "/s/vahta_sever"),
        ("/s/vahta_sever?before=120", "/s/vahta_sever?before=120"),
        ("/s/vahta_sever?after=5", "/s/vahta_sever?after=5"),
        ("/vahta_chat/91?embed=1", "/vahta_chat/91?embed=1"),
        ("/vahta_sever/7?embed=1&comment=502", "/vahta_sever/7?comment=502&embed=1"),
        ("/vahta_sever/7?embed=1&discussion=1&comments_limit=100", "/vahta_sever/7?embed=1&discussion=1&comments_limit=100"),
    ],
)
def test_public_pages_are_rebuilt_from_validated_parts(target, upstream_path):
    assert relay.upstream_target(target) == upstream_path


@pytest.mark.parametrize(
    "target",
    [
        "/c/1234567/89?embed=1",
        "/+AbCdEf123",
        "/joinchat/AbCdEf",
        "/s/joinchat",
        "/addlist/abc",
        "/iv?url=https://evil.invalid",
        "/vahta_chat/91",
        "/vahta_chat/91?embed=1&single=1",
        "/vahta_chat/91?embed=1&embed=1",
        "/s/vahta_sever?before=1&after=2",
        "/s/vahta_sever?before=abc",
        "/vahta_sever/7?embed=1&discussion=1&comments_limit=101",
        "/vahta_chat/12/91?embed=1",
        "//evil.invalid/x",
        "http://evil.invalid/s/vahta",
        "/s/vahta%5fsever",
        "/s/abc",
    ],
)
def test_private_and_unknown_pages_are_refused(target):
    with pytest.raises(relay.Rejected):
        relay.upstream_target(target)


def test_refused_paths_never_reach_t_me(relays, upstream):
    server = relays()
    for target in ("/c/1234567/89?embed=1", "/+AbCdEf123", "/joinchat/AbCdEf"):
        assert get(server, target)[0] in {400, 403}
    assert get(server, "/health")[0] == 200 and get(server, "/status")[0] == 200
    assert upstream.requests == []


def test_requests_keep_one_pace_and_carry_no_caller_headers(relays, upstream):
    server = relays(interval=0.3)
    results = []
    threads = [threading.Thread(target=lambda i=i: results.append(get(server, f"/vahta_chat/{i}?embed=1", {"Cookie": "stel_ssid=secret"}))) for i in range(1, 4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(10)
    assert sorted(r[0] for r in results) == [200, 200, 200]
    gaps = [b - a for a, b in zip(upstream.starts, upstream.starts[1:])]
    assert len(gaps) == 2 and min(gaps) >= 0.28
    headers = upstream.requests[0][1]
    assert "Cookie" not in headers and headers["User-Agent"] == relay.USER_AGENT


def test_refusal_pauses_and_retries_with_retry_after(relays, upstream):
    upstream.script = [(429, {"Retry-After": "0.5"})]
    server = relays(interval=0.05)
    status, _, body = get(server, "/vahta_chat/91?embed=1")
    assert status == 200 and body == b"<html>/vahta_chat/91?embed=1</html>"
    assert len(upstream.starts) == 2 and upstream.starts[1] - upstream.starts[0] >= 0.48


def test_refusals_back_off_doubling_and_reset_after_success(relays, upstream):
    upstream.script = [(503, {}), (403, {}), (502, {})]
    server = relays(interval=0.01, backoff=0.1, backoff_max=0.25)
    assert get(server, "/vahta_chat/91?embed=1")[0] == 200
    gaps = [b - a for a, b in zip(upstream.starts, upstream.starts[1:])]
    # 0.1, then doubled to 0.2, then capped at 0.25.
    assert gaps[0] >= 0.09 and gaps[1] >= 0.19 and 0.24 <= gaps[2] < 0.4
    assert server.pace.refusals == 0


def test_long_refusal_gives_up_with_advice(relays, upstream):
    upstream.script = [(429, {})] * 20
    server = relays(interval=0.01, backoff=0.1, backoff_max=0.1, give_up=0.35)
    status, _, body = get(server, "/vahta_chat/91?embed=1")
    assert status == 503 and b"keeps refusing" in body and b"Do other work" in body


def test_chat_redirect_is_passed_back_not_followed(relays, upstream):
    upstream.script = [(302, {"Location": "https://t.me/vahta_chat"})]
    server = relays()
    status, headers, _ = get(server, "/s/vahta_chat?before=94")
    assert status == 302 and headers["Location"] == "/vahta_chat" and len(upstream.requests) == 1


def test_foreign_redirect_location_is_dropped(relays, upstream):
    upstream.script = [(302, {"Location": "https://evil.invalid/x"})]
    status, headers, _ = get(relays(), "/s/vahta_chat")
    assert status == 302 and "Location" not in headers


def test_full_queue_fails_at_once():
    pace = relay.Pace(interval=0, backoff=0, backoff_max=0, queue_limit=1)
    pace.acquire(lambda: False)
    waiter = threading.Thread(target=lambda: (pace.acquire(lambda: False), pace.release()))
    waiter.start()
    deadline = time.monotonic() + 2
    while pace.read()["queued"] < 1 and time.monotonic() < deadline:
        time.sleep(0.01)
    with pytest.raises(relay.QueueFull):
        pace.acquire(lambda: False)
    pace.release()
    waiter.join(2)
    assert pace.read() == {"active": 0, "queued": 0, "paused_seconds": 0.0, "refusals": 0}
