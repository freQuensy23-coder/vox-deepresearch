import importlib.util
import json
from pathlib import Path

import pytest


PATH = Path(__file__).parent / "jev_bridge.py"


def load():
    assert PATH.exists(), "narrow Jev bridge missing"
    spec = importlib.util.spec_from_file_location("jev_bridge", PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize(
    "path",
    [
        "http://example.com/",
        "/openapi.json",
        "/v1/chat/completions",
        "/v1/batches/../../",
        "/v1/models?url=http://example.com",
        "/v1/batches/batch_a/results?offset=0&limit=1001",
    ],
)
def test_arbitrary_paths_cannot_reach_gateway(path):
    module = load()
    with pytest.raises(module.BridgeError):
        module.validate("GET", path, b"")


def test_cancel_only_accepts_exact_batch_path():
    module = load()
    module.validate("DELETE", "/v1/batches/batch_" + "a" * 32, b"")
    with pytest.raises(module.BridgeError):
        module.validate("DELETE", "/v1/models", b"")


def test_typed_only_bounded_payloads():
    module = load()
    valid = {
        "state": "Synthetic",
        "model": "jev-latest",
        "questions": {"relevant": {"type": "noul"}},
    }
    module.validate("POST", "/v1/systemone", json.dumps(valid).encode())
    valid["questions"]["relevant"]["type"] = "text"
    with pytest.raises(module.BridgeError):
        module.validate("POST", "/v1/systemone", json.dumps(valid).encode())
    with pytest.raises(module.BridgeError):
        module.validate("POST", "/v1/systemone", b"x" * (module.MAX_BYTES + 1))


def test_batch_preserves_agent_policy_and_exceeds_old_client_caps():
    module = load()
    valid = {
        "requests": [
            {
                "custom_id": str(i),
                "body": {
                    "state": "x" * 6000,
                    "model": "jev-latest",
                    "questions": {"x": {"type": "noul"}},
                },
            }
            for i in range(65)
        ],
        "policy": {"concurrency": 8},
    }
    module.validate("POST", "/v1/batches", json.dumps(valid).encode())


def test_more_than_four_concurrent_requests_complete():
    import concurrent.futures
    import http.client
    import threading
    import time

    module = load()

    class Handler(module.BaseHTTPRequestHandler):
        def do_GET(self):
            time.sleep(0.3)
            self.send_response(200)
            self.send_header("Content-Length", "2")
            self.end_headers()
            self.wfile.write(b"ok")

        def log_message(self, *args):
            pass

    server = module.InternalServer(("127.0.0.1", 0), Handler)
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()

    def call():
        connection = http.client.HTTPConnection(*server.server_address, timeout=2)
        try:
            connection.request("GET", "/v1/models")
            response = connection.getresponse()
            return response.status, response.read()
        finally:
            connection.close()

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
            results = list(pool.map(lambda _: call(), range(6)))
        assert results == [(200, b"ok")] * 6
    finally:
        server.shutdown()
        server.server_close()
        worker.join(timeout=2)


def test_missing_gateway_returns_explicit_unavailable_without_secret():
    module = load()

    class Missing:
        def __init__(self, *args, **kwargs):
            pass

        def request(self, *args, **kwargs):
            raise ConnectionRefusedError("private detail")

        def close(self):
            pass

    status, body = module.forward(
        "GET", "/v1/models", b"", "dummy-secret", connection_factory=Missing
    )
    assert status == 503
    assert "dummy-secret" not in body.decode()
    assert "private detail" not in body.decode()
