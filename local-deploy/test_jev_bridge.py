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


@pytest.mark.parametrize("path", ["http://example.com/", "/openapi.json", "/v1/chat/completions", "/v1/batches/../../", "/v1/models?url=http://example.com", "/v1/batches/batch_a/results?offset=0&limit=1001"])
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
    valid = {"state": "Synthetic", "model": "jev-latest", "questions": {"relevant": {"type": "noul"}}}
    module.validate("POST", "/v1/systemone", json.dumps(valid).encode())
    valid["questions"]["relevant"]["type"] = "text"
    with pytest.raises(module.BridgeError):
        module.validate("POST", "/v1/systemone", json.dumps(valid).encode())
    with pytest.raises(module.BridgeError):
        module.validate("POST", "/v1/systemone", b"x" * (256 * 1024 + 1))


def test_batch_requires_explicit_free_only_policy():
    module = load()
    valid = {"requests": [{"custom_id": "x", "body": {"state": "Synthetic", "model": "jev-latest", "questions": {"x": {"type": "noul"}}}}], "policy": {"allow_paid": False, "concurrency": 1, "free_wait_seconds": 600}}
    module.validate("POST", "/v1/batches", json.dumps(valid).encode())
    valid["policy"]["allow_paid"] = True
    with pytest.raises(module.BridgeError):
        module.validate("POST", "/v1/batches", json.dumps(valid).encode())


def test_missing_gateway_returns_explicit_unavailable_without_secret():
    module = load()
    class Missing:
        def __init__(self, *args, **kwargs):
            pass
        def request(self, *args, **kwargs):
            raise ConnectionRefusedError("private detail")
        def close(self):
            pass
    status, body = module.forward("GET", "/v1/models", b"", "dummy-secret", connection_factory=Missing)
    assert status == 503
    assert "dummy-secret" not in body.decode()
    assert "private detail" not in body.decode()
