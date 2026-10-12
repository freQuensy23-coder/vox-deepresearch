import json

import pytest
import requests

from deerflow.community.aio_sandbox.remote_backend import RemoteSandboxBackend
from deerflow.community.aio_sandbox.sandbox_info import SandboxInfo


def response(status, payload=None):
    result = requests.Response()
    result.status_code = status
    result._content = json.dumps(payload).encode()
    return result


@pytest.mark.parametrize("failure", [503, "timeout", 202, 204, "wrong-id", "running", "malformed"])
def test_strict_destroy_rejects_unconfirmed_delete(monkeypatch, caplog, failure):
    backend = RemoteSandboxBackend("http://synthetic.invalid")
    info = SandboxInfo(sandbox_id="synthetic", sandbox_url="http://synthetic.invalid")

    def delete(*args, **kwargs):
        if failure == "timeout":
            raise requests.Timeout("ambiguous deletion")
        payload = {"sandbox_id": "synthetic", "status": "Deleted"}
        if failure == "wrong-id":
            payload["sandbox_id"] = "other"
        if failure == "running":
            payload["status"] = "Running"
        if failure == "malformed":
            payload = []
        return response(failure if isinstance(failure, int) else 200, payload)

    monkeypatch.setattr(requests, "delete", delete)
    with pytest.raises(RuntimeError):
        backend.destroy_strict(info)
    assert "destroyed sandbox" not in caplog.text.lower()


@pytest.mark.parametrize("get_status", [200, 503, 404])
def test_delete_404_requires_verified_absence(monkeypatch, get_status):
    backend = RemoteSandboxBackend("http://synthetic.invalid", api_key="synthetic-key")
    info = SandboxInfo(sandbox_id="synthetic", sandbox_url="http://synthetic.invalid")
    calls = []

    def request(method, status):
        def perform(url, **kwargs):
            calls.append((method, url, kwargs))
            return response(status)
        return perform

    monkeypatch.setattr(requests, "delete", request("DELETE", 404))
    monkeypatch.setattr(requests, "get", request("GET", get_status))
    if get_status == 404:
        backend.destroy_strict(info)
    else:
        with pytest.raises(RuntimeError):
            backend.destroy_strict(info)
    assert [call[0] for call in calls] == ["DELETE", "GET"]
    assert all(call[1].endswith("/api/sandboxes/synthetic") for call in calls)
    assert all(call[2]["headers"] == {"X-API-Key": "synthetic-key"} for call in calls)
    assert all(0 < call[2]["timeout"] <= 15 for call in calls)


def test_confirmed_deleted_ack(monkeypatch):
    backend = RemoteSandboxBackend("http://synthetic.invalid")
    monkeypatch.setattr(requests, "delete", lambda *a, **k: response(200, {"sandbox_id": "synthetic", "status": "Deleted"}))
    backend.destroy_strict(SandboxInfo(sandbox_id="synthetic", sandbox_url="http://synthetic.invalid"))


@pytest.mark.parametrize("failure", [503, "timeout"])
def test_generic_destroy_preserves_best_effort_contract(monkeypatch, failure):
    def delete(*args, **kwargs):
        if failure == "timeout":
            raise requests.Timeout("ambiguous deletion")
        return response(503)

    monkeypatch.setattr(requests, "delete", delete)
    RemoteSandboxBackend("http://synthetic.invalid").destroy(SandboxInfo(sandbox_id="synthetic", sandbox_url="http://synthetic.invalid"))
