"""Offline wire-level coverage for configured Codex profiles and stalled streams."""

from __future__ import annotations

import json
from types import SimpleNamespace

import httpx
import pytest
from langchain_core.messages import HumanMessage

from deerflow.config.app_config import AppConfig
from deerflow.config.model_config import ModelConfig
from deerflow.config.sandbox_config import SandboxConfig
from deerflow.models import openai_codex_provider as provider
from deerflow.models.credential_loader import CodexCliCredential
from deerflow.models.factory import create_chat_model


@pytest.fixture(autouse=True)
def credentials(monkeypatch):
    monkeypatch.setattr(provider, "load_codex_cli_credential", lambda: CodexCliCredential("synthetic-token", "synthetic-account"))


def completed_response():
    return {
        "type": "response.completed",
        "response": {"output": [{"type": "message", "content": [{"type": "output_text", "text": "Summary ready"}]}], "usage": {}},
    }


def install_http(monkeypatch, handler):
    client_class = httpx.Client
    monkeypatch.setattr(provider.httpx, "Client", lambda **kwargs: client_class(transport=httpx.MockTransport(handler), **kwargs))


def test_factory_profile_honors_disabled_effort_fallback_and_service_tier(monkeypatch):
    requests = []

    def handle(request):
        payload = json.loads(request.content)
        requests.append(payload)
        event = {"type": "error", "code": "unsupported_value", "message": "This model does not support reasoning effort none"} if payload["reasoning"]["effort"] == "none" else completed_response()
        return httpx.Response(200, text="data: " + json.dumps(event) + "\n\n", headers={"content-type": "text/event-stream"})

    install_http(monkeypatch, handle)
    config = AppConfig(
        models=[ModelConfig(name="profile", use="deerflow.models.openai_codex_provider:CodexChatModel", model="gpt-6.1-sol", supports_thinking=True, service_tier="priority", none_effort_fallback="low")],
        sandbox=SandboxConfig(use="deerflow.sandbox.local:LocalSandboxProvider"),
    )
    model = create_chat_model("profile", thinking_enabled=False, app_config=config, attach_tracing=False)

    assert model.invoke([HumanMessage(content="Summarize this synthetic note")]).content == "Summary ready"
    assert requests[0]["reasoning"] == {"effort": "low"}
    assert requests[0]["service_tier"] == "priority"


@pytest.mark.parametrize("effort,fallback,expected", [("none", None, {"effort": "none"}), ("high", "low", {"effort": "high", "summary": "detailed"})])
def test_unconfigured_legacy_and_enabled_effort_are_preserved(monkeypatch, effort, fallback, expected):
    payloads = []

    def handle(request):
        payloads.append(json.loads(request.content))
        return httpx.Response(200, text="data: " + json.dumps(completed_response()) + "\n\n")

    install_http(monkeypatch, handle)
    model = provider.CodexChatModel(reasoning_effort=effort, none_effort_fallback=fallback)
    model.invoke([HumanMessage(content="Synthetic note")])
    assert payloads[0]["reasoning"] == expected
    assert "service_tier" not in payloads[0]


def test_keepalives_cannot_keep_a_stalled_stream_open_forever(monkeypatch):
    class Keepalives(httpx.SyncByteStream):
        closed = False

        def __iter__(self):
            for _ in range(10):
                yield b": keepalive\n\n"

        def close(self):
            self.closed = True

    stream = Keepalives()
    install_http(monkeypatch, lambda request: httpx.Response(200, stream=stream))
    ticks = iter(range(100))
    monkeypatch.setattr(provider, "time", SimpleNamespace(monotonic=lambda: next(ticks), sleep=lambda _: None))
    model = provider.CodexChatModel(stream_idle_timeout=1.5, retry_max_attempts=1)
    with pytest.raises(TimeoutError, match="no Codex stream events"):
        model.invoke([HumanMessage(content="Synthetic note")])
    assert stream.closed


@pytest.mark.parametrize("recovers", [True, False])
def test_transport_retry_is_bounded_and_can_recover(monkeypatch, recovers):
    requests = []

    def handle(request):
        requests.append(request)
        if len(requests) == 1 or not recovers:
            raise httpx.ReadTimeout("Synthetic transport interruption", request=request)
        return httpx.Response(200, text="data: " + json.dumps(completed_response()) + "\n\n")

    install_http(monkeypatch, handle)
    monkeypatch.setattr(provider.time, "sleep", lambda _: None)
    model = provider.CodexChatModel(retry_max_attempts=2)
    if recovers:
        assert model.invoke([HumanMessage(content="Synthetic note")]).content == "Summary ready"
    else:
        with pytest.raises(httpx.ReadTimeout):
            model.invoke([HumanMessage(content="Synthetic note")])
    assert len(requests) == 2
