import asyncio
import json
import os
import shutil
import stat
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
from langchain_core.messages import AIMessage, HumanMessage
from langchain_core.outputs import ChatGeneration, LLMResult


def tracing():
    from deerflow.models import local_trace

    return local_trace


def records(root):
    return [json.loads(p.read_text()) for p in root.rglob("*.json")]


def test_full_trace_permissions_redaction_and_correlation(tmp_path, monkeypatch):
    trace = tracing()
    monkeypatch.setenv("DEER_FLOW_LLM_TRACE_DIR", str(tmp_path / "traces"))
    handler = trace.LocalLLMTraceCallback()
    call_id, parent_id = uuid4(), uuid4()
    text = "sensitive synthetic content" * 100000
    handler.on_chat_model_start(
        {},
        [[HumanMessage(content=text)]],
        run_id=call_id,
        parent_run_id=parent_id,
        metadata={"thread_id": "thread-1", "run_id": "run-1"},
        invocation_params={"model": "offline", "tools": [{"name": "tool"}], "api_key": "never-store", "headers": {"Authorization": "Bearer never-store"}},
    )
    handler.on_llm_end(
        LLMResult(generations=[[ChatGeneration(message=AIMessage(content=text, tool_calls=[{"id": "tc1", "name": "tool", "args": {"full": text}}], usage_metadata={"input_tokens": 1, "output_tokens": 2, "total_tokens": 3}))]]),
        run_id=call_id,
    )
    rows = records(tmp_path)
    start = next(r for r in rows if r["event"] == "call_start")
    end = next(r for r in rows if r["event"] == "call_end")
    assert start["call_id"] == str(call_id)
    assert start["parent_call_id"] == str(parent_id)
    assert start["thread_id"] == "thread-1"
    assert start["run_id"] == "run-1"
    assert start["payload"]["messages"][0][0]["content"] == text
    assert end["payload"]["generations"][0][0]["message"]["tool_calls"][0]["args"]["full"] == text
    assert end["monotonic_ns"] >= start["monotonic_ns"]
    assert start["utc"].endswith("+00:00")
    for p in (tmp_path / "traces").rglob("*"):
        assert stat.S_IMODE(p.stat().st_mode) == (0o700 if p.is_dir() else 0o600)
        if p.is_file():
            assert "never-store" not in p.read_text()


def test_concurrent_atomic_records(tmp_path, monkeypatch):
    trace = tracing()
    monkeypatch.setenv("DEER_FLOW_LLM_TRACE_DIR", str(tmp_path / "traces"))

    def write(index):
        handler = trace.LocalLLMTraceCallback()
        call_id = uuid4()
        handler.on_chat_model_start({}, [[HumanMessage(content=str(index))]], run_id=call_id)
        handler.on_llm_error(ValueError("synthetic failure"), run_id=call_id)

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(write, range(32)))
    rows = records(tmp_path)
    assert len(rows) == 64
    assert len({r["call_id"] for r in rows}) == 32
    assert not list(tmp_path.rglob("*.pending"))


def test_cancellation_and_trace_failure_do_not_replace_result(tmp_path, monkeypatch, caplog):
    trace = tracing()
    monkeypatch.setenv("DEER_FLOW_LLM_TRACE_DIR", str(tmp_path / "traces"))
    handler = trace.LocalLLMTraceCallback()
    call_id = uuid4()
    handler.on_chat_model_start({}, [[HumanMessage(content="secret input")]], run_id=call_id)
    handler.on_llm_error(asyncio.CancelledError(), run_id=call_id)
    assert any(r["event"] == "call_cancelled" for r in records(tmp_path))

    def fail(*args, **kwargs):
        raise OSError("secret disk details")

    monkeypatch.setattr(trace, "_write_record", fail)
    handler.on_chat_model_start({}, [[HumanMessage(content="secret input")]], run_id=uuid4())
    assert "LLM trace write failed" in caplog.text
    assert "secret" not in caplog.text


def test_factory_installs_local_trace_even_without_external_tracing(monkeypatch):
    trace = tracing()
    from deerflow.config.app_config import AppConfig
    from deerflow.models.factory import create_chat_model

    config = AppConfig(models=[{"name": "offline", "model": "offline", "use": "langchain_core.language_models.fake_chat_models:FakeListChatModel", "responses": ["ok"]}], sandbox={"use": "deerflow.sandbox.local:LocalSandboxProvider"})
    model = create_chat_model("offline", app_config=config, attach_tracing=False)
    assert any(isinstance(h, trace.LocalLLMTraceCallback) for h in model.callbacks)


def test_codex_exact_attempts_stream_and_errors(tmp_path, monkeypatch):
    tracing()
    from deerflow.models import openai_codex_provider as provider

    monkeypatch.setenv("DEER_FLOW_LLM_TRACE_DIR", str(tmp_path / "traces"))
    monkeypatch.setattr(provider.CodexChatModel, "_load_codex_auth", lambda self: SimpleNamespace(access_token="never-store", account_id="offline"))
    monkeypatch.setattr(provider.time, "sleep", lambda _: None)
    requests = []

    def handle(request):
        requests.append(json.loads(request.content))
        if len(requests) == 1:
            return httpx.Response(429, json={"error": "retry me"})
        events = [
            {"type": "response.output_item.done", "output_index": 0, "item": {"type": "message", "content": [{"type": "output_text", "text": "complete answer"}]}},
            {"type": "response.completed", "response": {"output": [], "usage": {"input_tokens": 2, "output_tokens": 3, "total_tokens": 5}}},
        ]
        return httpx.Response(200, text="".join("data: " + json.dumps(e) + "\n\n" for e in events))

    client = httpx.Client
    monkeypatch.setattr(provider.httpx, "Client", lambda **kw: client(transport=httpx.MockTransport(handle), **kw))
    model = provider.CodexChatModel()
    result = model.invoke([HumanMessage(content="exact question")])
    assert result.content == "complete answer"
    rows = records(tmp_path)
    attempts = sorted([r for r in rows if r["event"] == "attempt_start"], key=lambda r: r["attempt"])
    assert len(attempts) == 2
    assert [r["payload"] for r in attempts] == requests
    assert len({r["call_id"] for r in attempts}) == 1
    assert any(r["event"] == "attempt_error" and r["payload"]["status_code"] == 429 for r in rows)
    assert len([r for r in rows if r["event"] == "provider_event"]) == 2
    final = next(r for r in rows if r["event"] == "attempt_end")
    assert final["payload"]["output"][0]["content"][0]["text"] == "complete answer"
    assert all("never-store" not in json.dumps(r) for r in rows)
    export = os.environ.get("DEER_FLOW_TEST_TRACE_EXPORT")
    if export:
        shutil.copytree(tmp_path / "traces", os.path.join(export, "codex-" + uuid4().hex))


@pytest.mark.asyncio
async def test_async_stream_aggregation_and_callback_io_off_loop(tmp_path, monkeypatch):
    import threading

    from langchain_core.language_models.fake_chat_models import FakeListChatModel

    trace = tracing()
    monkeypatch.setenv("DEER_FLOW_LLM_TRACE_DIR", str(tmp_path / "traces"))
    writes = []
    original = trace._write_record

    def write(*args):
        writes.append(threading.get_ident())
        return original(*args)

    monkeypatch.setattr(trace, "_write_record", write)
    model = FakeListChatModel(responses=["complete streamed answer"], callbacks=[trace.LocalLLMTraceCallback()])
    result = "".join([chunk.content async for chunk in model.astream("question", config={"callbacks": []})])
    assert result == "complete streamed answer"
    end = next(r for r in records(tmp_path) if r["event"] == "call_end")
    assert end["payload"]["generations"][0][0]["message"]["content"] == result
    assert writes and threading.get_ident() not in writes


@pytest.mark.asyncio
async def test_real_async_cancellation_traced(tmp_path, monkeypatch):
    from langchain_core.language_models.fake_chat_models import FakeListChatModel

    trace = tracing()
    monkeypatch.setenv("DEER_FLOW_LLM_TRACE_DIR", str(tmp_path / "traces"))
    entered = asyncio.Event()

    class WaitingModel(FakeListChatModel):
        async def _agenerate(self, *args, **kwargs):
            entered.set()
            await asyncio.Event().wait()

    model = WaitingModel(responses=["answer"], callbacks=[trace.LocalLLMTraceCallback()])
    trace.instrument_cancellation(model)
    task = asyncio.create_task(model.ainvoke("question"))
    await asyncio.wait_for(entered.wait(), 5)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert any(r["event"] == "call_cancelled" for r in records(tmp_path)), records(tmp_path)


def test_interrupted_recovery_and_owner_isolation(tmp_path, monkeypatch):
    trace = tracing()
    from deerflow.runtime.user_context import reset_current_user, set_current_user

    monkeypatch.setenv("DEER_FLOW_LLM_TRACE_DIR", str(tmp_path / "traces"))
    for owner in ["one", "two"]:
        token = set_current_user(SimpleNamespace(id=owner))
        try:
            call = trace.CallTrace()
            call.emit("call_start", {"messages": []})
        finally:
            reset_current_user(token)
    starts = records(tmp_path)
    assert len({p.parent.parent.name for p in tmp_path.rglob("*.json")}) == 2
    assert trace.mark_interrupted_calls(tmp_path / "traces", process_instance_id=starts[0]["process_instance_id"], confirmed_stopped=True) == 2
    assert len([r for r in records(tmp_path) if r["event"] == "call_interrupted"]) == 2
    assert trace.mark_interrupted_calls(tmp_path / "traces", process_instance_id=starts[0]["process_instance_id"], confirmed_stopped=True) == 0


def test_symlink_trace_root_is_rejected(tmp_path, monkeypatch, caplog):
    trace = tracing()
    outside = tmp_path / "outside"
    outside.mkdir()
    link = tmp_path / "trace-link"
    link.symlink_to(outside, target_is_directory=True)
    monkeypatch.setenv("DEER_FLOW_LLM_TRACE_DIR", str(link))
    trace.CallTrace().emit("call_start", {"text": "private"})
    assert not list(outside.iterdir())
    assert "LLM trace write failed" in caplog.text


def test_payload_secrets_and_tool_schema_are_literal(tmp_path, monkeypatch):
    trace = tracing()
    monkeypatch.setenv("DEER_FLOW_LLM_TRACE_DIR", str(tmp_path / "traces"))
    handler = trace.LocalLLMTraceCallback({"api_key": "transport-token", "headers": {"Authorization": "Bearer transport-token"}})
    call_id = uuid4()
    text = "Bearer ordinary-user-value"
    tool = {"name": "credentials", "parameters": {"properties": {"password": {"type": "string"}, "secret": {"type": "string"}, "headers": {"type": "object"}}}}
    handler.on_chat_model_start({}, [[HumanMessage(content=text)]], run_id=call_id, invocation_params={"tools": [tool], "api_key": "transport-token"})
    handler.on_llm_end(
        LLMResult(generations=[[ChatGeneration(message=AIMessage(content=text, tool_calls=[{"id": "t", "name": "credentials", "args": {"password": "literal-password", "secret": "literal-secret", "headers": {"Authorization": text}}}]))]]),
        run_id=call_id,
    )
    rows = records(tmp_path)
    start = next(r for r in rows if r["event"] == "call_start")
    end = next(r for r in rows if r["event"] == "call_end")
    assert start["payload"]["messages"][0][0]["content"] == text
    assert start["payload"]["invocation_params"]["tools"] == [tool]
    assert end["payload"]["generations"][0][0]["message"]["tool_calls"][0]["args"]["password"] == "literal-password"
    assert "transport-token" not in json.dumps(rows)


def test_trace_failure_persists_gap_and_health_on_recovery(tmp_path, monkeypatch):
    trace = tracing()
    monkeypatch.setenv("DEER_FLOW_LLM_TRACE_DIR", str(tmp_path / "traces"))
    original = trace._write_record
    failures = []

    def fail_once(*args):
        if not failures:
            failures.append(True)
            raise OSError("synthetic disk failure")
        return original(*args)

    monkeypatch.setattr(trace, "_write_record", fail_once)
    call = trace.CallTrace()
    call.emit("call_start", {"input": "missing record"})
    call.emit("call_end", {"output": "present record"})
    assert any(r["event"] == "trace_gap" for r in records(tmp_path))
    assert trace.trace_health()["complete"] is False


def test_openai_sdk_each_wire_attempt_and_body(tmp_path, monkeypatch):
    tracing()
    from deerflow.config.app_config import AppConfig
    from deerflow.models.factory import create_chat_model

    monkeypatch.setenv("DEER_FLOW_LLM_TRACE_DIR", str(tmp_path / "traces"))
    sent = []

    def handle(request):
        sent.append(json.loads(request.content))
        if len(sent) == 1:
            return httpx.Response(503, json={"error": {"message": "synthetic retry"}})
        return httpx.Response(
            200, json={"id": "offline", "choices": [{"index": 0, "message": {"role": "assistant", "content": "wire answer"}, "finish_reason": "stop"}], "usage": {"prompt_tokens": 1, "completion_tokens": 2, "total_tokens": 3}}
        )

    client = httpx.Client(transport=httpx.MockTransport(handle))
    config = AppConfig(models=[{"name": "offline", "model": "offline", "use": "langchain_openai:ChatOpenAI", "api_key": "never-store", "max_retries": 1}], sandbox={"use": "deerflow.sandbox.local:LocalSandboxProvider"})
    model = create_chat_model("offline", app_config=config, http_client=client)
    assert model.invoke("Bearer literal-input").content == "wire answer"
    rows = records(tmp_path)
    attempts = sorted([r for r in rows if r["event"] == "wire_attempt_start"], key=lambda r: r["attempt"])
    assert [r["payload"]["body"] for r in attempts] == sent
    assert len({r["call_id"] for r in attempts}) == 1
    assert "never-store" not in json.dumps(rows)
    assert len([r for r in rows if r["event"] == "wire_attempt_end"]) == 2


def test_untrusted_owner_metadata_does_not_change_bucket(tmp_path, monkeypatch):
    trace = tracing()
    from deerflow.runtime.user_context import reset_current_user, set_current_user

    monkeypatch.setenv("DEER_FLOW_LLM_TRACE_DIR", str(tmp_path / "traces"))
    token = set_current_user(SimpleNamespace(id="trusted-owner"))
    try:
        trace.CallTrace(metadata={"owner_id": "forged", "user_id": "forged"}).emit("call_start", {})
    finally:
        reset_current_user(token)
    assert records(tmp_path)[0]["owner_id"] == "trusted-owner"


@pytest.mark.asyncio
async def test_goal_evaluator_has_local_trace_with_run_correlation(tmp_path, monkeypatch):
    tracing()
    from deerflow.config.app_config import AppConfig
    from deerflow.models.factory import create_chat_model
    from deerflow.runtime.goal import build_goal_state, evaluate_goal_completion

    monkeypatch.setenv("DEER_FLOW_LLM_TRACE_DIR", str(tmp_path / "traces"))
    config = AppConfig(
        models=[{"name": "offline", "model": "offline", "use": "langchain_core.language_models.fake_chat_models:FakeListChatModel", "responses": ['{"satisfied": true}']}], sandbox={"use": "deerflow.sandbox.local:LocalSandboxProvider"}
    )
    model = create_chat_model("offline", app_config=config, attach_tracing=False)
    await evaluate_goal_completion(build_goal_state("synthetic goal"), [HumanMessage(content="goal"), AIMessage(content="synthetic evidence")], model=model, thread_id="goal-thread", run_id="goal-run", usage_callback=lambda records: None)
    start = next(r for r in records(tmp_path) if r["event"] == "call_start")
    assert start["thread_id"] == "goal-thread"
    assert start["run_id"] == "goal-run"
    assert any(r["event"] == "call_end" for r in records(tmp_path))


@pytest.mark.asyncio
async def test_stream_cancellation_preserves_partial_chunks(tmp_path, monkeypatch):
    tracing()
    from deerflow.config.app_config import AppConfig
    from deerflow.models.factory import create_chat_model

    monkeypatch.setenv("DEER_FLOW_LLM_TRACE_DIR", str(tmp_path / "traces"))
    config = AppConfig(
        models=[{"name": "offline", "model": "offline", "use": "langchain_core.language_models.fake_chat_models:FakeListChatModel", "responses": ["partial answer"]}], sandbox={"use": "deerflow.sandbox.local:LocalSandboxProvider"}
    )
    model = create_chat_model("offline", app_config=config)
    stream = model.astream("input")
    await anext(stream)
    await stream.aclose()
    rows = records(tmp_path)
    assert any(r["event"] == "call_chunk" for r in rows)
    assert any(r["event"] == "call_cancelled" for r in rows)


@pytest.mark.asyncio
async def test_async_openai_sdk_sse_and_final_aggregation(tmp_path, monkeypatch):
    import base64

    from deerflow.config.app_config import AppConfig
    from deerflow.models.factory import create_chat_model

    monkeypatch.setenv("DEER_FLOW_LLM_TRACE_DIR", str(tmp_path / "traces"))
    chunks = [
        {"id": "offline", "object": "chat.completion.chunk", "choices": [{"index": 0, "delta": {"role": "assistant", "content": "whole "}, "finish_reason": None}]},
        {"id": "offline", "object": "chat.completion.chunk", "choices": [{"index": 0, "delta": {"content": "answer"}, "finish_reason": "stop"}]},
    ]
    wire = "".join("data: " + json.dumps(chunk) + "\n\n" for chunk in chunks) + "data: [DONE]\n\n"
    client = httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(200, text=wire, headers={"Content-Type": "text/event-stream"})))
    config = AppConfig(models=[{"name": "offline", "model": "offline", "use": "langchain_openai:ChatOpenAI", "api_key": "transport-token"}], sandbox={"use": "deerflow.sandbox.local:LocalSandboxProvider"})
    model = create_chat_model("offline", app_config=config, http_async_client=client)
    assert "".join([chunk.content async for chunk in model.astream("question")]) == "whole answer"
    rows = records(tmp_path)
    end = next(r for r in rows if r["event"] == "call_end")
    assert end["payload"]["generations"][0][0]["message"]["content"] == "whole answer"
    wire_end = next(r for r in rows if r["event"] == "wire_attempt_end")
    assert base64.b64decode(wire_end["payload"]["body"]["base64"]).decode() == wire
    assert "transport-token" not in json.dumps(rows)
    await client.aclose()


@pytest.mark.asyncio
async def test_real_summary_invocation_keeps_compaction_scope(tmp_path, monkeypatch):
    trace = tracing()
    from deerflow.agents.middlewares.summarization_middleware import DeerFlowSummarizationMiddleware
    from deerflow.config.app_config import AppConfig
    from deerflow.models.factory import create_chat_model

    monkeypatch.setenv("DEER_FLOW_LLM_TRACE_DIR", str(tmp_path / "traces"))
    config = AppConfig(
        models=[{"name": "offline", "model": "offline", "use": "langchain_core.language_models.fake_chat_models:FakeListChatModel", "responses": ["complete summary"]}], sandbox={"use": "deerflow.sandbox.local:LocalSandboxProvider"}
    )
    model = create_chat_model("offline", app_config=config, attach_tracing=False)
    middleware = object.__new__(DeerFlowSummarizationMiddleware)
    with trace.trace_scope(thread_id="compact-thread", operation="manual_compaction"):
        assert await middleware._ainvoke_summary(model, "full compaction prompt") == "complete summary"
    start = next(r for r in records(tmp_path) if r["event"] == "call_start")
    assert start["thread_id"] == "compact-thread"
    assert start["operation"] == "manual_compaction"
    assert start["payload"]["messages"][0][0]["content"] == "full compaction prompt"


def test_recovered_gap_stays_in_original_owner_bucket(tmp_path, monkeypatch):
    trace = tracing()
    from deerflow.runtime.user_context import reset_current_user, set_current_user

    monkeypatch.setenv("DEER_FLOW_LLM_TRACE_DIR", str(tmp_path / "traces"))
    original = trace._write_record
    failures = []

    def fail_once(*args):
        if not failures:
            failures.append(True)
            raise OSError("synthetic")
        return original(*args)

    monkeypatch.setattr(trace, "_write_record", fail_once)
    for owner in ["first-owner", "second-owner"]:
        token = set_current_user(SimpleNamespace(id=owner))
        try:
            trace.CallTrace().emit("call_start", {})
        finally:
            reset_current_user(token)
    gap = next(r for r in records(tmp_path) if r["event"] == "trace_gap")
    assert gap["owner_id"] == "first-owner"
