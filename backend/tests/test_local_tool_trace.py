import asyncio
import json
import multiprocessing
import os
from types import SimpleNamespace

import pytest
from deerflow_extension_api import AgentScope
from langchain.agents.middleware import AgentMiddleware
from langchain_core.messages import ToolMessage
from langgraph.types import Command

from deerflow.extensions.stack import compose_with_extensions


def records(root):
    return sorted([json.loads(line) for p in root.rglob("*.jsonl") for line in p.read_text().splitlines() if line], key=lambda r: r["monotonic_ns"])


def stack(middlewares=()):
    return compose_with_extensions(list(middlewares), AgentScope.LEAD, None, SimpleNamespace(has_middleware_contributors=False))


def request(index="tc1"):
    return SimpleNamespace(tool_call={"name": "synthetic_echo", "args": {"value": "ordinary payload"}, "id": index}, runtime=SimpleNamespace(context={"thread_id": "thread-test", "run_id": "run-test"}))


def invoke(chain, req, handler):
    for middleware in reversed(chain):
        inner = handler

        def handler(req, middleware=middleware, inner=inner):
            return middleware.wrap_tool_call(req, inner)

    return handler(req)


async def ainvoke(chain, req, handler):
    for middleware in reversed(chain):
        inner = handler

        def handler(req, middleware=middleware, inner=inner):
            return middleware.awrap_tool_call(req, inner)

    return await handler(req)


def test_full_raw_visible_command_start_before_execution(tmp_path, monkeypatch):
    monkeypatch.setenv("DEER_FLOW_LLM_TRACE_DIR", str(tmp_path))
    text = "ordinary long payload " * 100000

    class Transform(AgentMiddleware):
        def wrap_tool_call(self, req, handler):
            result = handler(req)
            result.update["messages"][0].content = "visible short"
            return result

    def actual(req):
        rows = records(tmp_path)
        assert any(r["event"] == "tool_start" for r in rows)
        assert any(r["event"] == "tool_attempt_start" for r in rows)
        return Command(update={"messages": [ToolMessage(content=text, tool_call_id=req.tool_call["id"])], "ordinary": {"api_key_word_in_text": "preserved"}})

    result = invoke(stack([Transform()]), request(), actual)
    assert result.update["messages"][0].content == "visible short"
    rows = records(tmp_path)
    raw = next(r for r in rows if r["event"] == "tool_attempt_end")
    end = next(r for r in rows if r["event"] == "tool_end")
    assert raw["payload"]["result"]["update"]["messages"][0]["content"] == text
    assert end["payload"]["result"]["update"]["messages"][0]["content"] == "visible short"
    assert end["payload"]["duration_ns"] > 0
    assert end["payload"]["tool_call_id"] == "tc1"
    assert end["payload"]["execution_id"] == raw["payload"]["execution_id"]
    assert end["run_id"] == "run-test"
    assert end["thread_id"] == "thread-test"


def test_blocked_without_handler_is_durable(tmp_path, monkeypatch):
    monkeypatch.setenv("DEER_FLOW_LLM_TRACE_DIR", str(tmp_path))

    class Block(AgentMiddleware):
        def wrap_tool_call(self, req, handler):
            return ToolMessage(content="policy blocked", tool_call_id=req.tool_call["id"], status="error")

    result = invoke(stack([Block()]), request(), lambda req: pytest.fail("executed blocked call"))
    assert result.content == "policy blocked"
    end = next(r for r in records(tmp_path) if r["event"] == "tool_end")
    assert end["payload"]["status"] == "blocked"
    assert end["payload"]["attempt_count"] == 0


def test_trace_observers_preserve_result_identity(tmp_path, monkeypatch):
    monkeypatch.setenv("DEER_FLOW_LLM_TRACE_DIR", str(tmp_path))
    result = Command(update={"messages": [ToolMessage(content="unchanged", tool_call_id="tc1")]})
    assert invoke(stack(), request(), lambda req: result) is result
    assert result.update["messages"][0].content == "unchanged"


@pytest.mark.parametrize("error,status", [(ValueError("broken"), "error"), (TimeoutError("slow"), "timeout"), (asyncio.CancelledError(), "cancelled")])
def test_errors_cancel_timeout_and_retries(tmp_path, monkeypatch, error, status):
    monkeypatch.setenv("DEER_FLOW_LLM_TRACE_DIR", str(tmp_path))

    class Retry(AgentMiddleware):
        async def awrap_tool_call(self, req, handler):
            try:
                return await handler(req)
            except BaseException:
                return await handler(req)

    async def actual(req):
        raise error

    async def run():
        with pytest.raises(type(error)):
            await ainvoke(stack([Retry()]), request(), actual)

    asyncio.run(run())
    rows = records(tmp_path)
    attempts = [r for r in rows if r["event"] == "tool_attempt_end"]
    assert len(attempts) == 2
    assert [r["attempt"] for r in attempts] == [1, 2]
    assert all(r["payload"]["status"] == status for r in attempts)
    assert next(r for r in rows if r["event"] == "tool_end")["payload"]["status"] == status


def test_concurrent_calls_do_not_cross_link(tmp_path, monkeypatch):
    monkeypatch.setenv("DEER_FLOW_LLM_TRACE_DIR", str(tmp_path))

    async def actual(req):
        await asyncio.sleep(0.001)
        return ToolMessage(content=req.tool_call["id"], tool_call_id=req.tool_call["id"])

    async def run():
        await asyncio.gather(*(ainvoke(stack(), request(str(i)), actual) for i in range(12)))

    asyncio.run(run())
    ends = [r for r in records(tmp_path) if r["event"] == "tool_end"]
    assert len(ends) == 12
    assert len({r["call_id"] for r in ends}) == 12
    assert all(r["payload"]["tool_call_id"] == r["payload"]["result"]["content"] for r in ends)


def test_unfinished_start_recovery_requires_confirmed_death(tmp_path, monkeypatch):
    monkeypatch.setenv("DEER_FLOW_LLM_TRACE_DIR", str(tmp_path))
    from deerflow.models import local_trace

    worker = multiprocessing.get_context("fork").Process(target=lambda: invoke(stack(), request(), lambda req: os._exit(23)))
    worker.start()
    worker.join(10)
    assert worker.exitcode == 23
    assert [r["event"] for r in records(tmp_path)] == ["tool_start", "tool_attempt_start"]
    with pytest.raises(ValueError):
        local_trace.mark_interrupted_calls(tmp_path, process_instance_id=local_trace._PROCESS_INSTANCE_ID)
    assert local_trace.mark_interrupted_calls(tmp_path, process_instance_id=local_trace._PROCESS_INSTANCE_ID, confirmed_stopped=True) == 1
    assert records(tmp_path)[-1]["event"] == "tool_interrupted"


def test_native_tool_node_return_direct_command(tmp_path, monkeypatch):
    monkeypatch.setenv("DEER_FLOW_LLM_TRACE_DIR", str(tmp_path))
    from langchain.agents import create_agent
    from langchain_core.language_models.fake_chat_models import FakeMessagesListChatModel
    from langchain_core.messages import AIMessage, HumanMessage
    from langchain_core.tools import StructuredTool

    class Model(FakeMessagesListChatModel):
        def bind_tools(self, tools, **kwargs):
            return self

    def actual(value: str):
        assert any(r["event"] == "tool_attempt_start" for r in records(tmp_path))
        return value

    tool = StructuredTool.from_function(actual, name="synthetic_echo", description="Synthetic echo", return_direct=True)
    model = Model(responses=[AIMessage(content="", tool_calls=[{"id": "native1", "name": "synthetic_echo", "args": {"value": "native-result"}}])])
    agent = create_agent(model, [tool], middleware=stack())
    result = asyncio.run(agent.ainvoke({"messages": [HumanMessage(content="synthetic")]}))
    assert result["messages"][-1].content == "native-result"
    ends = [r for r in records(tmp_path) if r["event"] == "tool_end"]
    assert len(ends) == 1
    assert ends[0]["payload"]["result"]["content"] == "native-result"


def test_trace_health_exposes_unfinished_execution(tmp_path, monkeypatch):
    monkeypatch.setenv("DEER_FLOW_LLM_TRACE_DIR", str(tmp_path))
    from deerflow.models import local_trace

    def actual(req):
        health = local_trace.trace_health(tmp_path)
        assert health["unfinished_tool_execution_count"] == 1
        assert not health["complete"]
        return ToolMessage(content="done", tool_call_id=req.tool_call["id"])

    invoke(stack(), request(), actual)
    assert local_trace.trace_health(tmp_path)["unfinished_tool_execution_count"] == 0


@pytest.mark.parametrize("command", [False, True])
def test_real_output_budget_preserves_linked_raw_and_visible(tmp_path, monkeypatch, command):
    from deerflow.agents.middlewares.tool_output_budget_middleware import ToolOutputBudgetMiddleware
    from deerflow.config.tool_output_config import ToolOutputConfig

    monkeypatch.setenv("DEER_FLOW_LLM_TRACE_DIR", str(tmp_path / "traces"))
    req = request()
    req.runtime.state = {"thread_data": {"outputs_path": str(tmp_path / "outputs")}}
    original = "synthetic unicode 🙂 data\n" * 1000
    message = ToolMessage(content=original, tool_call_id="tc1")
    value = Command(update={"messages": [message]}) if command else message
    budget = ToolOutputBudgetMiddleware(config=ToolOutputConfig(externalize_min_chars=100, preview_head_chars=0, preview_tail_chars=0))
    result = invoke(stack([budget]), req, lambda req: value)
    rows = records(tmp_path / "traces")
    raw = next(r["payload"]["result"] for r in rows if r["event"] == "tool_attempt_end")
    visible = next(r["payload"]["result"] for r in rows if r["event"] == "tool_end")
    if command:
        raw, visible = raw["update"]["messages"][0], visible["update"]["messages"][0]
        result = result.update["messages"][0]
    assert raw["content"] == original
    assert visible["content"] == result.content
    assert "Scripts can automatically sort/filter/classify" in visible["content"]
    assert len(visible["content"]) < len(original)


def test_start_write_failure_prevents_unlogged_execution(tmp_path, monkeypatch):
    monkeypatch.setenv("DEER_FLOW_LLM_TRACE_DIR", str(tmp_path))
    from deerflow.models import local_trace

    original = local_trace._write_record
    monkeypatch.setattr(local_trace, "_write_record", lambda *args: (_ for _ in ()).throw(OSError("synthetic disk failure")))
    try:
        with pytest.raises(RuntimeError, match="not be persisted"):
            invoke(stack(), request(), lambda req: pytest.fail("unlogged execution"))
    finally:
        monkeypatch.setattr(local_trace, "_write_record", original)
        local_trace._trace_gaps.clear()
