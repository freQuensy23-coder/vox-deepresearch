import asyncio
import json
import subprocess
from types import SimpleNamespace

from langchain.agents.middleware import AgentMiddleware
from langchain_core.messages import AIMessage, ToolMessage

from test_local_tool_trace import invoke, request, records, stack


def test_guard_raw_before_outer_transform(tmp_path, monkeypatch):
    monkeypatch.setenv("DEER_FLOW_LLM_TRACE_DIR", str(tmp_path))

    class Transform(AgentMiddleware):
        def wrap_tool_call(self, req, handler):
            result = handler(req)
            result.content = "visible"
            return result

    class Block(AgentMiddleware):
        def wrap_tool_call(self, req, handler):
            return ToolMessage(content="raw guard", tool_call_id="tc1")

    invoke(stack([Transform(), Block()]), request(), lambda req: None)
    rows = records(tmp_path)
    raw = next(r for r in rows if r["event"] == "tool_guard_result")
    assert raw["payload"]["result"]["content"] == "raw guard"
    end = next(r for r in rows if r["event"] == "tool_end")
    assert end["payload"]["result"]["content"] == "visible"
    assert end["payload"]["execution_duration_ns"] is None


def test_removed_proposals_are_not_executions(tmp_path, monkeypatch):
    monkeypatch.setenv("DEER_FLOW_LLM_TRACE_DIR", str(tmp_path))

    class Guard(AgentMiddleware):
        def after_model(self, state, runtime):
            return {"messages": [AIMessage(content="blocked", id="m1")]}

    chain = stack([Guard()])
    state = {"messages": [AIMessage(content="", id="m1", tool_calls=[{"id": "blocked1", "name": "bash", "args": {"command": "synthetic"}}])]}
    next(m for m in chain if isinstance(m, Guard)).after_model(state, request().runtime)
    rows = records(tmp_path)
    assert len(rows) == 1
    assert rows[0]["event"] == "tool_proposal_blocked"
    assert rows[0]["payload"]["execution_duration_ns"] is None
    assert rows[0]["payload"]["tool_call_id"] == "blocked1"
    assert rows[0]["run_id"] == "run-test"


def test_bash_trace_environment_is_scoped(tmp_path, monkeypatch):
    from deerflow.sandbox.tools import _execute_bash_command
    monkeypatch.setenv("DEER_FLOW_LLM_TRACE_DIR", str(tmp_path))
    received = []
    sandbox = SimpleNamespace(execute_command=lambda command, **kwargs: received.append((command, kwargs)) or "ok")
    req = request()

    def actual(req):
        _execute_bash_command(sandbox, "python -c 'import os; print(os.environ[\"VOX_TOOL_TRACE_CONTEXT\"])'; cd /", runtime=req.runtime, env=None)
        return ToolMessage(content="ok", tool_call_id="tc1")

    invoke(stack(), req, actual)
    command, options = received[0]
    assert options["env"] is None
    output = subprocess.check_output(["bash", "-c", command + '\npwd; printf "unset=%s" "${VOX_TOOL_TRACE_CONTEXT-unset}"'], text=True)
    lines = output.splitlines()
    ctx = json.loads(lines[0])
    assert lines[1:] == ["/", "unset=unset"]
    end = next(r for r in records(tmp_path) if r["event"] == "tool_end")
    assert ctx["execution_id"] == end["call_id"]
    assert ctx["tool_call_id"] == "tc1"
    assert ctx["run_id"] == "run-test"
    assert ctx["tool_attempt"] == 1
