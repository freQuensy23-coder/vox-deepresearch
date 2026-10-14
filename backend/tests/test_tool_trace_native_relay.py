import asyncio
import json
import os
import shlex
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[2]
LOCAL_DEPLOY = Path("/trace-local-deploy") if Path("/trace-local-deploy").exists() else ROOT / "local-deploy"
SCRIPTS = Path("/trace-skills") if Path("/trace-skills").exists() else ROOT / "skills/public/telegram-research/scripts"
sys.path.insert(0, str(LOCAL_DEPLOY))
from test_local_tool_trace import records, stack  # noqa: E402
from test_vox_relay import relays, upstream  # noqa: E402, F401
from test_vox_relay_observability import completed  # noqa: E402


def test_native_tool_cli_relay_receipt(tmp_path, monkeypatch, relays, upstream):  # noqa: F811
    from langchain.agents import create_agent
    from langchain_core.language_models.fake_chat_models import FakeMessagesListChatModel
    from langchain_core.messages import AIMessage, HumanMessage
    from langchain_core.tools import StructuredTool

    from deerflow.sandbox.tools import _execute_bash_command

    monkeypatch.setenv("DEER_FLOW_LLM_TRACE_DIR", str(tmp_path / "tools"))
    server = relays()
    monkeypatch.setenv("VOX_RELAY_URL", "http://127.0.0.1:" + str(server.server_address[1]))

    class Model(FakeMessagesListChatModel):
        def bind_tools(self, tools, **kwargs):
            return self

    sandbox = SimpleNamespace(execute_command=lambda command, **kwargs: subprocess.check_output(["bash", "-c", command], text=True, env=os.environ.copy(), cwd=tmp_path))
    runtime = SimpleNamespace(context={"run_id": "synthetic-run", "thread_id": "synthetic-thread", "task_id": "synthetic-child"})

    def actual(value: str):
        command = shlex.quote(sys.executable) + " " + shlex.quote(str(SCRIPTS / "vox.py")) + " search synthetic --since 2026-01-01"
        return _execute_bash_command(sandbox, command, runtime=runtime, env=None)

    tool = StructuredTool.from_function(actual, name="synthetic_vox", description="Synthetic local relay", return_direct=True)
    model = Model(responses=[AIMessage(content="", tool_calls=[{"id": "native-relay-1", "name": "synthetic_vox", "args": {"value": "synthetic"}}])])
    agent = create_agent(model, [tool], middleware=stack(), context_schema=dict)
    result = asyncio.run(agent.ainvoke({"messages": [HumanMessage(content="synthetic")]}, context=runtime.context))
    completed(server)
    tools = records(tmp_path / "tools")
    end = next(row for row in tools if row["event"] == "tool_end")
    relay_rows = [json.loads(path.read_text(encoding="utf-8")) for path in (server.state_dir / "traces").glob("*.json")]
    assert relay_rows
    assert all(row["tool_correlation"]["execution_id"] == end["call_id"] for row in relay_rows)
    assert all(row["tool_correlation"]["tool_call_id"] == "native-relay-1" for row in relay_rows)
    assert all(row["tool_correlation"]["child_call_id"] == "synthetic-child" for row in relay_rows)
    assert not any("x-vox-tool-trace" == key.lower() for _, _, headers, _ in upstream.requests for key in headers)
    assert end["payload"]["result"]["content"] == result["messages"][-1].content
    assert end["payload"]["status"] == "success"
    output = os.environ.get("TRACE_PROOF_DIR")
    if output:
        import shutil

        target = Path(output)
        target.mkdir(parents=True, exist_ok=True)
        shutil.copytree(tmp_path / "tools", target / "tools", dirs_exist_ok=True)
        (target / "receipt.json").write_text(
            json.dumps(
                {
                    "status": "passed",
                    "proof_type": "native_fake_model_real_tool_cli_http_relay_synthetic_upstream",
                    "execution_id": end["call_id"],
                    "tool_records": len(tools),
                    "upstream_requests": len(upstream.requests),
                    "external_research_calls": 0,
                }
            )
        )
