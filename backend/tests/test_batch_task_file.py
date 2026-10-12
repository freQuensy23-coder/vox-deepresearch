"""Sandbox manifest admission: real bounded file reads plus native submitter path."""

import asyncio
import importlib
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from deerflow.subagents.config import SubagentConfig

mod = importlib.import_module("deerflow.tools.builtins.batch_task_tool")


@pytest.fixture
def runtime(tmp_path):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    return SimpleNamespace(
        state={"thread_data": {"workspace_path": str(workspace), "outputs_path": str(tmp_path / "outputs")}},
        context={"thread_id": "thread-1", "user_id": "user-1", "run_id": "run-1"},
        config={"metadata": {}, "configurable": {"thread_id": "thread-1"}},
    )


@pytest.fixture
def local_reader(monkeypatch):
    from deerflow.authz import sandbox_authz
    from deerflow.sandbox import tools

    monkeypatch.setattr(tools, "is_local_sandbox", lambda _runtime: True)
    monkeypatch.setattr(sandbox_authz, "authorize_sandbox_execution", lambda **_kwargs: None)
    monkeypatch.setattr(sandbox_authz, "safe_app_config", lambda: None)


@pytest.fixture
def submitter(monkeypatch):
    instance = AsyncMock()
    instance.submit.return_value = {"id": "batch-1", "status": "queued", "total_items": 1000}
    monkeypatch.setattr(mod, "_batch_submitter", lambda: instance)
    monkeypatch.setattr(mod, "get_available_subagent_names", lambda **_kwargs: ["general-purpose"])
    monkeypatch.setattr(mod, "get_subagent_config", lambda *_args, **_kwargs: SubagentConfig(name="general-purpose", description="test"))
    return instance


def write_manifest(runtime, payload):
    from pathlib import Path

    path = Path(runtime.state["thread_data"]["workspace_path"]) / "items.json"
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return path


def invoke(runtime, **kwargs):
    return mod.batch_task.coroutine(runtime=runtime, title="Fan out", subagent_type="general-purpose", tool_call_id="call-1", **kwargs)


@pytest.mark.asyncio
async def test_thousand_manifest_items_reach_same_native_submission(runtime, local_reader, submitter, monkeypatch):
    payload = [{"key": str(i), "prompt": f"Изучи группу {i}", "acceptance_criteria": [f"file:/mnt/user-data/outputs/{i}.md non-empty"]} for i in range(1000)]
    write_manifest(runtime, payload)

    async def load(runtime, path):
        return await asyncio.to_thread(mod._read_batch_items_file, runtime, path)

    monkeypatch.setattr(mod, "_load_batch_items_file", load)
    response = await invoke(runtime, items_file="/mnt/user-data/workspace/items.json", max_live_items=1000, max_running_items=32)
    request = submitter.submit.await_args.args[0]
    assert request.items == payload
    assert (request.user_id, request.thread_id, request.run_id, request.submission_key) == ("user-1", "thread-1", "run-1", "run-1:call-1")
    assert (request.max_live_items, request.max_running_items) == (1000, 32)
    assert len(response.update["messages"][0].content) < 500


@pytest.mark.parametrize("path", ["/etc/passwd", "items.json", "/mnt/user-data/uploads/items.json", "/mnt/user-data/workspace/../uploads/items.json"])
def test_manifest_cannot_read_other_locations(runtime, local_reader, path):
    with pytest.raises(ValueError):
        mod._read_batch_items_file(runtime, path)


def test_manifest_symlink_escape_denied(runtime, local_reader, tmp_path):
    path = write_manifest(runtime, [])
    outside = tmp_path / "outside.json"
    outside.write_text('[{"key":"secret","prompt":"secret"}]', encoding="utf-8")
    path.unlink()
    path.symlink_to(outside)
    with pytest.raises(ValueError, match="outside"):
        mod._read_batch_items_file(runtime, "/mnt/user-data/workspace/items.json")


@pytest.mark.parametrize("payload", [[], {}, [{"key": "x"}], [{"key": "x", "prompt": 123}]])
def test_invalid_manifest_rejected_without_echo(runtime, local_reader, payload):
    write_manifest(runtime, payload)
    with pytest.raises(ValueError, match="JSON array"):
        mod._read_batch_items_file(runtime, "/mnt/user-data/workspace/items.json")


def test_oversized_manifest_rejected(runtime, local_reader, monkeypatch):
    write_manifest(runtime, [{"key": "x", "prompt": "x" * 100}])
    monkeypatch.setattr(mod, "_BATCH_ITEMS_FILE_MAX_BYTES", 32)
    with pytest.raises(ValueError, match="manifest limit"):
        mod._read_batch_items_file(runtime, "/mnt/user-data/workspace/items.json")


@pytest.mark.asyncio
@pytest.mark.parametrize("kwargs", [{}, {"items": [], "items_file": "/mnt/user-data/workspace/items.json"}])
async def test_exactly_one_source(runtime, submitter, kwargs):
    response = await invoke(runtime, **kwargs)
    assert response.update["messages"][0].status == "error"
    submitter.submit.assert_not_awaited()


@pytest.mark.asyncio
async def test_denied_subagent_does_not_read_file(runtime, submitter, monkeypatch):
    reader = AsyncMock()
    monkeypatch.setattr(mod, "_load_batch_items_file", reader)
    monkeypatch.setattr(mod, "get_available_subagent_names", lambda **_kwargs: [])
    response = await invoke(runtime, items_file="/mnt/user-data/workspace/items.json")
    assert response.update["messages"][0].status == "error"
    reader.assert_not_awaited()
    submitter.submit.assert_not_awaited()


@pytest.mark.asyncio
async def test_stale_thread_does_not_read_file(runtime, submitter, monkeypatch):
    from deerflow.mcp_scope import THREAD_INCARNATION_CONTEXT_KEY, THREAD_INCARNATION_METADATA_GUARD_KEY

    reader = AsyncMock()
    monkeypatch.setattr(mod, "_load_batch_items_file", reader)
    runtime.context.update({THREAD_INCARNATION_METADATA_GUARD_KEY: True, THREAD_INCARNATION_CONTEXT_KEY: "old"})
    runtime.config["metadata"][THREAD_INCARNATION_CONTEXT_KEY] = "new"
    with pytest.raises(RuntimeError, match="stale thread incarnation"):
        await invoke(runtime, items_file="/mnt/user-data/workspace/items.json")
    reader.assert_not_awaited()
    submitter.submit.assert_not_awaited()


@pytest.mark.asyncio
async def test_file_duplicate_keys_do_not_submit(runtime, submitter, monkeypatch):
    monkeypatch.setattr(mod, "_load_batch_items_file", AsyncMock(return_value=[mod.BatchTaskItem(key="same", prompt="one"), mod.BatchTaskItem(key="same", prompt="two")]))
    response = await invoke(runtime, items_file="/mnt/user-data/workspace/items.json")
    assert "unique" in response.update["messages"][0].content
    submitter.submit.assert_not_awaited()


@pytest.mark.asyncio
async def test_sandbox_authorization_precedes_initialization_and_file_read(runtime, monkeypatch):
    from deerflow.sandbox import tools

    authorize = AsyncMock(side_effect=PermissionError("revoked"))
    initialize = AsyncMock()
    monkeypatch.setattr(tools, "authorize_sandbox_execution_async", authorize)
    monkeypatch.setattr(tools, "safe_app_config_async", AsyncMock(return_value=None))
    monkeypatch.setattr(tools, "ensure_sandbox_initialized_async", initialize)
    with pytest.raises(PermissionError, match="revoked"):
        await mod._load_batch_items_file(runtime, "/mnt/user-data/workspace/items.json")
    initialize.assert_not_awaited()


@pytest.mark.parametrize("content", [b'[{"key":"x","prompt": NaN}]', b"\xff", b'[{"key":"x"'])
def test_nonstandard_and_incomplete_json_rejected(runtime, local_reader, content):
    path = write_manifest(runtime, [])
    path.write_bytes(content)
    with pytest.raises(ValueError, match="JSON array"):
        mod._read_batch_items_file(runtime, "/mnt/user-data/workspace/items.json")
