import json
from types import SimpleNamespace

import pytest
from langchain_core.messages import ToolMessage
from langgraph.types import Command

from deerflow.agents.middlewares.tool_output_budget_middleware import ToolOutputBudgetMiddleware, _build_preview, _patch_model_messages
from deerflow.config.tool_output_config import ToolOutputConfig


@pytest.mark.parametrize(
    "content",
    [
        json.dumps({"items": [{"id": i} for i in range(100)]}),
        "id,value\n" + "1,value\n" * 100,
        "id\tvalue\n" + "1\tvalue\n" * 100,
        "---\nitems:\n" + "  - id: 1\n" * 100,
        "<items>" + "<item id='1'/>" * 100 + "</items>",
        "ordinary output\n" * 100,
        "\x00" + "x" * 1000,
    ],
    ids=["json", "csv", "tsv", "yaml", "xml", "text", "binary"],
)
@pytest.mark.parametrize("head,tail", [(0, 0), (20, 10)])
def test_preview_guides_full_file_scripting_outside_synopsis_budget(content, head, tail):
    preview = _build_preview(content, tool_name="query", virtual_path="/mnt/result.txt", head_chars=head, tail_chars=tail)
    access = preview.split("\n\nAccess:\n", 1)[1]
    assert "Scripts can automatically sort/filter/classify" in access
    assert "large volumes" in access
    assert "the full saved file, not this synopsis" in access
    assert "without loading all data into context" in access
    assert "Preserve raw data, IDs/provenance" in access
    assert "input/kept/excluded/unresolved counts" in access
    assert "never blindly discard classifier exclusions" in access
    assert access.endswith("Use read_file on /mnt/result.txt with start_line and end_line to inspect the raw output.")
    assert len(access) < 400


@pytest.mark.parametrize("mode", ["sync", "async", "command"])
@pytest.mark.asyncio
async def test_model_visible_notice_keeps_complete_source_and_accounting(tmp_path, mode):
    records = [{"id": i, "source": f"fixture://records/{i}", "text": "данные🙂" * 5} for i in range(100)]
    content = json.dumps(records, ensure_ascii=False)
    middleware = ToolOutputBudgetMiddleware(config=ToolOutputConfig(externalize_min_chars=100, preview_head_chars=0, preview_tail_chars=0))
    request = SimpleNamespace(tool_call={"name": "query", "id": "call-notice"}, runtime=SimpleNamespace(state={"thread_data": {"outputs_path": str(tmp_path)}}))
    original = ToolMessage(content=content, name="query", tool_call_id="call-notice", id="message-notice", artifact={"source": "fixture"})
    result = Command(update={"messages": [original], "other": "unchanged"}) if mode == "command" else original
    if mode == "async":

        async def handler(_):
            return result

        visible = await middleware.awrap_tool_call(request, handler)
    else:
        visible = middleware.wrap_tool_call(request, lambda _: result)
    if mode == "command":
        assert visible.update["other"] == "unchanged"
        visible = visible.update["messages"][0]
    assert "Scripts can automatically sort/filter/classify" in visible.content
    assert visible.tool_call_id == original.tool_call_id
    assert visible.id == original.id
    assert visible.artifact == original.artifact
    assert original.content == content
    files = list((tmp_path / ".tool-results").iterdir())
    assert len(files) == 1
    assert files[0].read_bytes() == content.encode("utf-8")
    assert json.loads(files[0].read_text(encoding="utf-8")) == records
    assert _patch_model_messages([visible], middleware._config) is None


def test_bash_script_notice_leaves_nonzero_exit_marker_last():
    preview = _build_preview("x" * 1000 + "\nExit Code: 7", tool_name="bash", virtual_path="/mnt/result.log", head_chars=0, tail_chars=0)
    assert "Scripts can automatically sort/filter/classify" in preview
    assert preview.endswith("\nExit Code: 7")


def test_unsaved_fallback_does_not_offer_nonexistent_full_file():
    middleware = ToolOutputBudgetMiddleware(config=ToolOutputConfig(externalize_min_chars=100, fallback_max_chars=500))
    request = SimpleNamespace(tool_call={"name": "query", "id": "call-unsaved"}, runtime=SimpleNamespace(state={}))
    original = ToolMessage(content="x" * 2000, name="query", tool_call_id="call-unsaved")
    visible = middleware.wrap_tool_call(request, lambda _: original)
    assert "Persistent storage unavailable" in visible.content
    assert "Scripts can automatically" not in visible.content
    assert "full saved file" not in visible.content
    assert len(visible.content) <= 500


def test_small_result_has_no_script_notice_or_saved_file(tmp_path):
    middleware = ToolOutputBudgetMiddleware(config=ToolOutputConfig())
    request = SimpleNamespace(tool_call={"name": "query", "id": "call-small"}, runtime=SimpleNamespace(state={"thread_data": {"outputs_path": str(tmp_path)}}))
    original = ToolMessage(content="small", name="query", tool_call_id="call-small")
    assert middleware.wrap_tool_call(request, lambda _: original) is original
    assert not (tmp_path / ".tool-results").exists()
