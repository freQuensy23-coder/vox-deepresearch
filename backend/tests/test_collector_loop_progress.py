import importlib.util
import json
import shlex
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from langchain_core.messages import AIMessage

from deerflow.agents.middlewares.loop_detection_middleware import LoopDetectionMiddleware


@pytest.fixture
def collector(tmp_path, monkeypatch):
    path = Path(__file__).resolve().parents[2] / "skills/public/telegram-research/scripts/collect_search.py"
    spec = importlib.util.spec_from_file_location("progress_collector", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    calls = []
    control = {"fail": False, "finish_at": 8}

    def invoke(pending, lock):
        calls.append(pending["window"])
        source = Path(pending["source_path"])
        if not control["fail"]:
            descending = pending["argv"][pending["argv"].index("--order") + 1] == "desc"
            cursor = module.timestamp(pending["window"]["to" if descending else "from"]) + timedelta(seconds=-10 if descending else 10)
            rows = [] if len(calls) >= control["finish_at"] else [{"id": len(calls), "group_id": -10011111, "date": cursor.isoformat(), "text": "SYNTHETIC"}]
            source.write_text(json.dumps({"messages": rows, "count": len(rows), "returned_count": len(rows), "stop_reason": "timeout" if rows else "finished", "stopped_at": cursor.isoformat()}), encoding="utf-8")
            Path(pending["stdout_path"]).write_text(str(source), encoding="utf-8")
        else:
            Path(pending["stdout_path"]).write_text("", encoding="utf-8")
        Path(pending["stderr_path"]).write_text("", encoding="utf-8")
        module.seal_receipt(pending, int(control["fail"]), False)

    monkeypatch.setattr(module, "invoke", invoke)
    workspace = tmp_path / "collection"
    argv = ["--query", "synthetic", "--from", "2026-01-01T00:00:00+00:00", "--to", "2026-01-02T00:00:00+00:00", "--workspace", str(workspace), "--max-pages", "1"]
    return module, argv, workspace, calls, control


def state(workspace):
    return json.loads((workspace / "manifest.json").read_text(encoding="utf-8"))


def admit(guard, runtime, argv, index):
    message = AIMessage(content="", tool_calls=[{"name": "bash", "args": {"command": shlex.join(["python", "collect_search.py", *argv]), "description": str(index)}, "id": str(index), "type": "tool_call"}])
    return guard.after_model({"messages": [message]}, runtime)


def guard_runtime():
    return LoopDetectionMiddleware(tool_freq_overrides={"bash": (80, 120)}), SimpleNamespace(context={"thread_id": "synthetic-thread", "run_id": "synthetic-run"})


@pytest.mark.parametrize("order", ["asc", "desc"])
def test_validated_cursor_allows_finite_progress_past_identical_limit(collector, order):
    module, argv, workspace, calls, control = collector
    argv.extend(["--order", order])
    boundary = "to" if order == "desc" else "from"
    assert module.main(argv) == 2
    originals = {p: p.read_bytes() for p in (workspace / "raw").iterdir()}
    guard, runtime = guard_runtime()
    for index in range(7):
        cursor = state(workspace)["remaining_window"][boundary]
        resume = [*argv, "--resume", "--expected-cursor", cursor]
        assert admit(guard, runtime, resume, index) is None
        assert module.main(resume) == (0 if index == 6 else 2)
    assert len(calls) == 8
    assert len({window[boundary] for window in calls}) == 8
    assert state(workspace)["traversal_complete"] is True
    assert state(workspace)["exhaustive"] is False
    assert runtime.context.get("stop_reason") is None
    assert all(p.read_bytes() == content for p, content in originals.items())


def test_unchanged_cursor_is_still_blocked_on_fifth_call(collector):
    module, argv, workspace, calls, control = collector
    assert module.main(argv) == 2
    control["fail"] = True
    cursor = state(workspace)["remaining_window"]["to"]
    resume = [*argv, "--resume", "--expected-cursor", cursor]
    guard, runtime = guard_runtime()
    for index in range(4):
        assert admit(guard, runtime, resume, index) is None
        assert module.main(resume) == 1
        assert state(workspace)["remaining_window"]["to"] == cursor
    result = admit(guard, runtime, resume, 4)
    assert result["messages"][0].tool_calls == []
    assert runtime.context["stop_reason"] == "loop_capped"
    assert len(calls) == 5


def test_legacy_resume_reproduces_false_loop_despite_cursor_progress(collector):
    module, argv, workspace, calls, control = collector
    assert module.main(argv) == 2
    guard, runtime = guard_runtime()
    resume = [*argv, "--resume"]
    for index in range(4):
        assert admit(guard, runtime, resume, index) is None
        assert module.main(resume) == 2
    result = admit(guard, runtime, resume, 4)
    assert result["messages"][0].tool_calls == []
    assert len({window["to"] for window in calls}) == 5
    assert state(workspace)["traversal_complete"] is False
    assert runtime.context["stop_reason"] == "loop_capped"


@pytest.mark.parametrize("cursor", ["not-a-date", "2026-01-01T01:00:00+00:00", "2026-01-02T00:00:00Z"])
def test_bad_cursor_fails_before_external_call_or_checkpoint_write(collector, cursor, capsys):
    module, argv, workspace, calls, control = collector
    assert module.main(argv) == 2
    before = {p: p.read_bytes() for p in workspace.rglob("*") if p.is_file()}
    assert module.main([*argv, "--resume", "--expected-cursor", cursor]) == 1
    assert "expected cursor" in capsys.readouterr().err.lower()
    assert len(calls) == 1
    assert {p: p.read_bytes() for p in workspace.rglob("*") if p.is_file()} == before


def test_cursor_requires_existing_resume(collector, capsys):
    module, argv, workspace, calls, control = collector
    assert module.main([*argv, "--expected-cursor", "2026-01-02T00:00:00+00:00"]) == 1
    assert "--resume" in capsys.readouterr().err
    assert calls == []
    assert not (workspace / "manifest.json").exists()


def test_stale_cursor_rejected_after_other_collector_advances(collector, capsys):
    module, argv, workspace, calls, control = collector
    assert module.main(argv) == 2
    resume = [*argv, "--resume", "--expected-cursor", state(workspace)["remaining_window"]["to"]]
    assert module.main(resume) == 2
    before = (workspace / "manifest.json").read_bytes()
    assert module.main(resume) == 1
    assert "expected cursor" in capsys.readouterr().err.lower()
    assert len(calls) == 2
    assert (workspace / "manifest.json").read_bytes() == before


def test_distinct_cursors_do_not_disable_frequency_hard_stop():
    guard, runtime = guard_runtime()
    for index in range(119):
        assert admit(guard, runtime, ["--resume", "--expected-cursor", str(index)], index) is None
    result = admit(guard, runtime, ["--resume", "--expected-cursor", "119"], 119)
    assert result["messages"][0].tool_calls == []
    assert "per-tool safety limit" in result["messages"][0].content
    assert runtime.context["stop_reason"] == "loop_capped"


@pytest.mark.parametrize("frequency", [False, True])
def test_forced_stop_does_not_claim_a_report_was_produced(frequency):
    guard = LoopDetectionMiddleware(hard_limit=5, tool_freq_hard_limit=5)
    runtime = SimpleNamespace(context={"thread_id": "synthetic-thread", "run_id": "synthetic-run"})
    result = None
    for index in range(5):
        result = admit(guard, runtime, [str(index)] if frequency else [], index)
    assert result is not None
    content = result["messages"][0].content
    assert "not a completed report" in content
    assert "Producing final answer" not in content
    assert result["messages"][0].tool_calls == []
    assert runtime.context["stop_reason"] == "loop_capped"
