import hashlib
import importlib.util
import json
from pathlib import Path

from deerflow.models import local_trace

SCRIPT = Path(__file__).resolve().parents[2] / "scripts/migrate_llm_traces.py"
spec = importlib.util.spec_from_file_location("migrate_llm_traces", SCRIPT)
migrate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(migrate)


def old_call(root, owner, call_id, records):
    call = root / owner / hashlib.sha256(call_id.encode()).hexdigest()
    call.mkdir(parents=True)
    for i, record in enumerate(records):
        (call / f"{record['monotonic_ns']}-{i}.json").write_text(json.dumps(record))
    return call


def lines(path):
    return [json.loads(line) for line in path.read_text().splitlines()]


def test_session_names_match_the_tracer(tmp_path):
    for thread in ["8af199f0-711e-4988-a78e-e1d055353581", None, "a/b", ".hidden", "x" * 200]:
        assert migrate.session_name(thread) == local_trace.session_file(tmp_path, thread).name


def test_migration_moves_every_record_into_session_files_and_removes_the_old_ones(tmp_path):
    root, stage = tmp_path / "traces", tmp_path / "stage"
    first = old_call(root, "owner", "call-1", [{"event": "call_end", "monotonic_ns": 2, "thread_id": "t1"}, {"event": "call_start", "monotonic_ns": 1, "thread_id": "t1"}])
    second = old_call(root, "owner", "call-2", [{"event": "call_start", "monotonic_ns": 3, "thread_id": None}])
    (second / "9-x.pending").write_text("partial")
    broken = old_call(root, "owner", "call-3", [{"event": "call_start", "monotonic_ns": 4, "thread_id": "t1"}])
    (broken / "5-bad.json").write_text("{not json")
    assert migrate.migrate_call(first, stage) == (2, 0)
    assert migrate.migrate_call(second, stage) == (1, 0)
    assert migrate.migrate_call(broken, stage) == (1, 1)
    assert [r["event"] for r in lines(root / "owner/t1.jsonl")] == ["call_start", "call_end", "call_start"]
    assert [r["event"] for r in lines(root / "owner/no-thread.jsonl")] == ["call_start"]
    assert not first.exists() and not second.exists()
    assert [p.name for p in broken.iterdir()] == ["5-bad.json"]  # unreadable files stay for a human
    assert not list(stage.rglob("*.jsonl"))


def test_failed_append_keeps_the_stage_and_a_rerun_flushes_it(tmp_path, monkeypatch):
    root, stage = tmp_path / "traces", tmp_path / "stage"
    call = old_call(root, "owner", "call-1", [{"event": "call_start", "monotonic_ns": 1, "thread_id": "t1"}])
    real = migrate.append
    monkeypatch.setattr(migrate, "append", lambda *a: (_ for _ in ()).throw(OSError(122, "Disk quota exceeded")))
    assert isinstance(migrate._safe(migrate.migrate_call, call, stage), OSError)
    assert not call.exists() and len(list(stage.rglob("*.jsonl"))) == 1
    monkeypatch.setattr(migrate, "append", real)
    monkeypatch.setattr("sys.argv", ["migrate", str(root), "--stage", str(stage)])
    migrate.main()
    assert [r["event"] for r in lines(root / "owner/t1.jsonl")] == ["call_start"]
    assert not stage.exists()
