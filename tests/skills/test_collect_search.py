import hashlib
import json
import os
import shutil
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

SCRIPT = (
    Path(__file__).resolve().parents[2]
    / "skills/public/telegram-research/scripts/collect_search.py"
)
START = "2026-01-01T00:00:00Z"
END = "2026-01-02T00:00:00Z"

FIXTURE_CLI = """import json
import os
import signal
import sys
from datetime import datetime
from pathlib import Path

root = Path(__file__).parent
config = json.loads((root / "fixture.json").read_text())
argv = sys.argv[1:]
calls = root / "calls.jsonl"
prior = calls.read_text().splitlines() if calls.exists() else []
with calls.open("a") as handle:
    handle.write(json.dumps(argv) + "\\n")
index = len(prior)
if index in config.get("fail_at", []):
    print("synthetic network failure", file=sys.stderr)
    sys.exit(1)
def option(name):
    return argv[argv.index(name) + 1]
def date(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00"))
if "responses" in config:
    response = config["responses"][index]
else:
    low, high = date(option("--from")), date(option("--to"))
    rows = [r for r in config["rows"] if low <= date(r["date"]) <= high]
    rows.sort(key=lambda r: date(r["date"]), reverse=option("--order") == "desc")
    rows = rows[:int(option("--limit"))]
    response = {"messages": rows, "count": len(rows), "returned_count": len(rows), "stop_reason": "finished"}
path = Path(option("--output"))
with path.open("x") as handle:
    handle.write(json.dumps(response))
print(path, flush=True)
if index in config.get("interrupt_at", []):
    os.kill(os.getppid(), signal.SIGTERM)
"""


def message(index, second=None, group=-10011111):
    return {
        "id": index,
        "group_id": group,
        "date": (
            datetime(2026, 1, 1, tzinfo=timezone.utc)
            + timedelta(seconds=second if second is not None else index * 2)
        ).isoformat(),
        "text": f"OBVIOUSLY SYNTHETIC fixture message {index}",
    }


def response(rows, reason="finished", cursor=None, **extra):
    result = {
        "messages": rows,
        "count": len(rows),
        "returned_count": len(rows),
        "stop_reason": reason,
    }
    if cursor is not None:
        result["stopped_at"] = cursor
    return {**result, **extra}


@pytest.fixture
def harness(tmp_path):
    root = tmp_path / "synthetic-cli"
    root.mkdir()
    (root / "vox.py").write_text(FIXTURE_CLI)
    config = root / "fixture.json"
    workspace = tmp_path / "collection"

    def run(data=None, *extra):
        assert SCRIPT.is_file(), "optional date-pagination collector is missing"
        shutil.copyfile(SCRIPT, root / "collect_search.py")
        if data is not None:
            config.write_text(json.dumps(data))
        result = subprocess.run(
            [
                sys.executable,
                str(root / "collect_search.py"),
                "--query",
                "synthetic",
                "--from",
                START,
                "--to",
                END,
                "--workspace",
                str(workspace),
                "--limit",
                "100",
                *extra,
            ],
            capture_output=True,
            check=False,
            text=True,
            timeout=30,
            env={**os.environ, "VOX_API_KEY": "synthetic-never-sent"},
        )
        return result

    return run, workspace, root


def manifest(workspace):
    return json.loads((workspace / "manifest.json").read_text())


def calls(root):
    path = root / "calls.jsonl"
    return (
        [json.loads(line) for line in path.read_text().splitlines()]
        if path.exists()
        else []
    )


def test_collects_1000_unique_over_false_finished_caps_and_overlap(harness):
    run, workspace, root = harness
    rows = [message(i) for i in range(1, 1001)]
    rows.insert(23, dict(rows[22]))
    result = run({"rows": rows}, "--max-pages", "30")
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == str(workspace / "manifest.json")
    saved = manifest(workspace)
    assert saved["status"] == "traversed"
    assert saved["traversal_complete"] is True
    assert saved["exhaustive"] is False
    assert saved["scope"] == "indexed_query_only"
    assert saved["unique_count"] == 1000
    output = json.loads(Path(saved["messages_path"]).read_text())
    assert len({(r["group_id"], r["id"]) for r in output}) == 1000
    assert len(calls(root)) > 10
    assert any(p["classification"] == "saturated" for p in saved["pages"])
    for page in saved["pages"]:
        source = Path(page["source_path"])
        assert source.is_file()
        assert hashlib.sha256(source.read_bytes()).hexdigest() == page["source_sha256"]
        assert Path(page["stdout_path"]).read_text().strip() == str(source)


@pytest.mark.parametrize("order", ["asc", "desc"])
def test_page_budget_resume_preserves_filters_and_never_repeats_pages(harness, order):
    run, workspace, root = harness
    card = root / "public.json"
    card.write_text(
        json.dumps(
            {
                "id": -10011111,
                "alias": "synthetic",
                "is_private": False,
                "type": "CHANNEL",
            }
        )
    )
    filters = [
        "--order",
        order,
        "--type",
        "post",
        "--include",
        "@synthetic",
        "--public-card",
        str(card),
        "--exclude",
        "-10022222",
        "--exclude-users",
        "777",
        "--no-forwards",
        "--dedup",
        "--stream",
        "false",
        "--server-timeout",
        "20",
    ]
    result = run(
        {"rows": [message(i) for i in range(1, 251)]}, *filters, "--max-pages", "1"
    )
    assert result.returncode == 2
    first = manifest(workspace)
    assert first["status"] == "paused"
    assert first["traversal_complete"] is False
    old_raw = Path(first["pages"][0]["source_path"]).read_bytes()
    result = run(None, *filters, "--max-pages", "10", "--resume")
    assert result.returncode == 0, result.stderr
    saved = manifest(workspace)
    assert saved["unique_count"] == 250
    assert Path(first["pages"][0]["source_path"]).read_bytes() == old_raw
    argv = calls(root)
    windows = [(a[a.index("--from") + 1], a[a.index("--to") + 1]) for a in argv]
    assert len(windows) == len(set(windows))
    for a in argv:
        for flag in [
            "--order",
            "--type",
            "--include",
            "--public-card",
            "--exclude",
            "--exclude-users",
            "--stream",
        ]:
            assert a[a.index(flag) + 1] == filters[filters.index(flag) + 1]
        assert a[a.index("--timeout") + 1] == "20"
        assert "--no-forwards" in a and "--dedup" in a


def test_resume_rejects_parameter_or_card_changes(harness):
    run, workspace, root = harness
    assert (
        run(
            {"rows": [message(i) for i in range(1, 201)]}, "--max-pages", "1"
        ).returncode
        == 2
    )
    before = (workspace / "manifest.json").read_bytes()
    result = run(None, "--resume", "--type", "comment")
    assert result.returncode == 1
    assert "parameter" in result.stderr.lower()
    assert len(calls(root)) == 1
    assert (workspace / "manifest.json").read_bytes() == before


def test_interrupted_completed_response_is_recovered_without_refetch(harness):
    run, workspace, root = harness
    rows = [message(i) for i in range(1, 201)]
    result = run({"rows": rows, "interrupt_at": [0]}, "--max-pages", "1")
    assert result.returncode != 0
    assert len(calls(root)) == 1
    result = run(None, "--resume", "--max-pages", "10")
    assert result.returncode == 0, result.stderr
    assert manifest(workspace)["unique_count"] == 200
    argv = calls(root)
    assert argv[0][argv[0].index("--to") + 1] != argv[1][argv[1].index("--to") + 1]


@pytest.mark.parametrize(
    "bad",
    [
        {"messages": [], "count": 0},
        response([message(1)], count=2),
        response([message(1)], returned_count=0),
        response([message(1)], count=True),
        response([{"id": 1, "group_id": -10011111, "date": "invalid"}]),
        response([message(1, second=90000)]),
        response([], "timeout", "2026-01-01T12:00:00Z"),
        response([message(1)], "timeout", "2025-01-01T00:00:00Z"),
        response([message(1)], "timeout", END),
        response([message(1)], "timeout"),
        response([message(1)], "unknown"),
        response([message(1)], stop_reason=["finished"]),
        response([message(1)], stop_reason={"reason": "finished"}),
        response([None]),
        response([message(1), message(2)]),
        response([message(1)], "timeout", 123),
        [],
    ],
)
def test_invalid_incomplete_or_out_of_window_does_not_close_coverage(harness, bad):
    run, workspace, root = harness
    result = run({"responses": [bad]})
    assert result.returncode == 1, result.stderr
    saved = manifest(workspace)
    assert saved["status"] == "unresolved"
    assert saved["traversal_complete"] is False
    assert not saved["coverage"]
    assert len(saved["pages"]) == 1
    assert len(calls(root)) == 1


def test_saturated_timestamp_is_unresolved_not_skipped(harness):
    run, workspace, root = harness
    result = run({"rows": [message(i, second=100) for i in range(1, 301)]})
    assert result.returncode == 1
    saved = manifest(workspace)
    assert saved["traversal_complete"] is False
    assert saved["status"] == "unresolved"
    assert "progress" in saved["reason"] or "timestamp" in saved["reason"]
    assert saved["unique_count"] == 100
    assert len(calls(root)) <= 2


def test_partial_cursor_remains_inside_window_and_overlaps(harness):
    run, workspace, root = harness
    partial = response([message(2, second=2000)], "timeout", "2026-01-01T00:20:00Z")
    final = response([message(2, second=2000), message(1, second=100)])
    result = run({"responses": [partial, final]})
    assert result.returncode == 0, result.stderr
    saved = manifest(workspace)
    assert saved["unique_count"] == 2
    assert saved["pages"][0]["classification"] == "partial"
    boundary = calls(root)[1]
    assert datetime.fromisoformat(
        boundary[boundary.index("--to") + 1]
    ) > datetime.fromisoformat(message(2, second=2000)["date"])


def test_network_error_stops_without_collector_retry_then_resumes(harness):
    run, workspace, root = harness
    result = run({"rows": [message(i) for i in range(1, 201)], "fail_at": [1]})
    assert result.returncode == 1
    assert len(calls(root)) == 2
    assert manifest(workspace)["status"] == "error"
    result = run(None, "--resume")
    assert result.returncode == 0, result.stderr
    assert manifest(workspace)["unique_count"] == 200
    assert calls(root)[1][:-2] == calls(root)[2][:-2]


def test_existing_workspace_requires_explicit_resume(harness):
    run, _workspace, root = harness
    assert run({"rows": [message(1)]}).returncode == 0
    assert run(None).returncode == 1
    assert len(calls(root)) == 1


@pytest.mark.parametrize("order", ["asc", "desc"])
def test_boundary_ties_survive_exclusive_date_filters(harness, order):
    run, workspace, root = harness
    cli = root / "vox.py"
    cli.write_text(
        cli.read_text().replace(
            'low <= date(r["date"]) <= high', 'low < date(r["date"]) < high'
        )
    )
    rows = [message(i, second=(i // 7 + 1) * 3) for i in range(1, 1001)]
    result = run({"rows": rows}, "--order", order, "--max-pages", "30")
    assert result.returncode == 0, result.stderr
    assert manifest(workspace)["unique_count"] == 1000


def test_resume_rejects_modified_card_contents(harness):
    run, _workspace, root = harness
    card = root / "public.json"
    card.write_text("{}")
    assert (
        run(
            {"rows": [message(i) for i in range(1, 201)]},
            "--public-card",
            str(card),
            "--max-pages",
            "1",
        ).returncode
        == 2
    )
    card.write_text('{"changed": true}')
    result = run(None, "--resume", "--public-card", str(card))
    assert result.returncode == 1
    assert len(calls(root)) == 1


def test_resume_rejects_mutated_immutable_source(harness):
    run, workspace, root = harness
    assert (
        run(
            {"rows": [message(i) for i in range(1, 201)]}, "--max-pages", "1"
        ).returncode
        == 2
    )
    source = Path(manifest(workspace)["pages"][0]["source_path"])
    source.chmod(0o600)
    source.write_text("{}")
    result = run(None, "--resume")
    assert result.returncode == 1
    assert "checksum" in result.stderr
    assert len(calls(root)) == 1


@pytest.mark.parametrize("size", [50, 62, 1285, 1999, 2000, 2001])
def test_saturation_uses_requested_limit_instead_of_old_50_message_cap(harness, size):
    run, workspace, root = harness
    rows = [message(i) for i in range(size, 0, -1)]
    result = run({"responses": [response(rows)]}, "--limit", "2000", "--max-pages", "1")
    saturated = size >= 2000
    assert result.returncode == (2 if saturated else 0), result.stderr
    saved = manifest(workspace)
    assert saved["parameters"]["saturation_threshold"] == 2000
    assert saved["pages"][0]["classification"] == (
        "saturated" if saturated else "provider_finished"
    )
    assert saved["traversal_complete"] is not saturated
    assert saved["unique_count"] == size
    assert len(calls(root)) == 1


def test_resume_normalizes_legacy_derived_saturation_threshold(harness):
    run, workspace, root = harness
    rows = [message(i) for i in range(2000, 0, -1)]
    assert (
        run(
            {"responses": [response(rows), response([rows[-1]])]},
            "--limit",
            "2000",
            "--max-pages",
            "1",
        ).returncode
        == 2
    )
    saved = manifest(workspace)
    source = Path(saved["pages"][0]["source_path"]).read_bytes()
    saved["parameters"]["saturation_threshold"] = 50
    saved["limitations"][-1] = "The OpenAPI description says cap 50 while its schema allows 10000; saturation conservatively uses min(limit, 50)."
    original_limitations = list(saved["limitations"])
    encoded_parameters = (
        json.dumps(saved["parameters"], ensure_ascii=False, sort_keys=True, indent=2)
        + "\n"
    ).encode()
    saved["parameter_hash"] = hashlib.sha256(encoded_parameters).hexdigest()
    (workspace / "manifest.json").write_text(json.dumps(saved), encoding="utf-8")
    result = run(None, "--limit", "2000", "--resume")
    assert result.returncode == 0, result.stderr
    resumed = manifest(workspace)
    assert resumed["parameters"]["saturation_threshold"] == 2000
    assert resumed["limitations"][:-1] == original_limitations[:-1]
    assert resumed["limitations"][-1].startswith("Saturation uses the requested per-page limit;")
    assert resumed["pages"][-1]["classification"] == "provider_finished"
    assert resumed["unique_count"] == 2000
    assert Path(saved["pages"][0]["source_path"]).read_bytes() == source
    assert len(calls(root)) == 2


def test_changed_public_card_between_pages_stops_before_next_call(harness):
    run, workspace, root = harness
    card = root / "public.json"
    card.write_text("{}")
    cli = root / "vox.py"
    cli.write_text(
        cli.read_text() + '\n(root / "public.json").write_text(\'{"changed":true}\')\n'
    )
    result = run(
        {"rows": [message(i) for i in range(1, 201)]}, "--public-card", str(card)
    )
    assert result.returncode == 1
    assert len(calls(root)) == 1
    assert manifest(workspace)["status"] == "error"


def test_receipts_are_immutable_and_bind_source_hash(harness):
    run, workspace, _root = harness
    assert run({"rows": [message(1)]}).returncode == 0
    page = manifest(workspace)["pages"][0]
    receipt = json.loads(Path(page["receipt_path"]).read_text())
    assert receipt["source_sha256"] == page["source_sha256"]
    for key in ("source_path", "stdout_path", "stderr_path", "receipt_path"):
        assert Path(page[key]).stat().st_mode & 0o222 == 0


def test_same_message_id_in_different_groups_is_not_deduplicated(harness):
    run, workspace, _root = harness
    result = run({"rows": [message(1), message(1, group=-10022222)]})
    assert result.returncode == 0, result.stderr
    assert manifest(workspace)["unique_count"] == 2


@pytest.mark.parametrize("size", [65, 89])
@pytest.mark.parametrize("order", ["asc", "desc"])
def test_oversized_finished_page_preserves_all_rows_and_continues(harness, size, order):
    run, workspace, root = harness
    rows = [message(i) for i in range(1, size + 1)]
    rows.sort(key=lambda row: row["date"], reverse=order == "desc")
    first = response(
        rows,
        cursor=rows[-1]["date"],
        group_summary=[{"group_id": -10011111, "message_indices": list(range(size))}],
        ungrouped_count=0,
        public_access={"synthetic_fixture": True},
    )
    options = ["--limit", "50", "--order", order, "--max-pages", "1"]
    result = run({"responses": [first, response([rows[-1]])]}, *options)
    assert result.returncode == 2, result.stderr
    saved = manifest(workspace)
    assert saved["unique_count"] == size
    assert saved["pages"][0]["accepted"] is True
    assert saved["pages"][0]["classification"] == "saturated"
    assert saved["traversal_complete"] is False
    assert saved["exhaustive"] is False
    assert saved["coverage"][0]["exhaustive"] is False
    edge = datetime.fromisoformat(rows[-1]["date"])
    bound = "to" if order == "desc" else "from"
    assert datetime.fromisoformat(saved["remaining_window"][bound]) == edge + timedelta(
        seconds=1 if order == "desc" else -1
    )
    page = saved["pages"][0]
    immutable = {key: Path(page[key]).read_bytes() for key in (
        "source_path", "stdout_path", "stderr_path", "receipt_path"
    )}
    assert json.loads(immutable["source_path"]) == first
    assert json.loads(Path(saved["messages_path"]).read_text()) == rows
    assert run(None, *options, "--resume").returncode == 0
    resumed = manifest(workspace)
    assert resumed["unique_count"] == size
    assert resumed["status"] == "traversed"
    assert resumed["exhaustive"] is False
    assert len(calls(root)) == 2
    for key, content in immutable.items():
        assert Path(page[key]).read_bytes() == content
        assert Path(page[key]).stat().st_mode & 0o222 == 0


@pytest.mark.parametrize(
    "fault,reason,accepted",
    [
        ("count", "Inconsistent response counts", False),
        ("returned_count", "Inconsistent response counts", False),
        ("identity", "Invalid message identity", False),
        ("date", "Invalid date or cursor", False),
        ("window", "Out-of-window message", False),
        ("order", "Response dates contradict", False),
        ("stop_reason", "Invalid response stop_reason", False),
        ("tie", "Saturated same timestamp", True),
        ("cursor", "Out-of-window partial cursor", True),
    ],
)
def test_oversized_page_keeps_validation_and_coverage_guards(harness, fault, reason, accepted):
    run, workspace, root = harness
    rows = [message(i) for i in range(65, 0, -1)]
    data = response(rows)
    if fault in {"count", "returned_count"}:
        data[fault] -= 1
    elif fault == "identity":
        rows[-1]["group_id"] = 1
    elif fault == "date":
        rows[-1]["date"] = "invalid"
    elif fault == "window":
        rows[-1]["date"] = "2025-12-31T00:00:00Z"
    elif fault == "order":
        rows[-1], rows[-2] = rows[-2], rows[-1]
    elif fault == "stop_reason":
        data["stop_reason"] = "unknown"
    elif fault == "tie":
        for row in rows:
            row["date"] = rows[0]["date"]
    elif fault == "cursor":
        data.update(stop_reason="timeout", stopped_at="2025-12-31T00:00:00Z")
    result = run({"responses": [data]}, "--limit", "50")
    assert result.returncode == 1
    saved = manifest(workspace)
    assert reason in saved["reason"]
    assert saved["status"] == "unresolved"
    assert saved["traversal_complete"] is False
    assert saved["exhaustive"] is False
    assert saved["coverage"] == []
    assert saved["unique_count"] == (65 if accepted else 0)
    assert saved["pages"][0].get("accepted", False) is accepted
    assert json.loads(Path(saved["pages"][0]["source_path"]).read_text()) == data
    assert len(calls(root)) == 1


def test_interrupted_oversized_page_resume_uses_saved_response(harness):
    run, workspace, root = harness
    rows = [message(i) for i in range(65, 0, -1)]
    result = run(
        {"responses": [response(rows), response([rows[-1]])], "interrupt_at": [0]},
        "--limit", "50",
    )
    assert result.returncode != 0
    pending = manifest(workspace)["pending"]
    original = Path(pending["source_path"]).read_bytes()
    assert run(None, "--limit", "50", "--resume").returncode == 0
    saved = manifest(workspace)
    assert saved["unique_count"] == 65
    assert saved["pages"][0]["classification"] == "saturated"
    assert saved["pages"][0]["recovered_after_interruption"] is True
    assert saved["exhaustive"] is False
    assert Path(pending["source_path"]).read_bytes() == original
    assert len(calls(root)) == 2
    argv = calls(root)
    assert argv[0][argv[0].index("--to") + 1] != argv[1][argv[1].index("--to") + 1]


def legacy_rejected_page(harness, data, order="desc"):
    run, workspace, root = harness
    result = run(
        {"responses": [data, response([data["messages"][-1]])]},
        "--limit", "50", "--order", order, "--max-pages", "1",
    )
    assert result.returncode in (0, 1, 2), result.stderr
    saved = manifest(workspace)
    page = saved["pages"][-1]
    page.pop("accepted", None)
    page["classification"] = "unresolved"
    saved.update(
        status="unresolved", reason="Invalid response limit or stop_reason.",
        unique_count=0, coverage=[], remaining_window=dict(page["window"]),
        traversal_complete=False,
    )
    (workspace / "manifest.json").write_text(json.dumps(saved))
    (workspace / "messages.json").write_text("[]")
    return saved


@pytest.mark.parametrize("size", [65, 89])
@pytest.mark.parametrize("order", ["asc", "desc"])
def test_legacy_rejected_page_resume_revalidates_without_initial_refetch(harness, size, order):
    run, workspace, root = harness
    rows = [message(i) for i in range(1, size + 1)]
    rows.sort(key=lambda row: row["date"], reverse=order == "desc")
    saved = legacy_rejected_page(harness, response(rows), order)
    page = saved["pages"][0]
    original = {key: Path(page[key]).read_bytes() for key in (
        "source_path", "stdout_path", "stderr_path", "receipt_path"
    )}
    result = run(None, "--limit", "50", "--order", order, "--max-pages", "1", "--resume")
    assert result.returncode == 0, result.stderr
    resumed = manifest(workspace)
    assert resumed["unique_count"] == size
    assert resumed["pages"][0] == page
    assert len(resumed["pages"]) == 3
    recovery = resumed["pages"][1]
    assert recovery["revalidated_from_page"] == 0
    assert recovery["source_path"] == page["source_path"]
    assert recovery["source_sha256"] == page["source_sha256"]
    assert recovery["receipt_sha256"] == hashlib.sha256(original["receipt_path"]).hexdigest()
    assert recovery["classification"] == "saturated"
    assert recovery["accepted"] is True
    assert resumed["traversal_complete"] is True
    assert resumed["exhaustive"] is False
    assert len(resumed["coverage"]) == 2
    assert len(calls(root)) == 2
    bound = "to" if order == "desc" else "from"
    edge = datetime.fromisoformat(rows[-1]["date"])
    narrowed = (edge + timedelta(seconds=1 if order == "desc" else -1)).isoformat()
    assert calls(root)[1][calls(root)[1].index("--" + bound) + 1] == narrowed
    assert json.loads((workspace / "messages.json").read_text()) == rows
    assert run(None, "--limit", "50", "--order", order, "--resume").returncode == 0
    assert manifest(workspace) == resumed
    assert len(calls(root)) == 2
    for key, content in original.items():
        assert Path(page[key]).read_bytes() == content
        assert Path(page[key]).stat().st_mode & 0o222 == 0


@pytest.mark.parametrize("size", [65, 89])
def test_legacy_recovery_integrates_with_real_cli_and_in_memory_transport(harness, size):
    run, workspace, root = harness
    cli = SCRIPT.with_name("vox.py")
    (root / "vox.py").write_text(f"""import importlib.util
import io
import json
import os
import socket
import sys
from pathlib import Path

root = Path(__file__).parent
os.environ["VOX_RELAY_URL"] = "http://vox-relay:8083"
spec = importlib.util.spec_from_file_location("offline_vox", {str(cli)!r})
vox = importlib.util.module_from_spec(spec)
spec.loader.exec_module(vox)
def forbidden_network(*args, **kwargs):
    raise AssertionError("Network is forbidden in this offline integration test")
socket.create_connection = forbidden_network
def open_response(req, timeout):
    assert req.full_url.startswith("http://vox-relay:8083/search/text?")
    config = json.loads((root / "fixture.json").read_text())
    calls = root / "calls.jsonl"
    prior = calls.read_text().splitlines() if calls.exists() else []
    with calls.open("a") as handle:
        handle.write(json.dumps(sys.argv[1:]) + "\\n")
    return io.BytesIO(json.dumps(config["responses"][len(prior)]).encode())
vox.RELAY_OPENER.open = open_response
raise SystemExit(vox.main(sys.argv[1:]) or 0)
""")
    rows = [message(i) for i in range(size, 0, -1)]
    saved = legacy_rejected_page(harness, response(rows))
    original = Path(saved["pages"][0]["source_path"]).read_bytes()
    result = run(None, "--limit", "50", "--resume")
    assert result.returncode == 0, result.stderr
    resumed = manifest(workspace)
    assert resumed["unique_count"] == size
    assert len(calls(root)) == 2
    assert resumed["pages"][0] == saved["pages"][0]
    assert Path(saved["pages"][0]["source_path"]).read_bytes() == original
    assert calls(root)[1][calls(root)[1].index("--to") + 1] == (
        datetime.fromisoformat(rows[-1]["date"]) + timedelta(seconds=1)
    ).isoformat()
    assert resumed["pages"][1]["classification"] == "saturated"
    assert resumed["exhaustive"] is False


def test_legacy_rejected_unsaturated_page_resume_completes_without_fetch(harness):
    run, workspace, root = harness
    saved = legacy_rejected_page(harness, response([message(1)]))
    page = saved["pages"][0]
    result = run(None, "--limit", "50", "--resume")
    assert result.returncode == 0, result.stderr
    resumed = manifest(workspace)
    assert resumed["status"] == "traversed"
    assert resumed["remaining_window"] is None
    assert resumed["unique_count"] == 1
    assert resumed["pages"][0] == page
    assert resumed["pages"][1]["classification"] == "provider_finished"
    assert len(resumed["coverage"]) == 1
    assert len(calls(root)) == 1


def test_pre_automatic_search_manifest_revalidates_legacy_command(harness):
    run, workspace, root = harness
    saved = legacy_rejected_page(harness, response([message(i) for i in range(65, 0, -1)]))
    page = saved["pages"][0]
    page["argv"][2] = "search"
    receipt_path = Path(page["receipt_path"])
    receipt = json.loads(receipt_path.read_text())
    receipt["argv"] = page["argv"]
    receipt_path.chmod(0o600)
    receipt_path.write_text(json.dumps(receipt))
    receipt_path.chmod(0o444)
    (workspace / "manifest.json").write_text(json.dumps(saved))
    result = run(None, "--limit", "50", "--resume")
    assert result.returncode == 0, result.stderr
    assert manifest(workspace)["unique_count"] == 65
    assert len(calls(root)) == 2


def test_legacy_recovery_survives_page_budget_and_aggregate_loss(harness):
    run, workspace, root = harness
    first = response([message(i) for i in range(164, 99, -1)])
    saved = legacy_rejected_page(harness, first)
    (root / "fixture.json").write_text(json.dumps({"responses": [
        first, response([message(i) for i in range(65, 0, -1)]), response([message(1)]),
    ]}))
    assert run(None, "--limit", "50", "--max-pages", "1", "--resume").returncode == 2
    paused = manifest(workspace)
    assert paused["unique_count"] == 130
    assert paused["pages"][0] == saved["pages"][0]
    assert len(paused["pages"]) == 3
    assert len(paused["coverage"]) == 2
    (workspace / "messages.json").write_text("[]")
    assert run(None, "--limit", "50", "--resume").returncode == 0
    resumed = manifest(workspace)
    assert resumed["unique_count"] == 130
    assert resumed["pages"][:3] == paused["pages"]
    assert len(resumed["coverage"]) == 3
    assert len([p for p in resumed["pages"] if "revalidated_from_page" in p]) == 1
    assert len(calls(root)) == 3


def test_legacy_recovery_preserves_preceding_accepted_pages(harness):
    run, workspace, root = harness
    first = response([message(i) for i in range(264, 199, -1)])
    rejected = response([message(i) for i in range(164, 99, -1)])
    assert run(
        {"responses": [first, rejected, response([message(100)])]},
        "--limit", "50", "--max-pages", "2",
    ).returncode == 2
    saved = manifest(workspace)
    page = saved["pages"][1]
    page.pop("accepted")
    page["classification"] = "unresolved"
    saved.update(
        status="unresolved", reason="Invalid response limit or stop_reason.",
        remaining_window=dict(page["window"]), unique_count=65,
        coverage=saved["coverage"][:1],
    )
    (workspace / "manifest.json").write_text(json.dumps(saved))
    (workspace / "messages.json").write_text("[]")
    assert run(None, "--limit", "50", "--resume").returncode == 0
    resumed = manifest(workspace)
    assert resumed["unique_count"] == 130
    assert resumed["pages"][:2] == saved["pages"]
    assert resumed["coverage"][0] == saved["coverage"][0]
    assert resumed["pages"][2]["revalidated_from_page"] == 1
    assert len(resumed["coverage"]) == 3
    assert len(calls(root)) == 3


def test_revalidated_receipt_hash_is_checked_on_repeat_resume(harness):
    run, workspace, root = harness
    legacy_rejected_page(harness, response([message(i) for i in range(65, 0, -1)]))
    assert run(None, "--limit", "50", "--resume").returncode == 0
    saved = manifest(workspace)
    receipt = Path(saved["pages"][1]["receipt_path"])
    receipt.chmod(0o600)
    receipt.write_text("{}")
    before = (workspace / "manifest.json").read_bytes()
    result = run(None, "--limit", "50", "--resume")
    assert result.returncode == 1
    assert "checksum" in result.stderr
    assert len(calls(root)) == 2
    assert (workspace / "manifest.json").read_bytes() == before


@pytest.mark.parametrize("fault", [
    "count", "returned_count", "identity", "date", "window", "order",
    "stop_reason", "tie", "cursor",
])
def test_legacy_rejected_page_still_invalid_stops_without_fetch(harness, fault):
    run, workspace, root = harness
    rows = [message(i) for i in range(65, 0, -1)]
    data = response(rows)
    if fault in {"count", "returned_count"}:
        data[fault] -= 1
    elif fault == "identity":
        rows[-1]["group_id"] = 1
    elif fault == "date":
        rows[-1]["date"] = "invalid"
    elif fault == "window":
        rows[-1]["date"] = "2025-12-31T00:00:00Z"
    elif fault == "order":
        rows[-1], rows[-2] = rows[-2], rows[-1]
    elif fault == "stop_reason":
        data["stop_reason"] = "unknown"
    elif fault == "tie":
        for row in rows:
            row["date"] = rows[0]["date"]
    elif fault == "cursor":
        data.update(stop_reason="timeout", stopped_at="2025-12-31T00:00:00Z")
    saved = legacy_rejected_page(harness, data)
    result = run(None, "--limit", "50", "--resume")
    assert result.returncode == 1
    assert len(calls(root)) == 1
    resumed = manifest(workspace)
    assert resumed["status"] == "unresolved"
    assert resumed["pages"] == saved["pages"]
    assert resumed["coverage"] == []
    assert resumed["unique_count"] == 0
    assert resumed["traversal_complete"] is False
    assert resumed["exhaustive"] is False


@pytest.mark.parametrize("fault", [
    "page_window", "remaining_window", "argv", "parameter_hash", "source",
    "missing_source_hash", "receipt_source_hash", "receipt_stdout_hash",
    "receipt_stderr_hash", "receipt_window", "receipt_argv", "receipt_path",
    "receipt_hash", "stdout", "stderr", "missing_receipt", "auth_error",
])
def test_legacy_rejected_page_requires_exact_provenance_before_fetch(harness, fault):
    run, workspace, root = harness
    saved = legacy_rejected_page(harness, response([message(i) for i in range(65, 0, -1)]))
    page = saved["pages"][0]
    receipt_path = Path(page["receipt_path"])
    receipt = json.loads(receipt_path.read_text())
    if fault == "page_window":
        page["window"]["to"] = "2026-01-01T23:00:00Z"
    elif fault == "remaining_window":
        saved["remaining_window"]["to"] = "2026-01-01T23:00:00Z"
    elif fault == "argv":
        page["argv"][3] = "different-query"
    elif fault == "parameter_hash":
        saved["parameter_hash"] = "0" * 64
    elif fault == "missing_source_hash":
        page.pop("source_sha256")
    elif fault.startswith("receipt_") and fault.endswith("_hash") and fault != "receipt_hash":
        receipt[fault.removeprefix("receipt_").removesuffix("_hash") + "_sha256"] = "0" * 64
    elif fault == "receipt_window":
        receipt["window"]["to"] = START
    elif fault == "receipt_argv":
        receipt["argv"][3] = "different-query"
    elif fault == "receipt_path":
        receipt["source_path"] += ".different"
    elif fault == "receipt_hash":
        page["receipt_sha256"] = hashlib.sha256(receipt_path.read_bytes()).hexdigest()
        receipt["recovered_after_interruption"] = True
    elif fault == "auth_error":
        page["returncode"] = receipt["returncode"] = 1
    if fault in {"source", "stdout", "stderr"}:
        path = Path(page[fault + "_path"])
        path.chmod(0o600)
        path.write_text("corrupted")
    if fault == "missing_receipt":
        receipt_path.unlink()
    elif fault.startswith("receipt_") or fault == "auth_error":
        receipt_path.chmod(0o600)
        receipt_path.write_text(json.dumps(receipt))
        receipt_path.chmod(0o444)
    (workspace / "manifest.json").write_text(json.dumps(saved))
    before = (workspace / "manifest.json").read_bytes()
    result = run(None, "--limit", "50", "--resume")
    assert result.returncode == 1
    assert len(calls(root)) == 1
    assert (workspace / "manifest.json").read_bytes() == before
    assert json.loads((workspace / "messages.json").read_text()) == []
