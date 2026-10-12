import hashlib
import json
import os
import subprocess
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import pytest

ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = Path(os.environ.get("VOX_TEST_SCRIPTS", ROOT / "skills/public/telegram-research/scripts"))
START = "2026-01-01T00:00:00Z"
END = "2026-01-02T00:00:00Z"


def message(i, second=None):
    return {"id": i, "group_id": -10011111, "date": (datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=i * 2 if second is None else second)).isoformat(), "text": f"SYNTHETIC {i}", "message_url": f"https://t.me/synthetic/{i}"}


@pytest.fixture
def fixture(tmp_path):
    state = {"rows": [message(i) for i in range(1, 121)], "calls": [], "cap": 50}

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format, *args):
            pass

        def do_GET(self):
            query = parse_qs(urlsplit(self.path).query)
            state["calls"].append(query)
            low = datetime.fromisoformat(query["from_date"][0].replace("Z", "+00:00"))
            high = datetime.fromisoformat(query["to_date"][0].replace("Z", "+00:00"))
            rows = [r for r in state["rows"] if low <= datetime.fromisoformat(r["date"]) <= high]
            rows.sort(key=lambda r: r["date"], reverse=query["order"][0] == "desc")
            rows = rows[:min(int(query["limit"][0]), state["cap"])]
            data = {"messages": rows, "count": len(rows), "stop_reason": "finished", "fixture_schema_evidence": "retained"}
            if state.get("partial") and len(state["calls"]) == 1:
                data.update(stop_reason="timeout", stopped_at=rows[-1]["date"])
            if state.get("delay") and len(state["calls"]) == 1:
                time.sleep(state["delay"])
            body = json.dumps(data).encode()
            try:
                self.send_response(200)
                self.end_headers()
                self.wfile.write(body)
            except BrokenPipeError:
                pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    state["url"] = f"http://127.0.0.1:{server.server_port}"
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    workspace = tmp_path / "collection"

    def run(*extra, collector=False, dates=True):
        argv = [sys.executable, str(SCRIPTS / ("collect_search.py" if collector else "vox.py"))]
        argv += ["--query", "synthetic"] if collector else ["search", "synthetic"]
        argv += ["--workspace", str(workspace)]
        if dates:
            argv += ["--from", START, "--to", END]
        result = subprocess.run(argv + list(extra), capture_output=True, text=True, timeout=20, env={**os.environ, "VOX_RELAY_URL": f"http://127.0.0.1:{server.server_port}", "VOX_API_KEY": "synthetic-not-used"}, cwd=tmp_path)
        saved = json.loads((workspace / "manifest.json").read_text()) if (workspace / "manifest.json").exists() else None
        return result, saved

    yield state, run, workspace
    server.shutdown()
    server.server_close()
    thread.join()


@pytest.mark.parametrize("order", ["asc", "desc"])
@pytest.mark.parametrize("collector", [False, True])
def test_default_requests_2000_and_automatically_traverses_real_cap(fixture, order, collector):
    state, run, workspace = fixture
    result, saved = run("--order", order, collector=collector)
    assert result.returncode == 0, result.stderr
    assert saved["traversal_complete"] is True
    assert saved["exhaustive"] is False
    assert len(state["calls"]) == 3
    assert all(c["limit"] == ["2000"] for c in state["calls"])
    assert {r["id"] for r in json.loads(Path(saved["messages_path"]).read_text())} == set(range(1, 121))
    assert all(p["argv"][2] == "search-page" for p in saved["pages"])
    for page in saved["pages"]:
        raw = Path(page["source_path"]).read_bytes()
        assert hashlib.sha256(raw).hexdigest() == page["source_sha256"]
        assert json.loads(raw)["fixture_schema_evidence"] == "retained"
        assert Path(page["stdout_path"]).read_text().strip() == page["source_path"]


@pytest.mark.parametrize("order", ["asc", "desc"])
def test_partial_timeout_continues_both_date_sides_without_losing_boundary(fixture, order):
    state, run, _ = fixture
    state["partial"] = True
    result, saved = run("--order", order)
    assert result.returncode == 0, result.stderr
    assert saved["pages"][0]["classification"] == "partial"
    rows = json.loads(Path(saved["messages_path"]).read_text())
    assert {r["id"] for r in rows} == set(range(1, 121))
    assert len(rows) == 120


@pytest.mark.parametrize("limit", [None, 10000])
def test_page_size_is_not_total_cap_and_schema_max_remains(fixture, limit):
    state, run, _ = fixture
    state["cap"] = 2000
    state["rows"] = [message(i) for i in range(1, 2401)]
    result, saved = run(*([] if limit is None else ["--limit", str(limit)]))
    assert result.returncode == 0, result.stderr
    assert saved["unique_count"] == 2400
    assert len(state["calls"]) == 3
    assert all(c["limit"] == [str(limit or 2000)] for c in state["calls"])
    assert {r["id"] for r in json.loads(Path(saved["messages_path"]).read_text())} == set(range(1, 2401))


def test_page_budget_resume_checks_cursor_and_preserves_raw(fixture):
    state, run, _ = fixture
    result, saved = run("--max-pages", "1")
    assert result.returncode == 2, result.stderr
    old = {p["source_path"]: Path(p["source_path"]).read_bytes() for p in saved["pages"]}
    wrong, _ = run("--resume", "--expected-cursor", START)
    assert wrong.returncode == 1
    assert len(state["calls"]) == 1
    result, saved = run("--resume", "--expected-cursor", saved["remaining_window"]["to"])
    assert result.returncode == 0, result.stderr
    assert saved["unique_count"] == 120
    assert all(Path(p).read_bytes() == b for p, b in old.items())


@pytest.mark.parametrize("order", ["asc", "desc"])
def test_saturated_timestamp_is_unresolved_without_skipping(fixture, order):
    state, run, _ = fixture
    state["rows"] = [message(i, 100) for i in range(1, 121)]
    result, saved = run("--order", order)
    assert result.returncode == 1, result.stderr
    assert saved["status"] == "unresolved"
    assert not saved["traversal_complete"]
    assert len(state["calls"]) == 1
    assert saved["remaining_window"] == {"from": START, "to": END}


def test_foreground_deadline_leaves_pending_and_resume_recovers(fixture):
    state, run, _ = fixture
    state["delay"] = 2
    started = time.monotonic()
    result, saved = run("--max-seconds", "1")
    assert result.returncode == 2, result.stderr
    assert time.monotonic() - started < 1.8
    assert saved["pending"] is not None
    assert saved["remaining_window"] == {"from": START, "to": END}
    result, saved = run("--resume", "--expected-cursor", END)
    assert result.returncode == 0, result.stderr
    assert saved["unique_count"] == 120
    assert state["calls"][0] == state["calls"][1]


def test_omitted_dates_freeze_explicit_window_for_resume(fixture):
    state, run, _ = fixture
    result, saved = run("--max-pages", "1", dates=False)
    assert result.returncode == 2, result.stderr
    initial = dict(saved["parameters"])
    result, saved = run("--resume", "--expected-cursor", saved["remaining_window"]["to"], dates=False)
    assert result.returncode == 0, result.stderr
    assert saved["parameters"] == initial
    assert saved["unique_count"] == 120


def test_stalled_overlap_does_not_close_remaining_window(fixture):
    state, run, _ = fixture
    state["rows"] = [message(i, 100 + i / 1000) for i in range(1, 121)]
    result, saved = run()
    assert result.returncode == 1, result.stderr
    assert saved["status"] == "unresolved"
    assert "No progress" in saved["reason"]
    assert len(state["calls"]) == 2
    assert saved["traversal_complete"] is False


def test_termination_stops_foreground_child_and_preserves_pending(fixture):
    state, run, workspace = fixture
    state["delay"] = 3
    argv = [sys.executable, str(SCRIPTS / "vox.py"), "search", "synthetic", "--from", START, "--to", END, "--workspace", str(workspace)]
    child = None
    with subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env={**os.environ, "VOX_RELAY_URL": state["url"]}) as process:
        try:
            deadline = time.monotonic() + 5
            while not state["calls"] and time.monotonic() < deadline:
                time.sleep(0.01)
            assert state["calls"]
            child = int(Path(f"/proc/{process.pid}/task/{process.pid}/children").read_text().strip())
            process.terminate()
            process.communicate(timeout=5)
            assert not Path(f"/proc/{child}").exists()
            saved = json.loads((workspace / "manifest.json").read_text())
            assert saved["pending"] is not None
            assert saved["status"] == "error"
        finally:
            if child and Path(f"/proc/{child}").exists():
                os.kill(child, 9)
            if process.poll() is None:
                process.kill()
                process.wait()


def test_search_output_is_manifest_snapshot_and_never_overwrites(fixture, tmp_path):
    state, run, workspace = fixture
    output = tmp_path / "result.json"
    result, saved = run("--output", str(output))
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == str(output)
    assert json.loads(output.read_text()) == saved
    old = output.read_bytes()
    result, _ = run("--resume", "--output", str(output))
    assert result.returncode == 1
    assert len(state["calls"]) == 3
    assert output.read_bytes() == old
