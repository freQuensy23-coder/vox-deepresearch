"""Move LLM traces from one file per record to one JSONL file per session.

    sudo -u <service user> python3 scripts/migrate_llm_traces.py backend/.deer-flow/llm-traces

The old layout is ROOT/<owner>/<sha256(call_id)>/<monotonic_ns>-<uuid>.json; on a network
volume every one of those small files costs a whole storage block. The new layout
(deerflow.models.local_trace.session_file) is ROOT/<owner>/<thread_id>.jsonl.

For each call directory the records are first staged on local disk, then the old files
are truncated and deleted (this is what frees the quota when the volume is full: a plain
delete moves the blocks to the volume's trash, where they still count), then the lines are
appended to the session files. A failed append leaves its stage behind; a rerun appends
the leftover stages first. Run it as the user that owns the traces, with the writers
stopped or running: new records already go to the session files.
"""

import argparse
import hashlib
import json
import os
import re
import shutil
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

CALL_DIR = re.compile(r"[0-9a-f]{64}")
SAFE_SESSION = re.compile(r"[A-Za-z0-9_.-]{1,128}")


def session_name(thread_id):
    """Same naming as deerflow.models.local_trace.session_file."""
    name = str(thread_id) if thread_id is not None else "no-thread"
    if not SAFE_SESSION.fullmatch(name) or name.startswith("."):
        name = hashlib.sha256(name.encode()).hexdigest()
    return f"{name}.jsonl"


_locks = {}
_locks_guard = threading.Lock()


def append(path, data):
    """Threads take turns per file. No flock: on the network volume a lock left by a dead
    client is never released."""
    with _locks_guard:
        lock = _locks.setdefault(path, threading.Lock())
    with lock:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND | os.O_NOFOLLOW, 0o600)
        try:
            view = memoryview(data)
            while view:
                view = view[os.write(descriptor, view) :]
        finally:
            os.close(descriptor)


def _release(path):
    """Truncate before unlinking: a deleted file goes to the volume's trash and keeps its
    blocks in the quota, an empty one does not."""
    try:
        os.truncate(path, 0)
    except FileNotFoundError:
        return
    path.unlink(missing_ok=True)


def flush_stage(stage, owner_dir):
    """Append every staged session chunk of one call to its session file, then drop the stage."""
    for chunk in sorted(stage.glob("*.jsonl")):
        append(owner_dir / chunk.name, chunk.read_bytes())
        chunk.unlink()
    stage.rmdir()


def migrate_call(call_dir, stage_root):
    """Returns (records moved, unreadable files left in place)."""
    owner_dir = call_dir.parent
    stage = stage_root / owner_dir.name / call_dir.name
    records, unreadable, done = [], 0, []
    for path in call_dir.iterdir():
        if path.suffix == ".pending":
            done.append(path)
            continue
        try:
            records.append(json.loads(path.read_text(encoding="utf-8")))
            done.append(path)
        except (OSError, ValueError):
            unreadable += 1
    records.sort(key=lambda r: r.get("monotonic_ns", 0))
    sessions = {}
    for record in records:
        sessions.setdefault(session_name(record.get("thread_id")), []).append(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")
    stage.mkdir(parents=True, exist_ok=True)
    for name, lines in sessions.items():
        (stage / name).write_text("".join(lines), encoding="utf-8")
    for path in done:
        _release(path)
    if not unreadable:
        call_dir.rmdir()
    flush_stage(stage, owner_dir)
    return len(records), unreadable


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("root", type=Path)
    parser.add_argument("--stage", type=Path, default=Path("/tmp/llm-trace-migration"))
    parser.add_argument("--workers", type=int, default=4, help="more parallel deletes can stall a network volume")
    args = parser.parse_args()
    root = args.root.resolve()

    for stage in sorted(args.stage.glob("*/*")) if args.stage.exists() else []:
        flush_stage(stage, root / stage.parent.name)
        print(f"flushed leftover stage {stage}", file=sys.stderr)

    calls = [call for owner in root.iterdir() if owner.is_dir() and not owner.is_symlink() for call in owner.iterdir() if call.is_dir() and CALL_DIR.fullmatch(call.name)]
    print(f"{len(calls)} call directories", file=sys.stderr)
    moved = unreadable = failed = 0
    with ThreadPoolExecutor(args.workers) as pool:
        futures = [pool.submit(_safe, migrate_call, call, args.stage) for call in calls]
        for index, future in enumerate(as_completed(futures), 1):
            result = future.result()
            if isinstance(result, Exception):
                failed += 1
                print(f"failed: {type(result).__name__}: {result}", file=sys.stderr)
            else:
                moved += result[0]
                unreadable += result[1]
            if index % 50 == 0:
                print(f"{index}/{len(calls)} calls, {moved} records moved", file=sys.stderr, flush=True)
    print(json.dumps({"calls": len(calls), "records_moved": moved, "unreadable_files_left": unreadable, "failed_calls": failed}))
    if args.stage.exists() and not any(args.stage.rglob("*.jsonl")):
        shutil.rmtree(args.stage)


def _safe(function, *args):
    try:
        return function(*args)
    except Exception as error:  # one broken call directory must not stop the migration
        return error


if __name__ == "__main__":
    main()
