"""Move LLM traces from one file per record to one JSONL file per session.

    sudo -u <service user> python3 scripts/migrate_llm_traces.py backend/.deer-flow/llm-traces

The old layout is ROOT/<owner>/<sha256(call_id)>/<monotonic_ns>-<uuid>.json; on a network
volume every one of those small files costs a whole storage block. The new layout
(deerflow.models.local_trace.session_file) is ROOT/<owner>/<thread_id>.jsonl.

For each call directory the records are first staged on local disk, then the old files
are deleted (this is what frees the quota when the volume is full), then the lines are
appended to the session files. A failed append leaves its stage behind; a rerun appends
the leftover stages first. Run it as the user that owns the traces, with the writers
stopped or running: new records already go to the session files.
"""

import argparse
import fcntl
import hashlib
import json
import os
import re
import shutil
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

CALL_DIR = re.compile(r"[0-9a-f]{64}")
SAFE_SESSION = re.compile(r"[A-Za-z0-9_.-]{1,128}")


def session_name(thread_id):
    """Same naming as deerflow.models.local_trace.session_file."""
    name = str(thread_id) if thread_id is not None else "no-thread"
    if not SAFE_SESSION.fullmatch(name) or name.startswith("."):
        name = hashlib.sha256(name.encode()).hexdigest()
    return f"{name}.jsonl"


def append(path, data):
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND | os.O_NOFOLLOW, 0o600)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        view = memoryview(data)
        while view:
            view = view[os.write(descriptor, view) :]
    finally:
        os.close(descriptor)


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
        path.unlink(missing_ok=True)
    if not unreadable:
        call_dir.rmdir()
    flush_stage(stage, owner_dir)
    return len(records), unreadable


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("root", type=Path)
    parser.add_argument("--stage", type=Path, default=Path("/tmp/llm-trace-migration"))
    parser.add_argument("--workers", type=int, default=32)
    args = parser.parse_args()
    root = args.root.resolve()

    for stage in sorted(args.stage.glob("*/*")) if args.stage.exists() else []:
        flush_stage(stage, root / stage.parent.name)
        print(f"flushed leftover stage {stage}", file=sys.stderr)

    calls = [call for owner in root.iterdir() if owner.is_dir() and not owner.is_symlink() for call in owner.iterdir() if call.is_dir() and CALL_DIR.fullmatch(call.name)]
    print(f"{len(calls)} call directories", file=sys.stderr)
    moved = unreadable = failed = 0
    with ThreadPoolExecutor(args.workers) as pool:
        for index, result in enumerate(pool.map(lambda call: _safe(migrate_call, call, args.stage), calls), 1):
            if isinstance(result, Exception):
                failed += 1
                print(f"failed: {type(result).__name__}: {result}", file=sys.stderr)
            else:
                moved += result[0]
                unreadable += result[1]
            if index % 200 == 0:
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
