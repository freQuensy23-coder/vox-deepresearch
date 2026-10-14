import argparse
import fcntl
import hashlib
import json
import os
import signal
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4


SATURATION_LIMITATION = "Saturation uses the requested per-page limit; an undersized provider-finished response ends this indexed-query traversal."
LEGACY_SATURATION_LIMITATION = "The OpenAPI description says cap 50 while its schema allows 10000; saturation conservatively uses min(limit, 50)."


class CollectionError(Exception):
    pass


def timestamp(value):
    if not isinstance(value, str):
        raise CollectionError("Invalid date or cursor in response.")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise CollectionError("Invalid date or cursor in response.") from None
    return parsed.replace(tzinfo=parsed.tzinfo or timezone.utc).astimezone(timezone.utc)


def bounded_integer(low, high):
    def parse(value):
        number = int(value)
        if not low <= number <= high:
            raise argparse.ArgumentTypeError(f"Expected {low}..{high}.")
        return number

    return parse


def arguments(argv):
    parser = argparse.ArgumentParser(
        description="Bounded date traversal of one explicit Vox indexed query; not autonomous discovery or proof of Telegram exhaustiveness."
    )
    parser.add_argument("--query", required=True)
    parser.add_argument("--from", dest="from_date", required=True)
    parser.add_argument("--to", dest="to_date", required=True)
    parser.add_argument("--workspace", type=Path, required=True)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--expected-cursor")
    parser.add_argument("--max-pages", type=bounded_integer(1, 10000), default=5)
    parser.add_argument("--limit", type=bounded_integer(1, 10000), default=2000)
    parser.add_argument("--max-seconds", type=bounded_integer(1, 840), default=840)
    parser.add_argument("--attempts", type=int, choices=[1, 2], default=2)
    parser.add_argument("--type", choices=["all", "post", "comment"], default="all")
    parser.add_argument("--order", choices=["asc", "desc"], default="desc")
    parser.add_argument("--include", nargs="+")
    parser.add_argument("--public-card", action="append")
    parser.add_argument("--exclude", nargs="+")
    parser.add_argument(
        "--exclude-users", nargs="+", type=bounded_integer(1, 2**63 - 1)
    )
    parser.add_argument("--no-forwards", action="store_true")
    parser.add_argument("--dedup", action="store_true")
    parser.add_argument("--stream", choices=["true", "false"], default="true")
    parser.add_argument(
        "--server-timeout",
        "--timeout",
        dest="timeout",
        type=bounded_integer(10, 600),
        default=600,
    )
    parser.add_argument("--max-chars", type=bounded_integer(0, 1000000), default=0)
    args = parser.parse_args(argv)
    if args.expected_cursor is not None:
        if not args.resume:
            raise CollectionError("Expected cursor requires --resume.")
        try:
            timestamp(args.expected_cursor)
        except CollectionError:
            raise CollectionError("Invalid expected cursor; copy the exact remaining_window boundary.") from None
    if not args.query.strip() or len(args.query) >= 50:
        parser.error("Text search requires a nonempty query under 50 characters.")
    if timestamp(args.from_date) > timestamp(args.to_date):
        parser.error("--from must not be after --to")
    return args


def digest(content):
    return hashlib.sha256(content).hexdigest()


def encoded(value):
    return (
        json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n"
    ).encode()


def atomic_json(path, value):
    temporary = path.with_name(f".{path.name}-{uuid4().hex}.tmp")
    try:
        with temporary.open("xb") as handle:
            handle.write(encoded(value))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        temporary.unlink(missing_ok=True)


def parameters(args):
    values = {
        key: value
        for key, value in vars(args).items()
        if key not in {"workspace", "resume", "max_pages", "expected_cursor", "max_seconds", "attempts"}
    }
    values["public_card"] = [
        str(Path(p).expanduser().resolve()) for p in args.public_card or []
    ]
    values["public_card_sha256"] = [
        digest(Path(p).read_bytes()) for p in values["public_card"]
    ]
    values["overlap_seconds"] = 1
    values["saturation_threshold"] = args.limit
    if args.attempts != 2:
        values["attempts"] = args.attempts
    return values


def command(params, window, source):
    argv = [
        sys.executable,
        str(Path(__file__).with_name("vox.py")),
        "search-page",
        params["query"],
    ]
    for flag, key in [
        ("--limit", "limit"),
        ("--type", "type"),
        ("--order", "order"),
        ("--stream", "stream"),
        ("--timeout", "timeout"),
        ("--max-chars", "max_chars"),
    ]:
        argv.extend([flag, str(params[key])])
    for flag, key in [
        ("--include", "include"),
        ("--exclude", "exclude"),
        ("--exclude-users", "exclude_users"),
    ]:
        if params[key]:
            argv.extend([flag, *map(str, params[key])])
    for card in params["public_card"]:
        argv.extend(["--public-card", card])
    for flag, key in [("--no-forwards", "no_forwards"), ("--dedup", "dedup")]:
        if params[key]:
            argv.append(flag)
    argv.extend(
        ["--from", window["from"], "--to", window["to"], "--output", str(source)]
    )
    if params.get("attempts", 2) != 2:
        argv[2:2] = ["--attempts", str(params["attempts"])]
    return argv


def validate_response(data, window, params):
    if not isinstance(data, dict) or not isinstance(data.get("messages"), list):
        raise CollectionError("Invalid response schema; messages must be a list.")
    rows = data["messages"]
    for name in ("count", "returned_count"):
        if type(data.get(name)) is not int or data[name] != len(rows):
            raise CollectionError(
                "Inconsistent response counts; filtered or missing rows leave coverage unresolved."
            )
    if data.get("stop_reason") not in ("finished", "timeout"):
        raise CollectionError("Invalid response stop_reason.")
    low, high = timestamp(window["from"]), timestamp(window["to"])
    dates = []
    for row in rows:
        if (
            not isinstance(row, dict)
            or type(row.get("id")) is not int
            or row["id"] <= 0
            or type(row.get("group_id")) is not int
            or row["group_id"] >= 0
        ):
            raise CollectionError(
                "Invalid message identity; cannot deduplicate safely."
            )
        date = timestamp(row.get("date"))
        if not low <= date <= high:
            raise CollectionError("Out-of-window message; coverage remains unresolved.")
        dates.append(date)
    if dates != sorted(dates, reverse=params["order"] == "desc"):
        raise CollectionError("Response dates contradict requested order.")
    return rows, dates


def continuation(data, dates, window, params):
    low, high = timestamp(window["from"]), timestamp(window["to"])
    partial = data["stop_reason"] == "timeout"
    saturated = len(dates) >= params["saturation_threshold"]
    if not partial and not saturated:
        return "provider_finished", None, dict(window)
    if not dates:
        raise CollectionError(
            "Incomplete empty response is not evidence of exhaustion."
        )
    descending = params["order"] == "desc"
    edge = min(dates) if descending else max(dates)
    if partial:
        cursor = timestamp(data.get("stopped_at"))
        if not low <= cursor <= high:
            raise CollectionError(
                "Out-of-window partial cursor; coverage remains unresolved."
            )
        edge = max(edge, cursor) if descending else min(edge, cursor)
    if saturated and min(dates) == max(dates):
        raise CollectionError(
            "Saturated same timestamp; date-only pagination cannot resolve ties."
        )
    boundary = (
        edge + timedelta(seconds=1) if descending else edge - timedelta(seconds=1)
    )
    if not low < boundary < high:
        raise CollectionError(
            "No progress with overlapping cursor; never skip a timestamp."
        )
    next_window = dict(window)
    covered = dict(window)
    next_window["to" if descending else "from"] = boundary.isoformat()
    covered["from" if descending else "to"] = boundary.isoformat()
    return "partial" if partial else "saturated", next_window, covered


def new_manifest(params, workspace):
    return {
        "version": 1,
        "parameters": params,
        "parameter_hash": digest(encoded(params)),
        "scope": "indexed_query_only",
        "status": "running",
        "reason": None,
        "traversal_complete": False,
        "exhaustive": False,
        "limitations": [
            "The API does not document date-bound inclusivity, snapshot consistency, or an exhaustive-result guarantee.",
            "traversed means the admitted indexed-query traversal ended on an unsaturated provider-finished response, not all Telegram results.",
            "Date-only inputs mean UTC midnight, not the end of that calendar day; naive datetimes are interpreted as UTC.",
            "One-second overlap protects boundary ties; a saturated timestamp or stalled overlap remains unresolved.",
            SATURATION_LIMITATION,
        ],
        "remaining_window": {"from": params["from_date"], "to": params["to_date"]},
        "pending": None,
        "pages": [],
        "coverage": [],
        "messages_path": str(workspace / "messages.json"),
        "unique_count": 0,
    }


def checkpoint(state, messages, workspace):
    state["unique_count"] = len(messages)
    atomic_json(workspace / "messages.json", list(messages.values()))
    atomic_json(workspace / "manifest.json", state)


def restore_messages(state):
    messages = {}
    for page in state["pages"]:
        if "receipt_sha256" in page:
            if digest(Path(page["receipt_path"]).read_bytes()) != page["receipt_sha256"]:
                raise CollectionError("Immutable receipt checksum changed; refusing resume.")
        if "source_sha256" not in page:
            continue
        content = Path(page["source_path"]).read_bytes()
        if digest(content) != page["source_sha256"]:
            raise CollectionError("Immutable source checksum changed; refusing resume.")
        if page.get("accepted"):
            for row in json.loads(content)["messages"]:
                messages.setdefault((row["group_id"], row["id"]), row)
    return messages


def make_pending(state, workspace):
    identifier = uuid4().hex
    source = workspace / "raw" / f"{identifier}.json"
    return {
        "window": dict(state["remaining_window"]),
        "source_path": str(source),
        "stdout_path": str(workspace / "receipts" / f"{identifier}.stdout"),
        "stderr_path": str(workspace / "receipts" / f"{identifier}.stderr"),
        "receipt_path": str(workspace / "receipts" / f"{identifier}.json"),
        "argv": command(state["parameters"], state["remaining_window"], source),
    }


def seal_receipt(pending, returncode, recovered):
    receipt = {
        **pending,
        "returncode": returncode,
        "recovered_after_interruption": recovered,
    }
    for name in ("source", "stdout", "stderr"):
        path = Path(pending[f"{name}_path"])
        if path.is_file():
            receipt[f"{name}_sha256"] = digest(path.read_bytes())
            path.chmod(0o444)
    path = Path(pending["receipt_path"])
    atomic_json(path, receipt)
    path.chmod(0o444)


def invoke(pending, lock, remaining):
    with (
        Path(pending["stdout_path"]).open("xb") as stdout,
        Path(pending["stderr_path"]).open("xb") as stderr,
    ):
        result = subprocess.run(
            pending["argv"],
            stdout=stdout,
            stderr=stderr,
            pass_fds=(lock.fileno(),),
            check=False,
            timeout=remaining,
        )
        for handle in (stdout, stderr):
            handle.flush()
            os.fsync(handle.fileno())
    seal_receipt(pending, result.returncode, False)


def consume(state, messages, workspace):
    page = dict(state["pending"])
    state["pending"] = None
    state["pages"].append(page)
    receipt_path = Path(page["receipt_path"])
    if receipt_path.exists():
        page["returncode"] = json.loads(receipt_path.read_text())["returncode"]
        if page["returncode"] != 0:
            state.update(
                status="error",
                reason="Vox CLI failed; see the immutable stderr receipt. No collector retry was attempted.",
            )
            checkpoint(state, messages, workspace)
            return False
    else:
        page.update(returncode=None, recovered_after_interruption=True)
        seal_receipt(page, None, True)
    try:
        source = Path(page["source_path"])
        if not source.is_file():
            raise CollectionError(
                "Interrupted command produced no immutable source; resume may retry this unfinished window."
            )
        content = source.read_bytes()
        page["source_sha256"] = digest(content)
        source.chmod(0o444)
        if Path(page["stdout_path"]).read_text().strip() != str(source):
            raise CollectionError(
                "Invalid stdout path receipt; refusing to infer a successful response."
            )
        rows, dates = validate_response(
            json.loads(content), page["window"], state["parameters"]
        )
        page["accepted"] = True
        for row in rows:
            messages.setdefault((row["group_id"], row["id"]), row)
        classification, next_window, covered = continuation(
            json.loads(content), dates, page["window"], state["parameters"]
        )
        page["classification"] = classification
        state["coverage"].append(
            {
                **covered,
                "status": classification,
                "source_path": str(source),
                "exhaustive": False,
            }
        )
        state["remaining_window"] = next_window
        if next_window is None:
            state.update(status="traversed", reason=None, traversal_complete=True)
    except (CollectionError, ValueError, OSError) as exc:
        page["classification"] = "unresolved"
        state.update(status="unresolved", reason=str(exc), traversal_complete=False)
        checkpoint(state, messages, workspace)
        return False
    checkpoint(state, messages, workspace)
    return True


def revalidate_rejected_page(state, messages, workspace):
    if state["pending"] or not state["pages"]:
        return True
    original = state["pages"][-1]
    if original.get("accepted") or original.get("classification") != "unresolved":
        return True
    if "source_sha256" not in original and not Path(original["source_path"]).is_file():
        return True
    expected_command = command(state["parameters"], state["remaining_window"], original["source_path"])
    legacy_command = list(expected_command)
    legacy_command[2] = "search"
    if (
        original.get("window") != state["remaining_window"]
        or original.get("argv") not in (expected_command, legacy_command)
        or type(original.get("returncode")) is not int
        or original["returncode"] != 0
        or "source_sha256" not in original
    ):
        raise CollectionError("Rejected page does not match the current successful request; refusing resume.")
    receipt_content = Path(original["receipt_path"]).read_bytes()
    receipt_hash = digest(receipt_content)
    receipt = json.loads(receipt_content)
    if (
        not isinstance(receipt, dict)
        or type(receipt.get("returncode")) is not int
        or receipt["returncode"] != 0
        or (
            "receipt_sha256" in original
            and original["receipt_sha256"] != receipt_hash
        )
        or any(
            receipt.get(key) != original[key]
            for key in ("window", "argv", "source_path", "stdout_path", "stderr_path", "receipt_path")
        )
    ):
        raise CollectionError("Immutable receipt does not match the rejected page; refusing resume.")
    contents = {}
    for name in ("source", "stdout", "stderr"):
        contents[name] = Path(original[f"{name}_path"]).read_bytes()
        if digest(contents[name]) != receipt.get(f"{name}_sha256"):
            raise CollectionError("Immutable receipt checksum changed; refusing resume.")
    if (
        receipt["source_sha256"] != original["source_sha256"]
        or contents["stdout"].decode().strip() != original["source_path"]
    ):
        raise CollectionError("Immutable source or stdout receipt mismatch; refusing resume.")
    try:
        data = json.loads(contents["source"])
        rows, dates = validate_response(data, original["window"], state["parameters"])
        classification, next_window, covered = continuation(
            data, dates, original["window"], state["parameters"]
        )
    except (CollectionError, ValueError) as exc:
        state.update(status="unresolved", reason=str(exc), traversal_complete=False)
        checkpoint(state, messages, workspace)
        return False
    state["pages"].append({
        **original,
        "accepted": True,
        "classification": classification,
        "revalidated_from_page": len(state["pages"]) - 1,
        "receipt_sha256": receipt_hash,
    })
    for row in rows:
        messages.setdefault((row["group_id"], row["id"]), row)
    state["coverage"].append({
        **covered,
        "status": classification,
        "source_path": original["source_path"],
        "exhaustive": False,
    })
    state.update(remaining_window=next_window, status="running", reason=None)
    if next_window is None:
        state.update(status="traversed", traversal_complete=True)
    checkpoint(state, messages, workspace)
    return True


def collect(args, workspace, lock):
    deadline = time.monotonic() + args.max_seconds
    params = parameters(args)
    path = workspace / "manifest.json"
    if path.exists():
        if not args.resume:
            raise CollectionError(
                "Workspace exists; use --resume with identical parameters."
            )
        state = json.loads(path.read_text())
        stored_params = state.get("parameters")
        normalized_params = dict(stored_params) if isinstance(stored_params, dict) else {}
        # The threshold is derived from limit; older checkpoints used a smaller cap.
        normalized_params["saturation_threshold"] = params["saturation_threshold"]
        if (
            state.get("version") != 1
            or state.get("parameter_hash") != digest(encoded(stored_params))
            or normalized_params != params
        ):
            raise CollectionError(
                "Resume parameters or public-card contents do not match the checkpoint parameter hash."
            )
        if args.expected_cursor is not None:
            window = state.get("remaining_window")
            boundary = "to" if params["order"] == "desc" else "from"
            if not isinstance(window, dict) or args.expected_cursor != window.get(boundary):
                raise CollectionError("Expected cursor does not match the checkpoint; inspect remaining_window before resuming.")
        state.update(parameters=params, parameter_hash=digest(encoded(params)))
        state["limitations"] = [
            SATURATION_LIMITATION if note == LEGACY_SATURATION_LIMITATION else note
            for note in state["limitations"]
        ]
        messages = restore_messages(state)
        if state["traversal_complete"]:
            checkpoint(state, messages, workspace)
            return 0
        if not revalidate_rejected_page(state, messages, workspace):
            return 1
        if state["traversal_complete"]:
            return 0
        state.update(status="running", reason=None)
    else:
        if args.resume:
            raise CollectionError("No manifest exists to resume.")
        state, messages = new_manifest(params, workspace), {}
    checkpoint(state, messages, workspace)
    if state["pending"]:
        pending = state["pending"]
        if not Path(pending["source_path"]).is_file() and not Path(pending["receipt_path"]).is_file():
            seal_receipt(pending, None, True)
            state["pages"].append({**pending, "classification": "interrupted", "accepted": False})
            state["pending"] = None
            checkpoint(state, messages, workspace)
        elif not consume(state, messages, workspace):
            return 1
    for _ in range(args.max_pages):
        if state["traversal_complete"]:
            return 0
        if parameters(args) != params:
            state.update(
                status="error",
                reason="Public-card parameters changed during traversal; refusing another call.",
            )
            checkpoint(state, messages, workspace)
            return 1
        state["pending"] = make_pending(state, workspace)
        checkpoint(state, messages, workspace)
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            state.update(status="paused", reason="Foreground time budget reached; pending window is resumable.")
            checkpoint(state, messages, workspace)
            return 2
        try:
            invoke(state["pending"], lock, remaining)
        except subprocess.TimeoutExpired:
            state.update(status="paused", reason="Foreground time budget reached; child stopped and pending receipt will be inspected on resume.")
            checkpoint(state, messages, workspace)
            return 2
        except (OSError, KeyboardInterrupt):
            state.update(
                status="error",
                reason="CLI invocation interrupted; resume will inspect the pending receipt before any new call.",
            )
            checkpoint(state, messages, workspace)
            return 1
        if not consume(state, messages, workspace):
            return 1
    if state["traversal_complete"]:
        return 0
    state.update(
        status="paused",
        reason="max_pages reached; remaining_window is resumable with identical parameters.",
    )
    checkpoint(state, messages, workspace)
    return 2


def interrupt_collection(signum, frame):
    raise KeyboardInterrupt


def run_collection(args):
    workspace = args.workspace.expanduser().resolve()
    workspace.mkdir(parents=True, exist_ok=True)
    with (workspace / ".collector.lock").open("a+b") as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise CollectionError("This workspace is already being collected.") from None
        for name in ("raw", "receipts"):
            (workspace / name).mkdir(exist_ok=True)
        previous_handler = signal.signal(signal.SIGTERM, interrupt_collection)
        try:
            return collect(args, workspace, lock)
        finally:
            signal.signal(signal.SIGTERM, previous_handler)


def main(argv=None):
    path = None
    try:
        args = arguments(argv)
        workspace = args.workspace.expanduser().resolve()
        path = workspace / "manifest.json"
        code = run_collection(args)
        if code:
            print("Collection is incomplete; inspect the manifest reason and remaining_window.", file=sys.stderr)
    except (CollectionError, OSError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        code = 1
    if path is not None and path.is_file():
        print(path)
    return code


if __name__ == "__main__":
    raise SystemExit(main())
