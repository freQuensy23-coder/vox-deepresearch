#!/usr/bin/env python3
"""Read-only Vox API primitives. Research decisions belong to the agent.

Python 3.10+, standard library only. JSON files, saved paths on stdout, diagnostics on stderr.
One bounded transient HTTP retry; bounded date continuation, no automatic research, link
crawling or personal reports.
"""

import argparse
import fcntl
import http.client
import importlib.util
import json
import math
import os
import re
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from contextlib import ExitStack, contextmanager, nullcontext
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from uuid import uuid4

BASE = "https://api.vox-lab.com"
KEY_FILE = Path.home() / ".config" / "vox" / "api_key"
TIMEOUT = 300
ATTEMPTS = 2


class VoxError(Exception):
    """An actionable error safe to show without a provider payload."""


class IncompleteSearchStream(VoxError):
    """A transport ended before returning the final search result."""


class Admission:
    def __init__(self):
        configured = os.environ.get("VOX_ADMISSION_DIR")
        self.directory = (
            Path(configured).expanduser()
            if configured is not None
            else Path.home() / ".cache" / "vox" / "admission"
        )
        if not self.directory.is_absolute():
            raise VoxError("Vox admission requires an absolute VOX_ADMISSION_DIR.")
        self.stack = ExitStack()

    def __enter__(self):
        try:
            self.directory.mkdir(mode=0o700, parents=True, exist_ok=True)
            gate = self.stack.enter_context((self.directory / "gate.lock").open("a+b"))
            fcntl.flock(gate, fcntl.LOCK_EX)
            try:
                fd = os.open(self.directory / "status.lock", os.O_RDWR | os.O_CREAT | os.O_EXCL, 0o600)
                new = True
            except FileExistsError:
                fd = os.open(self.directory / "status.lock", os.O_RDWR)
                new = False
            self.status_lock = self.stack.enter_context(os.fdopen(fd, "r+b"))
            with self.lock_status():
                try:
                    self.state = self.read_state()
                except FileNotFoundError:
                    if not new:
                        raise
                    self.state = {"version": 1, "last_start": 0, "cooldown_until": 0, "failures": 0}
                    self.write_state()
            while True:
                with self.lock_status():
                    self.state = self.read_state()
                    now = time.time()
                    delay = max(self.state["last_start"] + 5, self.state["cooldown_until"]) - now
                    if delay <= 0:
                        self.state["last_start"] = now
                        self.write_state()
                        return self
                time.sleep(min(delay, 60))
        except (OSError, ValueError, OverflowError):
            self.stack.close()
            raise VoxError("Vox admission unavailable or invalid; no request admitted.") from None
        except BaseException:
            self.stack.close()
            raise

    def __exit__(self, *args):
        self.stack.close()

    @contextmanager
    def lock_status(self):
        fcntl.flock(self.status_lock, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(self.status_lock, fcntl.LOCK_UN)

    def read_state(self):
        def unique_fields(pairs):
            result = dict(pairs)
            if len(result) != len(pairs):
                raise ValueError("Duplicate admission fields")
            return result

        with (self.directory / "status.json").open("rb") as handle:
            raw = handle.read(4097)
        if len(raw) > 4096:
            raise ValueError("Oversized admission state")
        state = json.loads(raw, object_pairs_hook=unique_fields)
        if not isinstance(state, dict) or set(state) != {"version", "last_start", "cooldown_until", "failures"}:
            raise ValueError("Invalid admission fields")
        if type(state["version"]) is not int or state["version"] != 1:
            raise ValueError("Invalid admission version")
        if type(state["failures"]) is not int or not 0 <= state["failures"] <= 5:
            raise ValueError("Invalid admission failure count")
        for field in ("last_start", "cooldown_until"):
            value = state[field]
            if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
                raise ValueError("Invalid admission timestamp")
        return state

    def write_state(self):
        temporary = None
        try:
            with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=self.directory, prefix=".status-", delete=False) as handle:
                temporary = Path(handle.name)
                json.dump(self.state, handle, allow_nan=False, separators=(",", ":"))
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, self.directory / "status.json")
            temporary = None
            fd = os.open(self.directory, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)

    def finish(self, failed=False, retry_after=None):
        try:
            with self.lock_status():
                self.state = self.read_state()
                if failed:
                    self.state["failures"] = min(self.state["failures"] + 1, 5)
                    now = time.time()
                    delay = min(60 * 2 ** (self.state["failures"] - 1), 900)
                    deadline = now + delay
                    if retry_after is not None:
                        value = retry_after.strip()
                        if re.fullmatch(r"[0-9]+", value):
                            deadline = max(deadline, now + int(value))
                        else:
                            try:
                                parsed = parsedate_to_datetime(value)
                                deadline = max(deadline, parsed.replace(tzinfo=parsed.tzinfo or timezone.utc).timestamp())
                            except (ValueError, TypeError, OverflowError):
                                pass
                    self.state["cooldown_until"] = max(self.state["cooldown_until"], deadline)
                else:
                    self.state["failures"] = 0
                    self.state["cooldown_until"] = 0
                self.write_state()
        except (OSError, ValueError, OverflowError):
            raise VoxError("Vox admission status could not be persisted; stopping.") from None


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        if fp is not None:
            fp.close()
        raise VoxError("Vox redirected the request; refusing to forward the API key.")


OPENER = urllib.request.build_opener(NoRedirect())
RELAY_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())


def read_search_stream(response):
    """Read Vox NDJSON progress frames until the actual SearchResult arrives."""
    last_notice = None
    for line in response:
        if not line.strip():
            continue
        frame = json.loads(line.decode("utf-8"))
        if isinstance(frame, dict) and isinstance(frame.get("messages"), list):
            return frame
        if not isinstance(frame, dict) or "point" not in frame:
            raise VoxError(
                "Unexpected search stream frame; no final search result received."
            )
        now = time.monotonic()
        if last_notice is None or now - last_notice >= 15:
            print("Vox search is running; received a progress frame.", file=sys.stderr)
            last_notice = now
    raise IncompleteSearchStream(
        "Search stream ended without a final result; progress is not an empty search."
    )


def api_key():
    key = os.getenv("VOX_API_KEY", "").strip()
    if not key and KEY_FILE.is_file():
        key = KEY_FILE.read_text(encoding="utf-8").strip()
    if not key:
        raise VoxError(
            "VOX_API_KEY is missing. Supply the skill secret or a local ~/.config/vox/api_key."
        )
    if not key.isascii() or any(ord(char) <= 32 or ord(char) == 127 for char in key):
        raise VoxError(
            "VOX_API_KEY must be a single ASCII token without embedded whitespace."
        )
    return key


def relay_base():
    value = os.environ.get("VOX_RELAY_URL")
    if value is None:
        return None
    try:
        url = urllib.parse.urlsplit(value)
        if (
            any(ord(char) <= 32 or ord(char) == 127 for char in value)
            or url.scheme != "http"
            or url.hostname not in {"vox-relay", "localhost", "127.0.0.1", "::1"}
            or url.username is not None
            or url.password is not None
            or url.path not in {"", "/"}
            or "?" in value
            or "#" in value
            or (url.port is not None and url.port < 1)
        ):
            raise ValueError
    except ValueError:
        raise VoxError("VOX_RELAY_URL must be an internal HTTP relay base without credentials, path, query or fragment.") from None
    return value.rstrip("/")


def call(path, params=None, method="GET"):
    params = {
        k: (str(v).lower() if isinstance(v, bool) else v)
        for k, v in (params or {}).items()
        if v is not None and v != "" and v is not False
    }
    relay = relay_base()
    url = (relay or BASE) + path + ("?" + urllib.parse.urlencode(params) if params else "")
    headers = {"Accept": "application/json", "User-Agent": "curl/8.7.1"}
    if relay is None:
        headers["Authorization"] = f"Bearer {api_key()}"
    elif os.environ.get("VOX_TOOL_TRACE_CONTEXT"):
        headers["X-Vox-Tool-Trace"] = os.environ["VOX_TOOL_TRACE_CONTEXT"]
    req = urllib.request.Request(url, method=method, headers=headers)
    socket_timeout = (
        max(TIMEOUT, int(params.get("timeout", 100)) + 30)
        if path == "/search/text"
        else TIMEOUT
    )
    attempts = ATTEMPTS if relay is None else 1
    opener = OPENER if relay is None else RELAY_OPENER
    for attempt in range(attempts):
        with Admission() if relay is None else nullcontext() as admission:
            retry_after = None
            try:
                with opener.open(req, timeout=socket_timeout) as response:
                    if path == "/search/text" and params.get("stream") == "true":
                        result = read_search_stream(response)
                    else:
                        result = json.loads(response.read().decode("utf-8"))
            except urllib.error.HTTPError as exc:
                try:
                    hints = {
                        401: "Check the supplied Vox credential.",
                        403: "The credential may lack this endpoint grant.",
                        404: "Not found through this lookup; this is not proof of absence in Telegram.",
                        429: "Rate limited; shared cooldown applies.",
                    }
                    message = f"HTTP {exc.code} at {path}. {hints.get(exc.code, 'Request did not succeed.')}"
                    transient = exc.code in {429, 502, 503, 504, 524}
                    retry_after = exc.headers.get("Retry-After")
                finally:
                    exc.close()
            except IncompleteSearchStream as exc:
                message = str(exc)
                transient = True
            except (
                urllib.error.URLError,
                TimeoutError,
                OSError,
                http.client.HTTPException,
            ) as exc:
                message = f"Network/timeout error ({type(exc).__name__}) at {path}."
                transient = True
            except (ValueError, UnicodeError):
                raise VoxError(f"Invalid JSON response at {path}.") from None
            else:
                if admission is not None:
                    admission.finish()
                return result
            if transient and admission is not None:
                admission.finish(failed=True, retry_after=retry_after)
            if not transient or attempt + 1 == attempts:
                raise VoxError(message) from None
        print(f"{message} Retrying the same read once after shared admission.", file=sys.stderr)


def clean(obj):
    if isinstance(obj, list):
        return [clean(item) for item in obj]
    if isinstance(obj, dict):
        result = {}
        for key, value in obj.items():
            if key.startswith("embedding") or key in {
                "access_hash",
                "phone",
                "phones",
                "contact_extras",
            }:
                continue
            if key == "report" and isinstance(value, str):
                try:
                    value = json.loads(value)
                except ValueError:
                    pass
            result[key] = clean(value)
        return result
    return obj


def group_locator(value):
    """Normalize a group locator without doing network I/O."""
    value = value.strip()
    if re.fullmatch(r"-?[1-9][0-9]*", value):
        number = int(value)
        return number if number < 0 else int(f"-100{number}")
    if value.startswith(("https://", "http://", "t.me/", "telegram.me/")):
        url = urllib.parse.urlsplit(value if "://" in value else "https://" + value)
        if (
            url.hostname not in {"t.me", "telegram.me"}
            or url.username
            or url.password
            or url.port
        ):
            raise VoxError("Expected a public Telegram alias or group ID.")
        parts = url.path.strip("/").split("/")
        if parts[0] == "s":
            parts = parts[1:]
        if (
            not parts
            or parts[0] in {"c", "joinchat", "m", "share", "addstickers"}
            or len(parts) > 2
            or (len(parts) == 2 and not parts[1].isdigit())
        ):
            raise VoxError(
                "Use a public @alias; invite, internal and utility links are not group locators."
            )
        value = parts[0]
    alias = value.lstrip("@")
    if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{3,31}", alias):
        raise VoxError(
            "Expected a public @alias or a numeric group ID; private invite links are unsupported."
        )
    return alias


def saved_card(value, card_paths):
    """Match a locator to a previously retrieved card, without network I/O."""
    locator = group_locator(value)
    for filename in card_paths:
        try:
            candidate = json.loads(Path(filename).read_text(encoding="utf-8"))
        except (OSError, ValueError):
            raise VoxError("Cannot read the saved group card as UTF-8 JSON.") from None
        if not isinstance(candidate, dict):
            raise VoxError(
                "A saved group card must be a JSON object from Vox group lookup."
            )
        matches = (
            candidate.get("id") == locator
            if isinstance(locator, int)
            else str(candidate.get("alias", "")).casefold() == locator.casefold()
        )
        if matches:
            if not isinstance(candidate.get("id"), int) or candidate["id"] >= 0:
                raise VoxError("The saved group card has no valid negative group ID.")
            return candidate
    raise VoxError("No supplied group card matches this group ID or alias.")


def group_id(value, card_paths=None):
    locator = group_locator(value)
    if isinstance(locator, int):
        return locator
    if card_paths:
        return saved_card(value, card_paths)["id"]
    raise VoxError(
        "Vox has no group-alias resolver in the verified API schema. "
        "Use a numeric group ID from search/groups/user-chats or a related_group, "
        "then compare the returned alias; or supply an existing --public-card. "
        "The user-username endpoint is not a group resolver."
    )


def group_card(value, card_paths=None):
    gid = group_id(value, card_paths)
    data = call(f"/groups/{gid}")
    if not isinstance(data, dict) or data.get("id") != gid:
        raise VoxError("Vox returned no matching group card.")
    locator = group_locator(value)
    if (
        isinstance(locator, str)
        and str(data.get("alias", "")).casefold() != locator.casefold()
    ):
        raise VoxError(
            "The current group alias differs from the saved card; recheck this candidate."
        )
    keep = (
        "id",
        "alias",
        "name",
        "type",
        "members_count",
        "about",
        "related_group",
        "is_private",
    )
    out = {key: data.get(key) for key in keep}
    out["public_access"] = "unconfirmed"
    if (
        data.get("is_private") is False
        and data.get("type") in {"CHAT", "CHANNEL"}
        and data.get("alias")
    ):
        out["public_access"] = "public_alias"
        out["url"] = f"https://t.me/{data['alias']}"
    elif data.get("is_private") is True or data.get("type") == "PRIVATE":
        out = {key: out[key] for key in ("id", "is_private", "type")}
        out["public_access"] = "private"
    return out


def is_public_card(card):
    return (
        isinstance(card, dict)
        and isinstance(card.get("id"), int)
        and card["id"] < 0
        and card.get("is_private") is False
        and card.get("type") in {"CHAT", "CHANNEL"}
        and isinstance(card.get("alias"), str)
        and bool(card["alias"])
    )


def public_group(value, card_paths=None):
    if card_paths:
        card = saved_card(value, card_paths)
    else:
        card = group_card(value)
    if not is_public_card(card):
        raise VoxError(
            "Public access is unconfirmed or private; content retrieval is disabled. Check a linked public channel separately."
        )
    return card


def trim_messages(messages, max_chars):
    for message in messages:
        text = message.get("text") or ""
        if max_chars and len(text) > max_chars:
            message.update(
                text=text[:max_chars] + "…", text_truncated=True, text_length=len(text)
            )
    return messages


def summarize_groups(messages):
    """Index this response by chat without fetching data or ranking relevance."""
    groups = {}
    authors = {}
    for index, message in enumerate(messages):
        gid = message.get("group_id")
        if gid is None:
            continue
        if gid not in groups:
            groups[gid] = {"group_id": gid, "message_count": 0, "message_indices": []}
            authors[gid] = set()
        group = groups[gid]
        for field in ("group_name", "group_alias", "group_url"):
            if message.get(field) and field not in group:
                group[field] = message[field]
        group["message_count"] += 1
        group["message_indices"].append(index)
        if message.get("user_id") is not None:
            authors[gid].add(message["user_id"])
    for gid, group in groups.items():
        group["unique_author_count"] = len(authors[gid])
    return sorted(groups.values(), key=lambda group: -group["message_count"])


def cmd_search_page(args):
    include = [
        str(public_group(value, args.public_card)["id"]) for value in args.include or []
    ]
    exclude = [str(group_id(value, args.public_card)) for value in args.exclude or []]
    data = call(
        "/search/text",
        {
            "query": args.query,
            "limit": args.limit,
            "from_date": args.from_date,
            "to_date": args.to_date,
            "order": args.order,
            "message_type": args.type,
            "include_groups_ids": ",".join(include),
            "exclude_groups_ids": ",".join(exclude),
            "exclude_from_users": ",".join(map(str, args.exclude_users or [])),
            "exclude_forwards": args.no_forwards,
            "deduplicate": args.dedup,
            "timeout": args.timeout,
            "stream": args.stream,
        },
    )
    if not isinstance(data, dict) or not isinstance(data.get("messages"), list):
        raise VoxError("Unexpected text-search response.")
    data = clean(data)
    data["messages"] = trim_messages(
        [m for m in data["messages"] if m.get("is_private") is not True], args.max_chars
    )
    data["returned_count"] = len(data["messages"])
    data["group_summary"] = summarize_groups(data["messages"])
    data["ungrouped_count"] = sum(m.get("group_id") is None for m in data["messages"])
    data["public_access"] = "check_group_cards"
    return data


def cmd_search(args):
    spec = importlib.util.spec_from_file_location("vox_collect_search", Path(__file__).with_name("collect_search.py"))
    if spec is None or spec.loader is None:
        raise VoxError("Date collector is unavailable.")
    collector = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(collector)
    if args.resume and args.workspace is None:
        raise VoxError("--resume requires the existing --workspace.")
    if args.expected_cursor is not None and not args.resume:
        raise VoxError("Expected cursor requires --resume.")
    if args.output is not None and os.path.lexists(Path(args.output).expanduser()):
        raise VoxError("Output already exists; choose a new snapshot path.")
    workspace = (args.workspace or Path.cwd() / "vox-results" / f"search-{uuid4().hex}").expanduser().resolve()
    manifest = workspace / "manifest.json"
    prior = json.loads(manifest.read_text())["parameters"] if args.resume and manifest.is_file() else {}
    values = {key: value for key, value in vars(args).items() if key not in {"cmd", "fn", "output"}}
    values.update(
        workspace=workspace,
        from_date=args.from_date or prior.get("from_date") or "1970-01-01T00:00:00+00:00",
        to_date=args.to_date or prior.get("to_date") or datetime.now(timezone.utc).isoformat(),
    )
    if collector.timestamp(values["from_date"]) > collector.timestamp(values["to_date"]):
        raise VoxError("--from must not be after --to")
    try:
        code = collector.run_collection(argparse.Namespace(**values))
    except collector.CollectionError as exc:
        raise VoxError(str(exc)) from None
    path = save_result(json.loads(manifest.read_text()), args.output, args.cmd) if args.output else manifest
    print(path)
    if code:
        print("Collection is incomplete; inspect status, reason and remaining_window in the manifest.", file=sys.stderr)
        raise SystemExit(code)


def cmd_groups(args):
    return {
        "candidates": clean(
            call(
                "/search/ai/raw/GROUP",
                {"query": args.query, "limit": args.limit, "offset": args.offset},
            )
        ),
        "evidence_type": "ai_discovery",
        "public_access": "check_group_cards",
    }


def cmd_group(args):
    return group_card(args.group, args.public_card)


def cmd_group_preview(args):
    data = call(
        "/reporter/parse",
        {
            "groups_search_query": args.query,
            "groups_limit": args.limit,
            "groups_offset": args.offset,
            "groups_regex": args.regex,
            # Send explicit false values; do not rely on server defaults.
            "run_parser": "false",
            "find_new_users": "false",
            "new_users_limit": 0,
        },
        method="POST",
    )
    if (
        not isinstance(data, dict)
        or data.get("run_parser") is not False
        or not isinstance(data.get("groups"), list)
    ):
        raise VoxError("Unexpected read-only group-preview response.")
    return {
        **clean(data),
        "evidence_type": "group_discovery_not_primary_evidence",
        "public_access": "check_group_cards",
    }


def cmd_posts(args):
    card = public_group(args.group, args.public_card)
    data = call(
        f"/groups/{card['id']}/messages",
        {"messages_limit": args.limit, "messages_min": 1, "members_min": 0},
    )
    if data is None:
        return {
            "id": card["id"],
            "posts": [],
            "status": "no_data",
            "evidence_type": "undated_text_sample",
        }
    if not isinstance(data, dict) or not isinstance(data.get("posts"), list):
        raise VoxError("Unexpected posts response.")
    return {**clean(data), "evidence_type": "undated_text_sample"}


def cmd_analytics(args):
    gid = public_group(args.group, args.public_card)["id"]
    data = (
        call(f"/ai_analytics/custom/GROUP/{gid}", {"custom_prompt": args.prompt})
        if args.prompt
        else call(f"/ai_analytics/GROUP/{gid}")
    )
    return {"evidence_type": "ai_summary_not_primary_evidence", "data": clean(data)}


def panel(user_id, name, limit=None):
    data = call(f"/user_tracker/{user_id}/panels/{name}", {"limit": limit})
    if (
        not isinstance(data, dict)
        or data.get("ok") is not True
        or not isinstance(data.get("rows"), list)
    ):
        raise VoxError(
            f"Vox panel {name} failed or returned an unexpected shape; this is not an empty result."
        )
    return clean(data)


def cmd_user_chats(args):
    data = panel(
        args.user_id,
        "top_groups" if args.top else "groups",
        args.limit if args.top else None,
    )
    rows = [r for r in data["rows"] if r.get("is_private") is not True]
    data["rows"] = rows[: args.limit]
    data["returned_count"] = len(data["rows"])
    data["locally_truncated"] = len(rows) > args.limit
    data["public_access"] = "check_group_cards"
    data["evidence_type"] = (
        "indexed_activity" if args.top else "indexed_presence_not_current_membership"
    )
    return data


def cmd_user_messages(args):
    gid = public_group(args.group, args.public_card)["id"]
    data = panel(args.user_id, "messages", args.limit)
    data["rows"] = trim_messages(
        [r for r in data["rows"] if r.get("group_id") == gid], args.max_chars
    )
    data["returned_count"] = len(data["rows"])
    data["group_id"] = gid
    data["scope_note"] = (
        "Filtered from a capped cross-chat panel; zero rows does not prove no activity in this group."
    )
    return data


def cmd_resolve(args):
    card = group_card(args.group, args.public_card)
    return {
        "input": args.group,
        "group_id": card["id"],
        "public_access": card["public_access"],
    }


def integer(minimum, maximum):
    def parse(value):
        number = int(value)
        if not minimum <= number <= maximum:
            raise argparse.ArgumentTypeError(f"Expected {minimum}..{maximum}.")
        return number

    return parse


def date_value(value):
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise argparse.ArgumentTypeError("Use YYYY-MM-DD or an ISO datetime.") from None
    return value


def query_value(value):
    if not value.strip() or len(value) >= 50:
        raise argparse.ArgumentTypeError(
            "Text search requires a non-empty query under 50 characters."
        )
    return value


def save_result(data, output, command):
    content = json.dumps(data, ensure_ascii=False, indent=2) + "\n"
    path = (
        Path(output).expanduser()
        if output is not None
        else Path.cwd() / "vox-results" / f"{command}-{uuid4().hex}.json"
    ).absolute()
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=".vox-",
            suffix=".tmp",
            delete=False,
        ) as handle:
            temporary = Path(handle.name)
            handle.write(content)
        os.link(temporary, path)
    finally:
        if temporary is not None:
            temporary.unlink()
    return path


def main(argv=None):
    global ATTEMPTS
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--attempts",
        type=int,
        choices=[1, 2],
        default=2,
        help="HTTP attempts per read, including one transient retry by default",
    )
    sub = parser.add_subparsers(dest="cmd", required=True)
    search = sub.add_parser(
        "search", aliases=["search-page"], help="bounded automatic date continuation; search-page is the single-page primitive"
    )
    search.add_argument("query", type=query_value)
    search.add_argument("--limit", type=integer(1, 10000), default=2000)
    search.add_argument("--workspace", type=Path)
    search.add_argument("--resume", action="store_true")
    search.add_argument("--expected-cursor")
    search.add_argument("--max-pages", type=integer(1, 10000), default=20)
    search.add_argument("--max-seconds", type=integer(1, 840), default=840)
    search.add_argument("--from", dest="from_date", type=date_value)
    search.add_argument("--to", dest="to_date", type=date_value)
    search.add_argument("--order", choices=["desc", "asc"], default="desc")
    search.add_argument("--type", choices=["all", "post", "comment"], default="all")
    search.add_argument("--include", nargs="+")
    search.add_argument(
        "--public-card",
        action="append",
        help="reuse a saved Vox group card; repeat for multiple included groups",
    )
    search.add_argument("--exclude", nargs="+")
    search.add_argument("--exclude-users", nargs="+", type=integer(1, 2**63 - 1))
    search.add_argument("--no-forwards", action="store_true")
    search.add_argument("--dedup", action="store_true")
    search.add_argument(
        "--stream",
        choices=["true", "false"],
        default="true",
        help="true: NDJSON progress (default); false: documented single JSON response",
    )
    search.add_argument(
        "--timeout",
        type=integer(10, 600),
        default=600,
        help="Vox search timeout in seconds; partial results are preserved",
    )
    search.add_argument(
        "--max-chars",
        type=integer(0, 1000000),
        default=0,
        help="0 keeps full text (default)",
    )
    search.set_defaults(fn=cmd_search_page)

    groups = sub.add_parser("groups", help="semantic candidate discovery, not evidence")
    groups.add_argument("query")
    groups.add_argument("--limit", type=integer(1, 1000), default=10)
    groups.add_argument("--offset", type=integer(0, 10000), default=0)
    groups.set_defaults(fn=cmd_groups)

    preview = sub.add_parser(
        "group-preview",
        help="group candidates with names and aliases; never run the parser",
    )
    preview.add_argument("query")
    preview.add_argument("--limit", type=integer(1, 50), default=10)
    preview.add_argument("--offset", type=integer(0, 1000), default=0)
    preview.add_argument("--regex", help="optional provider filter on the group report")
    preview.set_defaults(fn=cmd_group_preview)

    for name, handler, help_text in [
        ("group", cmd_group, "group card and public-access check"),
        ("resolve", cmd_resolve, "resolve and check a group alias"),
        ("posts", cmd_posts, "undated text sample from a public group"),
        ("analytics", cmd_analytics, "AI summary of a public group"),
    ]:
        command = sub.add_parser(name, help=help_text)
        command.add_argument("group")
        command.set_defaults(fn=handler)
        if name == "posts":
            command.add_argument("--limit", type=integer(1, 100), default=20)
        if name == "analytics":
            command.add_argument("--prompt")
        command.add_argument(
            "--public-card", action="append", help="reuse a saved Vox group card"
        )

    chats = sub.add_parser(
        "user-chats",
        help="candidate chats from indexed presence or activity; check public cards",
    )
    chats.add_argument("user_id", type=integer(1, 2**63 - 1))
    chats.add_argument(
        "--top",
        action="store_true",
        help="rank by observed messages instead of presence",
    )
    chats.add_argument("--limit", type=integer(1, 500), default=10)
    chats.set_defaults(fn=cmd_user_chats)
    messages = sub.add_parser(
        "user-messages", help="verify an observed author in a specified public group"
    )
    messages.add_argument("user_id", type=integer(1, 2**63 - 1))
    messages.add_argument("--group", required=True)
    messages.add_argument(
        "--public-card", action="append", help="reuse a saved Vox group card"
    )
    messages.add_argument(
        "--limit",
        type=integer(1, 500),
        default=50,
        help="cap on upstream cross-chat sample before local group filtering",
    )
    messages.add_argument("--max-chars", type=integer(0, 1000000), default=0)
    messages.set_defaults(fn=cmd_user_messages)
    for command in dict.fromkeys(sub.choices.values()):
        command.add_argument(
            "--output",
            metavar="FILE",
            help="save JSON to a new file (default: unique file in ./vox-results); never overwrite",
        )
    args = parser.parse_args(argv)
    ATTEMPTS = args.attempts
    if args.cmd in {"search", "search-page"} and args.from_date and args.to_date:
        start, end = [
            datetime.fromisoformat(v.replace("Z", "+00:00"))
            for v in (args.from_date, args.to_date)
        ]
        if start.replace(tzinfo=start.tzinfo or timezone.utc) > end.replace(
            tzinfo=end.tzinfo or timezone.utc
        ):
            parser.error("--from must not be after --to")
    try:
        if args.cmd == "search":
            return cmd_search(args)
        print(save_result(args.fn(args), args.output, args.cmd))
    except (VoxError, ValueError, OSError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
