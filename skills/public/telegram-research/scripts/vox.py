#!/usr/bin/env python3
"""Vox CLI for Telegram research. The only way to reach Vox; every call goes
through the Vox proxy at VOX_RELAY_URL. The proxy sends one request to Vox at a
time, retries failures itself and returns only complete results. If one Vox call
runs past its time limit, the command fails: the query is too heavy and should
be made more specific. An empty result is a valid answer.

Run from the research folder. It holds:
    registry.json       keywords and whales with their status
    messages_raw.json   every message Vox returned, deduplicated, with sources

    vox.py search QUERY --since 2025-10-14 [--until ...] [--kind all|post|comment]
    vox.py whale USER_ID [--since 2025-10-14]
    vox.py authors [--top 50]                       # active authors in messages_filtered.json
    vox.py add_keywords WORD [WORD ...] --source "..."
    vox.py add_whale USER_ID --alias NAME --reason "..."
    vox.py registry                                 # registry.json with each keyword's yield
"""

import fcntl
import hashlib
import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter, defaultdict
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

import fire

PAGE = 10000
SEARCH_TIMEOUT = 600
WHALE_LIMIT = 100000
REGISTRY = Path("registry.json")
RAW = Path("messages_raw.json")
FILTERED = Path("messages_filtered.json")


class VoxError(Exception):
    pass


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise VoxError("Vox proxy redirected the request; refusing to follow it.")


_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}), _NoRedirect())


def _relay():
    value = os.environ.get("VOX_RELAY_URL")
    if not value:
        raise VoxError("VOX_RELAY_URL is not set; it comes from sandbox.environment in config.yaml.")
    url = urllib.parse.urlsplit(value)
    if url.scheme != "http" or url.hostname not in {"localhost", "127.0.0.1", "::1"} or url.path not in {"", "/"}:
        raise VoxError("VOX_RELAY_URL must be the local Vox proxy base URL.")
    return value.rstrip("/")


def _call(path, params=None):
    params = {k: (str(v).lower() if isinstance(v, bool) else v) for k, v in (params or {}).items() if v not in (None, "")}
    url = _relay() + path + ("?" + urllib.parse.urlencode(params) if params else "")
    headers = {"Accept": "application/json"}
    if os.environ.get("VOX_TOOL_TRACE_CONTEXT"):
        headers["X-Vox-Tool-Trace"] = os.environ["VOX_TOOL_TRACE_CONTEXT"]
    # The proxy owns admission and deadlines; no client-side timeout.
    try:
        with _OPENER.open(urllib.request.Request(url, headers=headers), timeout=None) as response:
            if params.get("stream") == "true":
                for line in response:
                    if line.strip():
                        frame = json.loads(line)
                        if isinstance(frame, dict) and isinstance(frame.get("messages"), list):
                            return frame
                raise VoxError(f"Search stream at {path} ended without a result; rerun to resume.")
            return json.loads(response.read())
    except urllib.error.HTTPError as exc:
        try:
            detail = json.loads(exc.read(4096))
        except (ValueError, OSError):
            detail = {}
        if exc.code == 504 and isinstance(detail, dict) and detail.get("error") == "request_timeout":
            raise VoxError(detail["message"]) from None
        raise VoxError(f"HTTP {exc.code} at {path}.") from None
    except (urllib.error.URLError, OSError) as exc:
        raise VoxError(f"Network error ({type(exc).__name__}) at {path}; rerun to resume.") from None


def _date(value):
    moment = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return (moment if moment.tzinfo else moment.replace(tzinfo=timezone.utc)).astimezone(timezone.utc)


def _iso(moment):
    return _date(moment).strftime("%Y-%m-%dT%H:%M:%SZ")


def _slug(text):
    return "".join(c if c.isalnum() else "-" for c in str(text).casefold()).strip("-")[:60] or "query"


def _save(path, data):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=1), encoding="utf-8")
    os.replace(tmp, path)
    return str(path.resolve())


def _load(path, default=None):
    path = Path(path)
    if not path.exists() and default is not None:
        return default
    return json.loads(path.read_text(encoding="utf-8"))


@contextmanager
def _locked(path):
    """Serialize read-modify-write of a shared file between parallel agents."""
    with open(str(path) + ".lock", "w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        yield


def _message(row, user_id=None):
    message_id = row.get("id")
    if message_id is None and row.get("message_url"):
        tail = re.search(r"/(\d+)/?$", row["message_url"])
        message_id = int(tail.group(1)) if tail else None
    keep = ("group_id", "group_name", "group_alias", "group_url", "message_url", "date", "user_id", "user_alias", "text")
    message = {k: row.get(k) for k in keep if row.get(k) not in (None, "")}
    message["id"] = message_id
    if user_id is not None:
        message["user_id"] = user_id
    return message


def _key(message):
    if message.get("id") is not None:
        return f"{message.get('group_id')}:{message['id']}"
    digest = hashlib.sha256((message.get("text") or "").encode()).hexdigest()[:16]
    return f"{message.get('group_id')}:{message.get('date')}:{digest}"


def _pool(messages, source):
    """Merge messages into messages_raw.json, deduplicated, recording where each came from."""
    with _locked(RAW):
        pool = _load(RAW, {"messages": []})
        index = {_key(m): m for m in pool["messages"]}
        added = 0
        for message in messages:
            current = index.get(_key(message))
            if current is None:
                current = index[_key(message)] = {**message, "sources": []}
                pool["messages"].append(current)
                added += 1
            if source not in current["sources"]:
                current["sources"].append(source)
        _save(RAW, pool)
    print(f"{source}: {added} new, {len(pool['messages'])} in messages_raw.json", file=sys.stderr)


def _registry(update):
    with _locked(REGISTRY):
        registry = _load(REGISTRY, {"keywords": [], "whales": []})
        update(registry)
        _save(REGISTRY, registry)
    return registry


def _traverse(state, out, query):
    """Collect every match in the window. After each Vox call its new messages go
    to messages_raw.json and the checkpoint is saved, so an interrupted search
    loses nothing.

    A response below the limit is complete for its window. A full response is
    not: Vox returns whatever it gathered first, with gaps between dates. So a
    full window is split in half and both halves are searched again, until each
    returns fewer than PAGE matches.
    """
    seen = {_key(m) for m in state["messages"]}
    while state["pending"]:
        low, high = state["pending"].pop()
        page = _call(
            "/search/text",
            {
                "query": query,
                "limit": PAGE,
                "order": "desc",
                "message_type": state["kind"],
                "from_date": low,
                "to_date": high,
                "timeout": SEARCH_TIMEOUT,
                "stream": True,
            },
        )
        found = []
        for row in page["messages"]:
            message = _message(row)
            if row.get("is_private") is not True and _key(message) not in seen:
                seen.add(_key(message))
                found.append(message)
        _pool(found, f"keyword:{query}")
        state["messages"] += found
        state["calls"] += 1
        start, end = _date(low), _date(high)
        if len(page["messages"]) >= PAGE and end - start > timedelta(seconds=1):
            middle = _iso(start + (end - start) / 2)
            state["pending"] += [[low, middle], [middle, high]]
        _save(out, state)
        print(f"{query!r}: {state['calls']} calls, {len(state['pending'])} windows left, {len(state['messages'])} messages", file=sys.stderr)


class Vox:
    """Vox research CLI. Run it from the research folder."""

    def search(self, query, since, until=None, kind="all"):
        """All messages matching QUERY (words are ANDed) from SINCE to UNTIL (default now).
        Full responses are split by date until complete. Each Vox response is added to
        messages_raw.json right away; the keyword is marked searched at the end."""
        query = str(query)
        if not 3 <= len(query) < 50:
            raise VoxError("Vox needs a query of 3..49 characters.")
        until = _iso(until or datetime.now(timezone.utc))
        out = Path(f"vox/search/{_slug(query)}_{_iso(since)[:10]}_{until[:10]}.json")
        if out.exists():
            state = _load(out)
            # The checkpoint may hold messages a crash kept out of the pool.
            _pool(state["messages"], f"keyword:{query}")
        else:
            state = {"query": query, "since": _iso(since), "until": until, "kind": kind, "pending": [[_iso(since), until]], "calls": 0, "messages": []}
        _traverse(state, out, query)
        state["complete"] = True
        _save(out, state)

        def mark(registry):
            for item in registry["keywords"]:
                if item["word"] == query:
                    item["status"] = "searched"

        _registry(mark)
        return str(RAW.resolve())

    def whale(self, user_id, since=None):
        """A whale's messages in public chats (Vox user_tracker messages panel only).
        Adds them to messages_raw.json and marks the whale fetched."""
        uid = int(user_id)
        data = _call(f"/user_tracker/{uid}/panels/messages", {"limit": WHALE_LIMIT})
        _save(f"vox/whales/{uid}.json", data)
        low = _date(since) if since else None
        messages = [_message(row, user_id=uid) for row in data.get("rows") or [] if row.get("is_private") is not True and (low is None or _date(row["date"]) >= low)]
        _pool(messages, f"whale:{uid}")

        def mark(registry):
            for item in registry["whales"]:
                if item["user_id"] == uid:
                    item["status"] = "fetched"

        _registry(mark)
        return str(RAW.resolve())

    def authors(self, top=50):
        """Most active authors in messages_filtered.json: message and group counts, sample URLs."""
        messages = _load(FILTERED)["messages"]
        counts = Counter(m["user_id"] for m in messages if m.get("user_id"))
        aliases, groups, samples = {}, defaultdict(set), defaultdict(list)
        for m in messages:
            uid = m.get("user_id")
            if not uid:
                continue
            if not aliases.get(uid):
                aliases[uid] = m.get("user_alias")
            groups[uid].add(m.get("group_id"))
            if len(samples[uid]) < 5 and m.get("message_url"):
                samples[uid].append(m["message_url"])
        rows = [{"user_id": uid, "user_alias": aliases[uid], "messages": n, "groups": len(groups[uid]), "sample_urls": samples[uid]} for uid, n in counts.most_common(int(top))]
        return _save("vox/authors.json", {"total_messages": len(messages), "authors": rows})

    def add_keywords(self, *words, source=""):
        """Add new keywords to registry.json (status todo); known ones are skipped."""

        def add(registry):
            known = {item["word"].casefold() for item in registry["keywords"]}
            for word in words:
                if str(word).casefold() not in known:
                    known.add(str(word).casefold())
                    registry["keywords"].append({"word": str(word), "source": source, "status": "todo"})

        registry = _registry(add)
        return f"{sum(k['status'] == 'todo' for k in registry['keywords'])} keywords to search"

    def add_whale(self, user_id, alias="", reason=""):
        """Add a whale to registry.json (status todo)."""
        uid = int(user_id)

        def add(registry):
            if all(item["user_id"] != uid for item in registry["whales"]):
                registry["whales"].append({"user_id": uid, "user_alias": alias, "reason": reason, "status": "todo"})

        registry = _registry(add)
        return f"{len(registry['whales'])} whales"

    def registry(self):
        """Print registry.json with each keyword's and whale's yield: messages in
        messages_raw.json and on-topic ones in messages_filtered.json."""
        registry = _load(REGISTRY, {"keywords": [], "whales": []})
        raw = Counter(s for m in _load(RAW, {"messages": []})["messages"] for s in m.get("sources", []))
        filtered = Counter(s for m in _load(FILTERED, {"messages": []})["messages"] for s in m.get("sources", []))
        for item in registry["keywords"]:
            item["raw"], item["on_topic"] = raw[f"keyword:{item['word']}"], filtered[f"keyword:{item['word']}"]
        for item in registry["whales"]:
            item["raw"], item["on_topic"] = raw[f"whale:{item['user_id']}"], filtered[f"whale:{item['user_id']}"]
        return json.dumps(registry, ensure_ascii=False, indent=1)


def main():
    try:
        fire.Fire(Vox, serialize=lambda value: value)
    except VoxError as exc:
        print(f"error: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
