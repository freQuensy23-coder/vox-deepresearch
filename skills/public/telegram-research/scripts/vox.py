#!/usr/bin/env python3
"""Vox CLI for Telegram research. The only way to reach Vox; every call goes
through the Vox proxy at VOX_RELAY_URL, which owns rate limiting.

Each command saves JSON and prints its path. Commands that page through dates
checkpoint after every page and resume from the same output file.

    vox.py search QUERY --since 2025-10-14 [--until ...] [--kind all|post|comment]
    vox.py semantic QUERY [--limit 50]
    vox.py group GROUP_ID
    vox.py download GROUP_ID --since 2025-10-14
    vox.py authors DOWNLOAD.json [--top 30]
    vox.py rank RELEVANT.json SEMANTIC.json [SEMANTIC2.json ...] [--top 16] [--exclude ID,ID]
"""

import json
import math
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import fire

PAGE = 2000
SEARCH_TIMEOUT = 600
# Vox has no "all messages of a chat" endpoint and rejects queries shorter than
# three characters, so a group download is the union of searches for frequent
# words inside that group. Very short messages without any of them are missed.
FREQUENT_WORDS = (
    "что", "это", "как", "так", "все", "всё", "уже", "есть", "если", "или",
    "для", "там", "тут", "нет", "кто", "когда", "можно", "надо", "будет", "был",
    "было", "только", "еще", "ещё", "вот", "где", "сейчас", "меня", "вас", "его",
)


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


def _load(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _message(row):
    keep = ("id", "group_id", "group_name", "group_alias", "group_url", "message_url", "date", "user_id", "user_alias", "text")
    return {k: row.get(k) for k in keep if row.get(k) is not None}


def _traverse(state, out, query, include=None):
    """Page from the cursor back to `since`, newest first, checkpointing each page."""
    seen = {(m.get("group_id"), m["id"]) for m in state["messages"]}
    since = _date(state["since"])
    while state["cursor"] is not None:
        page = _call(
            "/search/text",
            {
                "query": query,
                "limit": PAGE,
                "order": "desc",
                "message_type": state["kind"],
                "from_date": _iso(since),
                "to_date": state["cursor"],
                "include_groups_ids": include,
                "timeout": SEARCH_TIMEOUT,
                "stream": True,
            },
        )
        for row in page["messages"]:
            key = (row.get("group_id"), row.get("id"))
            if row.get("is_private") is not True and key not in seen:
                seen.add(key)
                state["messages"].append(_message(row))
        state["pages"] += 1
        timed_out = page.get("stop_reason") == "timeout"
        if not timed_out and len(page["messages"]) < PAGE:
            state["cursor"] = None
        else:
            dates = [_date(r["date"]) for r in page["messages"] if r.get("date")]
            if timed_out and page.get("stopped_at"):
                dates.append(_date(page["stopped_at"]))
            if not dates:
                raise VoxError(f"Vox stopped ({page.get('stop_reason')}) with no date to continue from; rerun to resume.")
            oldest = min(dates)
            # The next page repeats the boundary second (rows are deduplicated, never skipped);
            # step past it only when a whole page shares that second.
            if _iso(oldest) == state["cursor"]:
                oldest -= timedelta(seconds=1)
            state["cursor"] = None if oldest <= since else _iso(oldest)
        _save(out, state)
        print(f"{query!r}: page {state['pages']}, {len(state['messages'])} messages", file=sys.stderr)


class Vox:
    """Vox research CLI. Output is JSON on disk; the printed value is its path."""

    def search(self, query, since, until=None, kind="all", out=None):
        """All messages matching QUERY (words are ANDed) from SINCE to UNTIL (default now). Resumes OUT."""
        query = str(query)
        if not 3 <= len(query) < 50:
            raise VoxError("Vox needs a query of 3..49 characters.")
        out = Path(out or f"vox/search/{_slug(query)}.json")
        if out.exists():
            state = _load(out)
            if (state["query"], state["since"], state["kind"]) != (query, _iso(since), kind):
                raise VoxError(f"{out} holds a different search; pass another --out.")
        else:
            until = _iso(until or datetime.now(timezone.utc))
            state = {"query": query, "since": _iso(since), "until": until, "kind": kind, "cursor": until, "pages": 0, "messages": []}
        _traverse(state, out, query)
        state["complete"] = True
        state["groups"] = dict(Counter(str(m["group_id"]) for m in state["messages"] if m.get("group_id")).most_common())
        return _save(out, state)

    def semantic(self, query, limit=50, out=None):
        """Groups found by Vox semantic search for QUERY (a topic description, not keywords)."""
        groups = []
        for row in _call("/search/ai/raw/GROUP", {"query": query, "limit": int(limit)}):
            report = row.get("report")
            if isinstance(report, str):
                try:
                    report = json.loads(report)
                except ValueError:
                    pass
            groups.append({"group_id": row.get("id"), "type": row.get("type"), "message_count": row.get("message_count"), "report": report})
        return _save(out or f"vox/semantic/{_slug(query)}.json", {"query": query, "groups": groups})

    def group(self, group_id, out=None):
        """Group card: name, alias, type, members, privacy."""
        card = _call(f"/groups/{int(group_id)}")
        card = {k: card.get(k) for k in ("id", "alias", "name", "title", "type", "members_count", "about", "is_private", "related_group")}
        if card.get("alias") and not card.get("is_private"):
            card["url"] = f"https://t.me/{card['alias']}"
        return _save(out or f"vox/groups/{int(group_id)}/card.json", card)

    def download(self, group_id, since, until=None, out=None):
        """Every message of a group from SINCE (union of frequent-word searches inside it). Resumes OUT."""
        gid = int(group_id)
        out = Path(out or f"vox/groups/{gid}/messages.json")
        if out.exists():
            state = _load(out)
            if (state["group_id"], state["since"]) != (gid, _iso(since)):
                raise VoxError(f"{out} holds a different download; pass another --out.")
        else:
            until = _iso(until or datetime.now(timezone.utc))
            state = {"group_id": gid, "since": _iso(since), "until": until, "kind": "all", "words_done": [], "cursor": None, "pages": 0, "messages": []}
        for word in FREQUENT_WORDS:
            if word in state["words_done"]:
                continue
            if state["cursor"] is None:
                state["cursor"] = state["until"]
            _traverse(state, out, word, include=str(gid))
            state["words_done"].append(word)
            _save(out, state)
        state["complete"] = True
        state["messages"].sort(key=lambda m: m.get("date") or "")
        return _save(out, state)

    def authors(self, download, top=30):
        """Most active authors of a downloaded group, with sample message URLs."""
        data = _load(download)
        counts = Counter(m["user_id"] for m in data["messages"] if m.get("user_id"))
        aliases, samples = {}, defaultdict(list)
        for m in data["messages"]:
            if m.get("user_id"):
                if not aliases.get(m["user_id"]):
                    aliases[m["user_id"]] = m.get("user_alias")
                if len(samples[m["user_id"]]) < 5 and m.get("message_url"):
                    samples[m["user_id"]].append(m["message_url"])
        rows = [{"user_id": uid, "user_alias": aliases[uid], "messages": n, "sample_urls": samples[uid]} for uid, n in counts.most_common(int(top))]
        return _save(Path(download).with_name("authors.json"), {"group_id": data.get("group_id"), "total_messages": len(data["messages"]), "authors": rows})

    def rank(self, relevant, *semantic, top=16, exclude="", out="vox/ranking.json"):
        """Top groups by relevant unique messages + group size, each normalised, +1 if found by both searches."""
        skip = {int(x) for x in str(exclude).replace("(", "").replace(")", "").split(",") if x.strip()}
        texts, names, sizes = defaultdict(set), {}, {}
        for m in _load(relevant)["messages"]:
            if m.get("group_id"):
                texts[m["group_id"]].add(m.get("text"))
                names.setdefault(m["group_id"], {"group_name": m.get("group_name"), "group_alias": m.get("group_alias")})
        for path in semantic:
            for g in _load(path)["groups"]:
                if g.get("group_id"):
                    sizes[g["group_id"]] = max(sizes.get(g["group_id"], 0), g.get("message_count") or 0)
        candidates = (set(texts) | set(sizes)) - skip
        max_unique = max((len(texts[g]) for g in candidates if g in texts), default=0) or 1
        max_size = max((math.log1p(sizes.get(g, 0)) for g in candidates), default=0) or 1
        rows = []
        for g in candidates:
            unique, both = len(texts.get(g, ())), g in texts and g in sizes
            score = unique / max_unique + math.log1p(sizes.get(g, 0)) / max_size + (1 if both else 0)
            rows.append({"group_id": g, **names.get(g, {}), "score": round(score, 4), "unique_relevant_messages": unique, "message_count": sizes.get(g), "in_text_search": g in texts, "in_semantic": g in sizes})
        rows.sort(key=lambda r: -r["score"])
        return _save(out, {"top": int(top), "groups": rows[: int(top)], "rest": rows[int(top) :]})


def main():
    try:
        fire.Fire(Vox, serialize=lambda value: value)
    except VoxError as exc:
        print(f"error: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
