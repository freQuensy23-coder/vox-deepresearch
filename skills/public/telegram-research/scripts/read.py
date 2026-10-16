"""Compact reader for the filtered messages: for subagents that read thousands of messages.

    read.py --grep 'вахт|смен' --limit 50          # regex like rg, case-insensitive
    read.py --count --grep 'билет|дорога'          # only how many match, per chat
    read.py --vertical NAME [--rejected]           # output of `jev.py subset`
    read.py --offset 100 --limit 100               # next page

Reads messages_filtered.json (or --file). Near-identical texts are shown once
with their number of copies: the same advert posted in many chats, or with a
changed phone number, is one entry.
"""

import json
import re
import sys
import textwrap
from collections import Counter
from pathlib import Path

import fire

FILTERED = Path("messages_filtered.json")


def _shape(text):
    """Texts that start with the same 40 words (ignoring case, digits, punctuation) are near-duplicates."""
    return " ".join(re.sub(r"[\W\d_]+", " ", (text or "").casefold()).split()[:40])


def collapse(messages):
    """One entry per near-duplicate group: the earliest message, `copies` = all copies in it."""
    groups = {}
    for m in sorted(messages, key=lambda m: m.get("date") or ""):
        shape = _shape(m.get("text"))
        if shape in groups:
            first = groups[shape]
            first["copies"] += m.get("duplicates", 0) + 1
            first["chats"].add(m.get("group_id"))
        else:
            groups[shape] = {**m, "copies": m.get("duplicates", 0) + 1, "chats": {m.get("group_id")}}
    return list(groups.values())


def select(messages, grep=None, group=None, since=None, until=None):
    pattern = re.compile(grep, re.IGNORECASE) if grep else None
    out = []
    for m in messages:
        date = (m.get("date") or "")[:10]
        if pattern and not pattern.search(m.get("text") or ""):
            continue
        if group and str(group).casefold() not in f"{m.get('group_name')} {m.get('group_alias')}".casefold():
            continue
        if (since and date < str(since)) or (until and date > str(until)):
            continue
        out.append(m)
    return out


def render(m, number, width):
    where = m.get("group_name") or m.get("group_alias") or m.get("group_id")
    copies = f" · ×{m['copies']} в {len(m['chats'])} чатах" if m["copies"] > 1 else ""
    head = f"#{number} {(m.get('date') or '')[:10]} · {where} · @{m.get('user_alias') or m.get('user_id')}{copies}"
    body = textwrap.fill(" ".join((m.get("text") or "").split()), width=int(width), initial_indent="  ", subsequent_indent="  ")
    return f"{head}\n{body}\n  {m.get('message_url')}"


def read(file=None, vertical=None, rejected=False, grep=None, group=None, since=None, until=None, offset=0, limit=100, width=100, count=False):
    """Print the messages that match, one block each: number, date, chat, author, copies, text, link.

    FILE: JSON in the messages_filtered.json format (default messages_filtered.json).
    VERTICAL, REJECTED: read verticals/VERTICAL/accepted.json (rejected.json with REJECTED).
    GREP: regex on the text. GROUP: substring of the chat name or alias.
    SINCE, UNTIL: dates YYYY-MM-DD. OFFSET, LIMIT: page of the result, oldest first.
    COUNT: print only the number of entries and their split by chat.
    """
    if vertical:
        file = Path("verticals") / vertical / ("rejected.json" if rejected else "accepted.json")
    messages = json.loads(Path(file or FILTERED).read_text())["messages"]
    found = collapse(select(messages, grep, group, since, until))
    if count:
        by_chat = Counter(m.get("group_name") or m.get("group_alias") or str(m.get("group_id")) for m in found)
        head = f"{len(found)} entries ({len(messages)} messages before collapsing near-duplicates), {len(by_chat)} chats"
        return "\n".join([head] + [f"{n:>6}  {name}" for name, n in by_chat.most_common(30)])
    offset, limit = int(offset), int(limit)
    page = found[offset : offset + limit]
    blocks = [render(m, offset + i + 1, width) for i, m in enumerate(page)]
    end = offset + len(page)
    blocks.append(f"-- {offset + 1 if page else 0}–{end} of {len(found)}" + (f"; next page: --offset {end}" if end < len(found) else ""))
    return "\n\n".join(blocks)


def main():
    try:
        fire.Fire(read, serialize=lambda value: value)
    except (OSError, ValueError, KeyError, re.error) as error:
        print(f"read: {error}", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
