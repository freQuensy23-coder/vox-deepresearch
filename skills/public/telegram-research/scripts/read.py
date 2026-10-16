"""Compact reader for the filtered messages: for subagents that read thousands of messages.

    read.py --grep 'вахт|смен'                     # regex like rg, case-insensitive
    read.py --count --grep 'билет|дорога'          # only how many match, per chat
    read.py --chunks                               # the reading chunks of the whole selection
    read.py --offset 600 --end 900                 # read one chunk, page by page
    read.py --offset 100                           # next page

Reads messages_filtered.json (or --file). Near-identical texts are shown once
with their number of copies: the same advert posted in many chats, or with a
changed phone number, is one entry. A page holds as many entries as fit in
MAX_CHARS, so it is never cut by the tool output limit; the last line says
where the next page starts.
"""

import json
import re
import sys
import textwrap
from collections import Counter
from pathlib import Path

import fire

FILTERED = Path("messages_filtered.json")
CHUNK = 300  # entries per reader: fixed, so notes/START.md always names the same chunk


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


def read(file=None, grep=None, group=None, since=None, until=None, offset=0, end=None, max_chars=18000, width=100, count=False, chunks=False):
    """Print the messages that match, one block each: number, date, chat, author, copies, text, link.

    FILE: JSON in the messages_filtered.json format (default messages_filtered.json).
    GREP: regex on the text. GROUP: substring of the chat name or alias.
    SINCE, UNTIL: dates YYYY-MM-DD. OFFSET, END: entries [OFFSET, END), oldest first.
    MAX_CHARS: page size; keep it below the tool output limit.
    COUNT: print only the number of entries and their split by chat.
    CHUNKS: print the ranges that split all entries into reading chunks of CHUNK entries.
    """
    messages = json.loads(Path(file or FILTERED).read_text())["messages"]
    found = collapse(select(messages, grep, group, since, until))
    if count:
        by_chat = Counter(m.get("group_name") or m.get("group_alias") or str(m.get("group_id")) for m in found)
        head = f"{len(found)} entries ({len(messages)} messages before collapsing near-duplicates), {len(by_chat)} chats"
        return "\n".join([head] + [f"{n:>6}  {name}" for name, n in by_chat.most_common(30)])
    if chunks:
        return "\n".join(f"--offset {start} --end {min(start + CHUNK, len(found))}" for start in range(0, len(found), CHUNK))
    offset = int(offset)
    stop = min(int(end), len(found)) if end is not None else len(found)
    blocks, size, position = [], 0, offset
    while position < stop:
        block = render(found[position], position + 1, width)
        if blocks and size + len(block) > int(max_chars):
            break
        blocks.append(block)
        size += len(block) + 2
        position += 1
    tail = f"-- {offset + 1 if blocks else 0}–{position} of {len(found)}"
    if position < stop:
        tail += f"; next page: --offset {position}" + (f" --end {stop}" if end is not None else "")
    elif end is not None:
        tail += f"; range up to {stop} read to the end"
    blocks.append(tail)
    return "\n\n".join(blocks)


def main():
    try:
        fire.Fire(read, serialize=lambda value: value)
    except (OSError, ValueError, KeyError, re.error) as error:
        print(f"read: {error}", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
