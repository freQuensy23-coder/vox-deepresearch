"""Technical facts of the research, generated from the data so the report never states them by hand.

    facts.py --out report/facts.md

Markdown: counts of pool, filtered messages, groups, keywords, whales, links and
contacts, then the full table of chats with direct links and message counts.
"""

import json
import re
import sys
from collections import Counter
from contextlib import closing
from pathlib import Path
from urllib.parse import urlsplit

import fire

import pool

FILTERED = Path("messages_filtered.json")
REGISTRY = Path("registry.json")
URL = re.compile(r"https?://[^\s<>()\[\]\"']+|\b(?:t\.me|wa\.me|instagram\.com|youtu\.be)/[^\s<>()\[\]\"']+", re.IGNORECASE)
HANDLE = re.compile(r"(?<![\w/.])@[A-Za-z]\w{3,31}")
PHONE = re.compile(r"(?<!\d)\+?\d[\d\s\-()]{9,15}\d(?!\d)")


def _cell(text):
    return str(text).replace("|", "\\|").replace("\n", " ")


def _chat_link(m):
    return m.get("group_url") or (m.get("message_url") or "").rsplit("/", 1)[0]


def _domain(url):
    host = urlsplit(url if "//" in url else "https://" + url).hostname or ""
    return host.removeprefix("www.")


def links(messages):
    """Unique links, @handles and phone numbers in the messages: counted, never listed."""
    urls, handles, phones = set(), set(), set()
    for m in messages:
        text = m.get("text") or ""
        urls.update(u.rstrip(".,;:!?") for u in URL.findall(text))
        handles.update(h.casefold() for h in HANDLE.findall(text))
        phones.update(re.sub(r"\D", "", p) for p in PHONE.findall(text))
    return urls, handles, phones


def pool_by_group():
    with closing(pool.connect()) as db:
        total = db.execute("SELECT count(*) FROM messages").fetchone()[0]
        return total, dict(db.execute("SELECT group_id, count(*) FROM messages GROUP BY group_id"))


def facts(out=None):
    """Write the markdown with the facts and the full table of chats to OUT (stdout without it)."""
    data = json.loads(FILTERED.read_text())
    messages = data["messages"]
    registry = json.loads(REGISTRY.read_text()) if REGISTRY.exists() else {"keywords": [], "whales": []}
    total, in_pool = pool_by_group()
    dates = sorted(m["date"] for m in messages if m.get("date"))
    urls, handles, phones = links(messages)
    domains = Counter(_domain(u) for u in urls)
    chats = {}
    for m in messages:
        chat = chats.setdefault(m.get("group_id"), {"name": m.get("group_name") or m.get("group_alias") or str(m.get("group_id")), "link": _chat_link(m), "topic": 0, "copies": 0})
        chat["topic"] += 1
        chat["copies"] += m.get("duplicates", 0) + 1
    kw = Counter(k["status"] for k in registry["keywords"])
    wh = Counter(w["status"] for w in registry["whales"])
    period = f"{dates[0][:10]} — {dates[-1][:10]}" if dates else "—"
    lines = [
        "## Технические данные",
        "",
        "| Показатель | Значение |",
        "|---|---:|",
        f"| Период сообщений по теме | {period} |",
        f"| Сообщений в пуле (ответы Vox без повторов) | {total} |",
        f"| Уникальных текстов в пуле | {data.get('unique_texts', '—')} |",
        f"| Сообщений по теме после Jev (уникальные тексты) | {len(messages)} |",
        f"| Они же с копиями (тиражирование) | {sum(c['copies'] for c in chats.values())} |",
        f"| Групп в пуле / по теме | {len(in_pool)} / {len(chats)} |",
        f"| Ключевых слов: найдено / всего | {kw['searched']} / {len(registry['keywords'])} |",
        f"| Китов: сообщения получены / всего | {wh['fetched']} / {len(registry['whales'])} |",
        f"| Уникальных ссылок в сообщениях по теме | {len(urls)} (доменов: {len(domains)}) |",
        f"| Уникальных @-контактов | {len(handles)} |",
        f"| Уникальных телефонов | {len(phones)} |",
        "",
        "Инструкция Jev: " + _cell(data.get("instruction", "—")) + f" (порог {data.get('threshold', '—')})",
        "",
        "### Ссылки по доменам (топ 15)",
        "",
        "| Домен | Уникальных ссылок |",
        "|---|---:|",
        *[f"| {_cell(d)} | {n} |" for d, n in domains.most_common(15)],
        "",
        "### Все группы по теме",
        "",
        "| # | Группа | Ссылка | По теме | С копиями | В пуле |",
        "|---:|---|---|---:|---:|---:|",
        *[
            f"| {i} | {_cell(c['name'])} | {c['link']} | {c['topic']} | {c['copies']} | {in_pool.get(gid, 0)} |"
            for i, (gid, c) in enumerate(sorted(chats.items(), key=lambda kv: -kv[1]["topic"]), 1)
        ],
        "",
    ]
    text = "\n".join(lines)
    if not out:
        return text
    Path(out).parent.mkdir(parents=True, exist_ok=True)
    Path(out).write_text(text, encoding="utf-8")
    return f"{out}: {len(chats)} groups"


def main():
    try:
        fire.Fire(facts, serialize=lambda value: value)
    except (OSError, ValueError, KeyError) as error:
        print(f"facts: {error}", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
