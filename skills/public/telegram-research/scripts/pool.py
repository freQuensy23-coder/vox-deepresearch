"""The research folder's message pool: messages_raw.db (SQLite).

Every message Vox returned, deduplicated by key, with every source that found it.
Writes are single inserts, so a client never loads the whole pool, and SQLite
serializes parallel writers.

    messages(key PRIMARY KEY, id, group_id, group_name, group_alias, group_url,
             message_url, date, user_id, user_alias, text)
    sources(key, source)          # source: keyword:WORD or whale:USER_ID
"""

import sqlite3
from contextlib import closing
from pathlib import Path

DB = Path("messages_raw.db")
FIELDS = ("id", "group_id", "group_name", "group_alias", "group_url", "message_url", "date", "user_id", "user_alias", "text")


def connect():
    db = sqlite3.connect(DB, timeout=600)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute(f"CREATE TABLE IF NOT EXISTS messages (key TEXT PRIMARY KEY, {', '.join(FIELDS)})")
    db.execute("CREATE TABLE IF NOT EXISTS sources (key TEXT NOT NULL, source TEXT NOT NULL, PRIMARY KEY (key, source))")
    return db


def add(keyed, source):
    """Add (key, message) pairs found by SOURCE. Returns (new messages, pool size)."""
    with closing(connect()) as db, db:
        before = db.execute("SELECT count(*) FROM messages").fetchone()[0]
        db.executemany(
            f"INSERT OR IGNORE INTO messages (key, {', '.join(FIELDS)}) VALUES (?{', ?' * len(FIELDS)})",
            [(key, *(message.get(field) for field in FIELDS)) for key, message in keyed],
        )
        db.executemany("INSERT OR IGNORE INTO sources (key, source) VALUES (?, ?)", [(key, source) for key, _ in keyed])
        after = db.execute("SELECT count(*) FROM messages").fetchone()[0]
    return after - before, after


def rows():
    """Every message as a dict with its `sources` list."""
    with closing(connect()) as db:
        sources = {}
        for key, source in db.execute("SELECT key, source FROM sources ORDER BY rowid"):
            sources.setdefault(key, []).append(source)
        for row in db.execute(f"SELECT key, {', '.join(FIELDS)} FROM messages ORDER BY rowid"):
            yield {**{field: value for field, value in zip(FIELDS, row[1:]) if value is not None}, "sources": sources.get(row[0], [])}


def source_counts():
    """Messages in the pool per source."""
    if not DB.exists():
        return {}
    with closing(connect()) as db:
        return dict(db.execute("SELECT source, count(*) FROM sources GROUP BY source"))
