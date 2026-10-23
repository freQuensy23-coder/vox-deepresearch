#!/usr/bin/env python3
"""Public Telegram web pages for the deep stage: a message, its neighbours, the
replies it answers, the comments under a channel post.

Only public chats and channels, through the t.me proxy at TME_RELAY_URL. The
proxy keeps one request pace for every research on the host and waits out
t.me's refusals itself, so a command may take minutes; never wrap it in
`timeout`. Private chats (t.me/c/...) and invite links are refused. No Telegram
account or session is used.

Run from the research folder. Every fetched page is kept in tme.db and never
fetched twice, so reopening a message costs nothing.

    tme.py show URL [--before 5] [--after 5]        # the message and its neighbours in the chat or channel
    tme.py thread URL                                # the messages it replies to, up to the first one
    tme.py comments URL [--limit 100]                # the latest comments under a channel post
    tme.py page CHANNEL [--before ID | --after ID]   # 20 posts of a channel, to scroll it
"""

import json
import os
import re
import sqlite3
import sys
import textwrap
import urllib.error
import urllib.parse
import urllib.request
from contextlib import closing
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path

import fire

DB = Path("tme.db")
NAME = r"[A-Za-z][A-Za-z0-9_]{3,31}"
# t.me pages name a channel's private discussion chat by its internal id (c1234567890).
PRIVATE = re.compile(r"c\d+", re.IGNORECASE)
MAX_CHARS = 18000
WIDTH = 100
VOID = {"area", "base", "br", "col", "embed", "hr", "img", "input", "link", "meta", "param", "source", "track", "wbr"}
MEDIA = {
    "tgme_widget_message_photo": "фото",
    "tgme_widget_message_video": "видео",
    "tgme_widget_message_roundvideo": "видеосообщение",
    "tgme_widget_message_voice": "голосовое",
    "tgme_widget_message_document": "файл",
    "tgme_widget_message_sticker": "стикер",
    "tgme_widget_message_poll": "опрос",
    "tgme_widget_message_location": "место",
}


class TmeError(Exception):
    pass


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}), _NoRedirect())


def _relay():
    value = os.getenv("TME_RELAY_URL")
    if not value:
        raise TmeError("TME_RELAY_URL is not set; it comes from sandbox.environment in config.yaml.")
    url = urllib.parse.urlsplit(value)
    if url.scheme != "http" or url.hostname not in {"localhost", "127.0.0.1", "::1"} or url.path not in {"", "/"}:
        raise TmeError("TME_RELAY_URL must be the local t.me proxy base URL.")
    return value.rstrip("/")


def _path(url):
    """Path segments and query of a public t.me link; private links are refused."""
    text = str(url).strip()
    parts = urllib.parse.urlsplit(text if "//" in text else "https://" + text)
    if (parts.hostname or "").removeprefix("www.") not in {"t.me", "telegram.me"}:
        raise TmeError(f"{url}: not a t.me link.")
    path = [p for p in parts.path.split("/") if p]
    if path and (path[0] in {"c", "joinchat"} or path[0].startswith("+")):
        raise TmeError(f"{url}: a private chat or invite link. Only public chats and channels may be read.")
    if path and path[0] == "s":
        path = path[1:]
    if path and PRIVATE.fullmatch(path[0]):
        raise TmeError(f"{url}: a private chat (internal id). Only public chats and channels may be read.")
    if not path or not re.fullmatch(NAME, path[0]):
        raise TmeError(f"{url}: expected a public chat or channel link like https://t.me/NAME/123.")
    return path, urllib.parse.parse_qs(parts.query)


def parse_url(url):
    """(name, message id, comment id or None) of a public t.me message link."""
    path, query = _path(url)
    if len(path) < 2 or not path[-1].isdigit():
        raise TmeError(f"{url}: expected a public message link like https://t.me/NAME/123.")
    comment = query.get("comment", [""])[0]
    return path[0], int(path[-1]), int(comment) if comment.isdigit() else None


class _Page(HTMLParser):
    """Messages of a t.me widget page: channel listing, single embed or discussion."""

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.messages, self.stack, self.current = [], [], None
        self.missing = False
        self.header = ""

    def _active(self):
        return set().union(*(roles for _, roles in self.stack)) if self.stack else set()

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        classes = set((attrs.get("class") or "").split())
        active = self._active()
        roles = set()
        if "tgme_widget_message" in classes and ("data-post" in attrs or "data-post-id" in attrs):
            post = attrs.get("data-post") or ""
            self.current = {
                "post": post,
                "id": int(attrs.get("data-post-id") or post.rsplit("/", 1)[-1]),
                "author": None,
                "owner": "",
                "signature": "",
                "forwarded": "",
                "reply_to": None,
                "reply_text": "",
                "media": [],
                "text": "",
                "date": None,
                "url": None,
            }
            self.messages.append(self.current)
        if "tgme_widget_message_error" in classes:
            self.missing = True
        if "js-header" in classes:
            roles.add("header")
        current = self.current
        if current is not None and "skip" not in active:
            if "tgme_widget_message_reply_template" in classes:
                roles.add("skip")
            elif "tgme_widget_message_reply" in classes:
                roles.add("reply")
                target = attrs.get("data-reply-to")
                link = re.search(rf"t\.me/({NAME})/(\d+)", attrs.get("href") or "")
                current["reply_to"] = f"{link.group(1)}/{link.group(2)}" if link else (f"#{target}" if target else "?")
            elif "js-message_reply_text" in classes:
                roles.add("reply_text")
            elif "js-message_text" in classes and "reply" not in active:
                roles.add("text")
            elif "tgme_widget_message_author_name" in classes and "reply" not in active:
                roles.add("author")
                current["author"] = current["author"] or ""
            elif "tgme_widget_message_owner_name" in classes:
                roles.add("owner")
            elif "tgme_widget_message_from_author" in classes:
                roles.add("signature")
            elif "tgme_widget_message_forwarded_from_name" in classes:
                roles.add("forwarded")
            elif "tgme_widget_message_date" in classes and tag == "a":
                current["url"] = attrs.get("href")
                roles.add("date")
            for name, label in MEDIA.items():
                if name in classes and label not in current["media"]:
                    current["media"].append(label)
            if tag == "time" and "date" in active and attrs.get("datetime"):
                current["date"] = attrs["datetime"]
            if tag == "br" and "text" in active:
                current["text"] += "\n"
            if tag == "a" and "text" in active and attrs.get("href"):
                roles.add("link:" + attrs["href"])
        if tag not in VOID:
            self.stack.append((tag, roles))

    def handle_endtag(self, tag):
        for index in range(len(self.stack) - 1, -1, -1):
            if self.stack[index][0] == tag:
                for _, roles in self.stack[index:]:
                    for role in roles:
                        if role.startswith("link:") and self.current is not None:
                            self._link(role[5:])
                del self.stack[index:]
                return

    def _link(self, href):
        """Keep a hidden link target visible: `text <href>` unless the text already shows it."""
        tail = self.current["text"][-200:].casefold()
        bare = re.sub(r"^https?://(www\.)?", "", href).rstrip("/").casefold()
        mention = re.fullmatch(r"t\.me/(\w+)", bare)
        if not bare or bare in tail or href.startswith("?") or (mention and tail.endswith("@" + mention.group(1))):
            return
        self.current["text"] += f" <{href}>"

    def handle_data(self, data):
        active = self._active()
        if "header" in active:
            self.header += data
        if self.current is None or "skip" in active:
            return
        for role in ("reply_text", "text", "author", "owner", "signature", "forwarded"):
            if role in active:
                self.current[role] += data
                return


def parse_page(html):
    """(messages, missing, header) of a t.me widget page."""
    page = _Page()
    page.feed(html)
    page.close()
    messages = []
    for m in page.messages:
        # A chat message names its author (empty when the user hides it); a channel post has only a signature or the channel.
        author = " ".join(m["author"].split()) or "автор скрыт" if m["author"] is not None else " ".join((m["signature"] or m["owner"]).split())
        messages.append(
            {
                "id": m["id"],
                "post": m["post"],
                "date": m["date"],
                "author": author,
                "forwarded": " ".join(m["forwarded"].split()),
                "reply_to": m["reply_to"],
                "reply_text": " ".join(m["reply_text"].split()),
                "media": m["media"],
                "text": m["text"].strip(),
                "url": m["url"],
            }
        )
    return messages, page.missing, " ".join(page.header.split())


def _connect():
    db = sqlite3.connect(DB, timeout=600)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("CREATE TABLE IF NOT EXISTS pages (path TEXT PRIMARY KEY, status INTEGER, location TEXT, page TEXT, fetched_at TEXT)")
    return db


def _fetch(path):
    """(status, location, html) of one t.me path from the proxy."""
    request = urllib.request.Request(_relay() + path, headers={"Accept": "text/html"})
    try:
        with _OPENER.open(request, timeout=None) as response:
            return response.status, None, response.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        try:
            detail = json.loads(exc.read(65536) or b"{}")
        except ValueError:
            detail = {}
        if 300 <= exc.code < 400:
            return exc.code, detail.get("location") or exc.headers.get("Location"), ""
        if exc.code == 404:
            return 404, None, ""
        if isinstance(detail, dict) and detail.get("message"):
            raise TmeError(detail["message"]) from None
        if exc.code == 403:
            raise TmeError(f"{path}: the t.me proxy reads only public chats and channels.") from None
        raise TmeError(f"HTTP {exc.code} from the t.me proxy at {path}.") from None
    except (urllib.error.URLError, OSError) as exc:
        raise TmeError(f"Network error ({type(exc).__name__}) at the t.me proxy; is it running?") from None


def get(path):
    """{status, location, messages, missing, header} of one page; fetched once, then read from tme.db."""
    with closing(_connect()) as db:
        row = db.execute("SELECT status, location, page FROM pages WHERE path = ?", (path,)).fetchone()
    if row:
        return {"status": row[0], "location": row[1], **json.loads(row[2])}
    status, location, html = _fetch(path)
    messages, missing, header = parse_page(html) if html else ([], status == 404, "")
    page = {"messages": messages, "missing": missing, "header": header}
    with closing(_connect()) as db, db:
        db.execute(
            "INSERT OR REPLACE INTO pages VALUES (?, ?, ?, ?, ?)",
            (path, status, location, json.dumps(page, ensure_ascii=False), datetime.now(timezone.utc).isoformat(timespec="seconds")),
        )
    return {"status": status, "location": location, **page}


def _public(post):
    """(name, id) of a `name/id` post of a public chat, None for a private one."""
    link = re.fullmatch(rf"({NAME})/(\d+)", post or "")
    return (link.group(1), int(link.group(2))) if link and not PRIVATE.fullmatch(link.group(1)) else None


def _message(name, mid):
    """One message by its embed page, or None if it is deleted or missing."""
    page = get(f"/{name}/{mid}?embed=1")
    found = [m for m in page["messages"] if m["id"] == mid]
    return found[0] if found and not page["missing"] else None


def _channel_page(name, before=None, after=None):
    """Posts of a public channel's /s/ page, or None if NAME is a chat (t.me redirects)."""
    with closing(_connect()) as db:
        # Any /s/ page of NAME that redirected already says it is a chat: no request needed.
        if db.execute("SELECT 1 FROM pages WHERE (path = ? OR path GLOB ?) AND status BETWEEN 300 AND 399", (f"/s/{name}", f"/s/{name}[?]*")).fetchone():
            return None
    query = f"?before={before}" if before else (f"?after={after}" if after else "")
    page = get(f"/s/{name}{query}")
    if 300 <= page["status"] < 400:
        return None
    return sorted(page["messages"], key=lambda m: m["id"])


def render(m, name, mark=False, width=WIDTH):
    when = (m.get("date") or "")[:16].replace("T", " ")
    head = [f"{'▶ ' if mark else ''}#{m['id']}", when, m.get("author") or "?"]
    if m.get("forwarded"):
        head.append(f"переслано от {m['forwarded']}")
    if m.get("reply_to"):
        quote = m.get("reply_text") or ""
        head.append(f"↩ {m['reply_to']}" + (f" «{quote[:120]}{'…' if len(quote) > 120 else ''}»" if quote else ""))
    if m.get("media"):
        head.append("[" + ", ".join(m["media"]) + "]")
    body = textwrap.fill(" ".join((m.get("text") or "").split()), width=int(width), initial_indent="  ", subsequent_indent="  ")
    url = m.get("url") or "https://t.me/{}/{}".format(name, m["id"])
    return "\n".join(line for line in (" · ".join(h for h in head if h), body, "  " + url) if line.strip())


def _blocks(head, blocks, tail=None, max_chars=MAX_CHARS):
    """HEAD, as many BLOCKS as fit in MAX_CHARS, then TAIL or how many were left out."""
    out, size = [head], len(head)
    for index, block in enumerate(blocks):
        if index and size + len(block) > int(max_chars):
            out.append(f"-- ещё {len(blocks) - index} не показано: сузи окно")
            return "\n\n".join(out)
        out.append(block)
        size += len(block) + 2
    if tail:
        out.append(tail)
    return "\n\n".join(out)


class Tme:
    """Public t.me pages for the deep stage. Run it from the research folder."""

    def show(self, url, before=5, after=5, max_chars=MAX_CHARS):
        """The message at URL and its neighbours: BEFORE earlier and AFTER later
        messages of the same chat or channel, oldest first, the message marked ▶."""
        name, mid, comment = parse_url(url)
        if comment is not None:
            page = get(f"/{name}/{mid}?embed=1&comment={comment}")
            if page["missing"] or not page["messages"]:
                return f"{name}/{mid}?comment={comment}: комментарий удалён или не найден"
            return render(page["messages"][0], name, mark=True) + f"\n\n-- соседние комментарии: tme.py comments https://t.me/{name}/{mid}"
        low, high = max(1, mid - int(before)), mid + int(after)
        posts = _channel_page(name, before=high + 1)
        if posts is None:
            kind = "чат"
            found = {i: _message(name, i) for i in range(low, high + 1)}
        else:
            kind = "канал"
            found = {}
            while posts:
                found.update({m["id"]: m for m in posts if low <= m["id"] <= high})
                if posts[0]["id"] <= low:
                    break
                posts = _channel_page(name, before=posts[0]["id"])
            found = {i: found.get(i) for i in range(low, high + 1)}
        blocks = [render(m, name, mark=i == mid) if m else f"{'▶ ' if i == mid else ''}#{i} · нет: удалено, служебное или не найдено" for i, m in found.items()]
        if kind == "канал":
            blocks = [b for i, b in zip(found, blocks) if found[i] or i == mid]
        return _blocks(f"{name} ({kind}) · #{low}–#{high}", blocks, max_chars=max_chars)

    def thread(self, url, depth=50, max_chars=MAX_CHARS):
        """The messages URL replies to, one after another up to the first one,
        oldest first, the message itself last."""
        name, mid, comment = parse_url(url)
        if comment is not None:
            # A comment lives in the channel's discussion chat: follow it there if
            # that chat is public, else within the post's public comments.
            posts = [m["post"] for m in get(f"/{name}/{mid}?embed=1&comment={comment}")["messages"] if m.get("post")]
            if not posts:
                return f"{name}/{mid}?comment={comment}: комментарий удалён или не найден"
            if _public(posts[0]) is None:
                return self._comment_thread(name, mid, comment, int(depth), max_chars)
            name, mid = _public(posts[0])
        chain, seen, at = [], set(), (name, mid)
        while at and at not in seen and len(chain) <= int(depth):
            seen.add(at)
            message = _message(*at)
            if message is None:
                chain.append((at, None))
                break
            chain.append((at, message))
            at = _public(message.get("reply_to"))
        blocks = [render(m, n, mark=(n, i) == (name, mid)) if m else f"#{i} в {n} · нет: удалено или не найдено" for (n, i), m in reversed(chain)]
        tail = None
        if chain and chain[-1][1] is not None and (chain[-1][1].get("reply_to") or "").startswith("#"):
            tail = "-- выше по цепочке ответ на комментарий: смотри comments у поста"
        return _blocks(f"{name}/{mid}: цепочка ответов, {len(chain)} сообщ.", blocks, tail, max_chars=max_chars)

    def _comment_thread(self, name, mid, comment, depth, max_chars):
        """The reply chain of a comment from the post's public comments page."""
        page = get(f"/{name}/{mid}?embed=1&discussion=1&comments_limit=100")
        by_id = {m["id"]: m for m in page["messages"]}
        chain, at = [], comment
        while at in by_id and at not in [m["id"] for m in chain] and len(chain) <= depth:
            chain.append(by_id[at])
            reply = by_id[at].get("reply_to") or ""
            at = int(reply[1:]) if reply.startswith("#") and reply[1:].isdigit() else None
        if not chain:
            return f"{name}/{mid}?comment={comment}: комментария нет среди последних 100 под постом; чат обсуждения закрыт"
        tail = "-- начало цепочки старше последних 100 комментариев; чат обсуждения закрыт" if at is not None else None
        blocks = [render(m, name, mark=m["id"] == comment) for m in reversed(chain)]
        return _blocks(f"{name}/{mid}?comment={comment}: цепочка ответов, {len(chain)} коммент.", blocks, tail, max_chars=max_chars)

    def comments(self, url, limit=100, max_chars=MAX_CHARS):
        """The latest LIMIT comments (at most 100) under the channel post at URL,
        oldest first. If the post has more, says where the discussion chat is."""
        name, mid, _ = parse_url(url)
        limit = max(1, min(100, int(limit)))
        page = get(f"/{name}/{mid}?embed=1&discussion=1&comments_limit={limit}")
        found = sorted(page["messages"], key=lambda m: m["id"])
        head = f"{name}/{mid}: {page['header'] or 'комментариев нет'}"
        if not found:
            return head + "\n\nКомментариев нет или они выключены."
        tail = None
        total = re.match(r"(\d+)", page["header"] or "")
        if total and int(total.group(1)) > len(found):
            chat = get(f"/{name}/{mid}?embed=1&comment={found[0]['id']}")
            where = next((_public(m.get("post")) for m in chat["messages"] if _public(m.get("post"))), None)
            tail = f"-- показаны последние {len(found)} из {total.group(1)}." + (
                f" Ранние — в чате обсуждения: tme.py show https://t.me/{where[0]}/{found[0]['id']} --before 20 --after 0" if where else " Ранние недоступны: чат обсуждения закрыт."
            )
        return _blocks(head, [render(m, name) for m in found], tail, max_chars=max_chars)

    def page(self, channel, before=None, after=None, max_chars=MAX_CHARS):
        """20 posts of a public CHANNEL (name or link): the latest, or those BEFORE / AFTER a post id."""
        name = str(channel).strip() if re.fullmatch(NAME, str(channel).strip()) else _path(channel)[0][0]
        posts = _channel_page(name, before=before, after=after)
        if posts is None:
            return f"{name} — чат, а не канал: у чатов нет ленты. Открывай сообщения через show."
        if not posts:
            return f"{name}: постов нет."
        tail = f"-- раньше: page {name} --before {posts[0]['id']}; позже: page {name} --after {posts[-1]['id']}"
        return _blocks(f"{name} (канал) · #{posts[0]['id']}–#{posts[-1]['id']}", [render(m, name) for m in posts], tail, max_chars=max_chars)


def main():
    try:
        fire.Fire(Tme, serialize=lambda value: value)
    except TmeError as exc:
        print(f"error: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
