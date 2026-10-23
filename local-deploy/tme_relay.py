"""t.me proxy for the deep stage of Telegram research: public web pages only.

Every research thread on the host reaches t.me through this proxy, so one pace
holds for all of them: requests go to t.me one at a time, in arrival order, at
least `interval_seconds` apart. When t.me refuses (429, 403, 5xx or a network
error), every request waits (Retry-After, else a backoff doubling from
`backoff_seconds` up to `backoff_max_seconds`) and the refused one is retried; the
caller keeps its place. A request refused for longer than `give_up_seconds` fails
with a message telling the agent to do other work and retry later.

Only public web pages are forwarded, without cookies or any Telegram session:

    /s/{channel}[?before=ID | ?after=ID]                 a page of a public channel's posts
    /{name}/{id}?embed=1                                  one message of a public chat or channel
    /{name}/{id}?embed=1&comment=ID                       one comment under a channel post
    /{name}/{id}?embed=1&discussion=1&comments_limit=N    the latest comments under a channel post

Private chats (/c/...), invite links (/+..., /joinchat/...) and every other path
are refused. Settings: `tme_relay` in config.yaml.
"""

import http.client
import json
import re
import select
import socket
import ssl
import sys
import threading
import time
import urllib.parse
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

UPSTREAM = ("t.me", 443)
USER_AGENT = "Mozilla/5.0 (compatible; vox-deepresearch)"
MAX_BODY = 8 * 1024 * 1024
NAME = r"[A-Za-z][A-Za-z0-9_]{3,31}"
ID = r"[1-9][0-9]{0,11}"
# t.me paths that look like a name but are not a public chat or channel.
RESERVED = {
    "joinchat",
    "addlist",
    "addstickers",
    "addemoji",
    "addtheme",
    "share",
    "proxy",
    "socks",
    "login",
    "setlanguage",
    "boost",
    "contact",
    "invoice",
    "giftcode",
    "confirmphone",
    "iv",
    "nft",
}
COMMENTS_LIMIT = 100


class Rejected(Exception):
    def __init__(self, status):
        self.status = status


class Cancelled(Exception):
    pass


class QueueFull(Exception):
    pass


def upstream_target(target):
    """The t.me path for a caller's target, rebuilt from validated parts, or Rejected."""
    if len(target) > 512 or not target.startswith("/") or target.startswith("//") or "\\" in target or "#" in target:
        raise Rejected(400)
    parts = urllib.parse.urlsplit(target)
    if parts.scheme or parts.netloc or "%" in parts.path:
        raise Rejected(400)
    try:
        pairs = urllib.parse.parse_qsl(parts.query, keep_blank_values=True, strict_parsing=bool(parts.query), max_num_fields=4)
    except ValueError:
        raise Rejected(400) from None
    query = dict(pairs)
    if len(query) != len(pairs):
        raise Rejected(400)
    channel = re.fullmatch(rf"/s/({NAME})", parts.path)
    message = re.fullmatch(rf"/({NAME})/({ID})", parts.path)
    name = (channel or message).group(1) if (channel or message) else None
    if name is None or name.casefold() in RESERVED:
        raise Rejected(403)
    if channel:
        if not query:
            return f"/s/{name}"
        if len(query) == 1 and next(iter(query)) in {"before", "after"} and re.fullmatch(ID, next(iter(query.values()))):
            key, value = next(iter(query.items()))
            return f"/s/{name}?{key}={value}"
        raise Rejected(400)
    post = message.group(2)
    if query == {"embed": "1"}:
        return f"/{name}/{post}?embed=1"
    if set(query) == {"embed", "comment"} and query["embed"] == "1" and re.fullmatch(ID, query["comment"]):
        return f"/{name}/{post}?comment={query['comment']}&embed=1"
    if set(query) == {"embed", "discussion", "comments_limit"} and query["embed"] == "1" and query["discussion"] == "1":
        if re.fullmatch(r"[1-9][0-9]{0,2}", query["comments_limit"]) and int(query["comments_limit"]) <= COMMENTS_LIMIT:
            return f"/{name}/{post}?embed=1&discussion=1&comments_limit={query['comments_limit']}"
    raise Rejected(400)


def retry_after(value, now=time.time):
    """Seconds from a Retry-After header (seconds or an HTTP date), or None."""
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        pass
    try:
        moment = parsedate_to_datetime(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return max(0.0, moment.replace(tzinfo=moment.tzinfo or timezone.utc).timestamp() - now())


class Pace:
    """One t.me request at a time, FIFO, at least `interval` apart. A refusal moves
    the next start for everyone: Retry-After, else a doubling backoff."""

    def __init__(self, interval, backoff, backoff_max, queue_limit):
        self.interval, self.backoff, self.backoff_max, self.queue_limit = interval, backoff, backoff_max, queue_limit
        self.condition = threading.Condition()
        self.waiting = []
        self.active = False
        self.next_start = 0.0
        self.refusals = 0

    def acquire(self, cancelled):
        ticket = object()
        with self.condition:
            if len(self.waiting) >= self.queue_limit:
                raise QueueFull()
            self.waiting.append(ticket)
            try:
                while not (not self.active and self.waiting[0] is ticket):
                    if cancelled():
                        raise Cancelled()
                    self.condition.wait(0.05)
                self.waiting.pop(0)
                self.active = True
            finally:
                if ticket in self.waiting:
                    self.waiting.remove(ticket)
                self.condition.notify_all()

    def release(self):
        with self.condition:
            self.active = False
            self.condition.notify_all()

    def wait_turn(self, cancelled):
        """Sleep until the next request may start (only the holder calls this)."""
        while time.monotonic() < self.next_start:
            if cancelled():
                raise Cancelled()
            time.sleep(min(0.05, max(0.0, self.next_start - time.monotonic())))

    def served(self):
        self.refusals = 0
        self.next_start = time.monotonic() + self.interval

    def refused(self, delay=None):
        """Record a refusal; returns how long every request now waits."""
        if delay is None:
            delay = min(self.backoff * 2**self.refusals, self.backoff_max)
        self.refusals += 1
        delay = max(delay, self.interval)
        self.next_start = time.monotonic() + delay
        return delay

    def read(self):
        with self.condition:
            return {
                "active": int(self.active),
                "queued": len(self.waiting),
                "paused_seconds": round(max(0.0, self.next_start - time.monotonic()), 1),
                "refusals": self.refusals,
            }


class RelayServer(ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = 128

    def __init__(self, address, *, interval, backoff, backoff_max, give_up, queue_limit, call_timeout, upstream=UPSTREAM, tls=True):
        self.pace = Pace(interval, backoff, backoff_max, queue_limit)
        self.give_up, self.call_timeout, self.upstream, self.tls = give_up, call_timeout, upstream, tls
        super().__init__(address, RelayHandler)

    def fetch(self, target):
        """One GET to t.me: (status, Location, body). No cookies, no redirects followed."""
        host, port = self.upstream
        if self.tls:
            connection = http.client.HTTPSConnection(host, port, timeout=self.call_timeout, context=ssl.create_default_context())
        else:
            connection = http.client.HTTPConnection(host, port, timeout=self.call_timeout)
        try:
            connection.request(
                "GET",
                target,
                headers={"User-Agent": USER_AGENT, "Accept": "text/html", "Accept-Encoding": "identity", "Accept-Language": "en", "Connection": "close"},
            )
            response = connection.getresponse()
            body = response.read(MAX_BODY + 1)
            if len(body) > MAX_BODY:
                raise http.client.HTTPException("response too large")
            return response.status, response.getheader("Location"), response.getheader("Retry-After"), body
        finally:
            connection.close()

    def handle_error(self, request, client_address):
        pass


def log(event, **fields):
    print(json.dumps({"at": datetime.now(timezone.utc).isoformat(timespec="seconds"), "event": event, **fields}, ensure_ascii=False), flush=True)


class RelayHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "TmeRelay"
    sys_version = ""

    def log_message(self, format, *args):
        pass

    def send_error(self, code, message=None, explain=None):
        self.reply(code)

    def reply(self, status, payload=None, body=None, content_type="application/json", location=None):
        if body is None:
            body = json.dumps(payload if payload is not None else {"status": status}, ensure_ascii=False).encode()
        self.close_connection = True
        self.send_response(status)
        self.send_header("Connection", "close")
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        if location is not None:
            self.send_header("Location", location)
        self.end_headers()
        self.wfile.write(body)

    def disconnected(self):
        try:
            return bool(select.select([self.connection], [], [], 0)[0]) and not self.connection.recv(1, socket.MSG_PEEK | socket.MSG_DONTWAIT)
        except OSError:
            return True

    def do_GET(self):
        if self.path in {"/health", "/status"}:
            self.reply(200, {"ok": True} if self.path == "/health" else self.server.pace.read())
            return
        try:
            target = upstream_target(self.path)
        except Rejected as exc:
            self.reply(exc.status)
            return
        pace = self.server.pace
        try:
            pace.acquire(self.disconnected)
        except Cancelled:
            return
        except QueueFull:
            self.reply(
                503,
                {
                    "error": "queue_full",
                    "message": f"t.me queue is full: {pace.queue_limit} requests are already waiting. Do other work meanwhile (read collected messages, write) and retry in a couple of minutes.",
                },
            )
            return
        try:
            refused_since = None
            while True:
                pace.wait_turn(self.disconnected)
                try:
                    status, location, wait, body = self.server.fetch(target)
                except (OSError, http.client.HTTPException) as exc:
                    status, location, wait, body = None, None, None, repr(exc)
                if status is not None and status not in (403, 429) and status < 500:
                    pace.served()
                    log("served", target=target, status=status)
                    break
                refused_since = refused_since or time.monotonic()
                delay = pace.refused(retry_after(wait))
                log("refused", target=target, status=status, wait=round(delay, 1))
                if time.monotonic() - refused_since + delay > self.server.give_up:
                    self.reply(
                        503,
                        {
                            "error": "refused",
                            "message": f"t.me keeps refusing requests from this host (last answer: {status or 'network error'}); the proxy is waiting it out. "
                            "Do other work meanwhile (read collected messages, write) and retry in a few minutes.",
                        },
                    )
                    return
        except Cancelled:
            return
        finally:
            pace.release()
        if 300 <= status < 400:
            # Not followed: a public chat redirects /s/{name} to its main page.
            path = urllib.parse.urlsplit(location or "").path if (location or "").startswith(("https://t.me/", "/")) else None
            self.reply(status, {"status": status, "location": path}, location=path)
        elif status == 200:
            self.reply(200, body=body, content_type="text/html; charset=utf-8")
        else:
            self.reply(status, {"status": status})


def main():
    import deploy_config

    config = deploy_config.section("tme_relay")
    server = RelayServer(
        ("127.0.0.1", int(config["port"])),
        interval=float(config["interval_seconds"]),
        backoff=float(config["backoff_seconds"]),
        backoff_max=float(config["backoff_max_seconds"]),
        give_up=float(config["give_up_seconds"]),
        queue_limit=int(config["queue_limit"]),
        call_timeout=float(config["call_timeout_seconds"]),
    )
    log("listening", port=server.server_address[1], file=sys.argv[0])
    try:
        server.serve_forever()
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
