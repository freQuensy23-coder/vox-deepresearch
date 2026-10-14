import http.client
import ipaddress
import json
import math
import os
import re
import select
import socket
import ssl
import threading
import time
import urllib.parse
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from vox_json_probe import CompletionProbe


UPSTREAM = "api.vox-lab.com"
REQUEST_DEADLINE = 10.0
DEFAULTS = dict(client_timeout=30.0)
MAX_TARGET = 8192
MAX_HEADERS = 16384
MAX_BODY = 16384
INTEGER_BOUNDS = dict(
    limit=(1, 10000),
    offset=(0, 10000),
    timeout=(10, 600),
    messages_limit=(1, 100),
    messages_min=(1, 1),
    members_min=(0, 0),
    groups_limit=(1, 50),
    groups_offset=(0, 1000),
    new_users_limit=(0, 0),
)
SEARCH_FIELDS = {
    "query",
    "limit",
    "from_date",
    "to_date",
    "order",
    "message_type",
    "include_groups_ids",
    "exclude_groups_ids",
    "exclude_from_users",
    "exclude_forwards",
    "deduplicate",
    "timeout",
    "stream",
}
PREVIEW_FIELDS = {
    "groups_search_query",
    "groups_limit",
    "groups_offset",
    "groups_regex",
    "run_parser",
    "find_new_users",
    "new_users_limit",
}
REQUEST_HEADERS = {
    "host",
    "authorization",
    "accept",
    "user-agent",
    "connection",
    "content-length",
    "content-type",
    "accept-encoding",
    "x-vox-tool-trace",
}


class Rejected(Exception):
    def __init__(self, status):
        self.status = status


class Cancelled(Exception):
    pass


def unique_object(pairs):
    result = dict(pairs)
    if len(result) != len(pairs):
        raise ValueError("duplicate fields")
    return result


def validate_fields(values, allowed, path, json_body=False):
    if not set(values) <= allowed:
        raise Rejected(400)
    for name, value in values.items():
        if name in {"run_parser", "find_new_users"}:
            if value is not False if json_body else value != "false":
                raise Rejected(400)
        elif name in INTEGER_BOUNDS:
            if json_body:
                if type(value) is not int:
                    raise Rejected(400)
                number = value
            else:
                if not re.fullmatch(r"[0-9]{1,19}", value):
                    raise Rejected(400)
                number = int(value)
            low, high = INTEGER_BOUNDS[name]
            if name == "limit":
                high = (
                    1000
                    if path == "/search/ai/raw/GROUP"
                    else 500
                    if path.startswith("/user_tracker/")
                    else high
                )
            if not low <= number <= high:
                raise Rejected(400)
        else:
            if (
                not isinstance(value, str)
                or len(value) > 4096
                or any(ord(c) < 32 or ord(c) == 127 for c in value)
            ):
                raise Rejected(400)
            if name == "query" and (
                not value.strip() or (path == "/search/text" and len(value) >= 50)
            ):
                raise Rejected(400)
            choices = dict(
                order={"asc", "desc"},
                message_type={"all", "post", "comment"},
                stream={"true", "false"},
                exclude_forwards={"true", "false"},
                deduplicate={"true", "false"},
            )
            if name in choices and value not in choices[name]:
                raise Rejected(400)
            if name in {"from_date", "to_date"}:
                try:
                    datetime.fromisoformat(value.replace("Z", "+00:00"))
                except ValueError:
                    raise Rejected(400) from None
            if name in {
                "include_groups_ids",
                "exclude_groups_ids",
                "exclude_from_users",
            }:
                if not re.fullmatch(r"-?[1-9][0-9]{0,18}(,-?[1-9][0-9]{0,18})*", value):
                    raise Rejected(400)
                if any(abs(int(number)) > 2**63 - 1 for number in value.split(",")):
                    raise Rejected(400)
    if path == "/reporter/parse" and not {
        "run_parser",
        "find_new_users",
        "new_users_limit",
    } <= set(values):
        raise Rejected(400)


def validate_target(method, target):
    if len(target.encode("utf-8")) > MAX_TARGET:
        raise Rejected(414)
    if (
        not target.startswith("/")
        or target.startswith("//")
        or "#" in target
        or "\\" in target
    ):
        raise Rejected(400)
    parts = urllib.parse.urlsplit(target)
    if parts.scheme or parts.netloc or "%" in parts.path:
        raise Rejected(400)
    if method not in {"GET", "POST"}:
        raise Rejected(405)
    if re.search(r"%(?![0-9A-Fa-f]{2})", parts.query):
        raise Rejected(400)
    try:
        pairs = urllib.parse.parse_qsl(
            parts.query,
            keep_blank_values=True,
            strict_parsing=True,
            encoding="utf-8",
            errors="strict",
            max_num_fields=32,
        )
        values = unique_object(pairs)
    except (ValueError, UnicodeError):
        raise Rejected(400) from None
    path = parts.path
    if method == "POST":
        if path != "/reporter/parse":
            raise Rejected(405)
        if values:
            validate_fields(values, PREVIEW_FIELDS, path)
        return path, values
    if path in {"/health", "/status"}:
        allowed = set()
    elif path == "/search/text":
        allowed = SEARCH_FIELDS
    elif path == "/search/ai/raw/GROUP":
        allowed = {"query", "limit", "offset"}
    elif re.fullmatch(r"/groups/-[1-9][0-9]{0,18}", path):
        allowed = set()
    elif re.fullmatch(r"/groups/-[1-9][0-9]{0,18}/messages", path):
        allowed = {"messages_limit", "messages_min", "members_min"}
    elif re.fullmatch(r"/ai_analytics/GROUP/-[1-9][0-9]{0,18}", path):
        allowed = set()
    elif re.fullmatch(r"/ai_analytics/custom/GROUP/-[1-9][0-9]{0,18}", path):
        allowed = {"custom_prompt"}
    elif re.fullmatch(
        r"/user_tracker/[1-9][0-9]{0,18}/panels/(groups|top_groups|messages)", path
    ):
        allowed = {"limit"}
    else:
        raise Rejected(403)
    validate_fields(values, allowed, path)
    return path, values


class Gate:
    """One active request and an unbounded FIFO of callers; no adaptive state."""

    def __init__(self):
        self.condition = threading.Condition()
        self.waiting = []
        self.active = False

    def acquire(self, cancelled):
        ticket = object()
        with self.condition:
            self.waiting.append(ticket)
            self.condition.notify_all()
            try:
                while True:
                    if cancelled():
                        raise Cancelled()
                    if not self.active and self.waiting[0] is ticket:
                        self.waiting.pop(0)
                        self.active = True
                        return
                    self.condition.wait(0.05)
            finally:
                if ticket in self.waiting:
                    self.waiting.remove(ticket)
                self.condition.notify_all()

    def release(self):
        with self.condition:
            self.active = False
            self.condition.notify_all()

    def read(self):
        with self.condition:
            return {
                "active": int(self.active),
                "queued": len(self.waiting),
                "max_inflight": 1,
                "request_deadline": REQUEST_DEADLINE,
            }


def retry_delay(value):
    if value is not None:
        try:
            delay = float(value)
        except ValueError:
            try:
                date = parsedate_to_datetime(value)
                delay = (
                    date.replace(tzinfo=date.tzinfo or timezone.utc).timestamp()
                    - time.time()
                )
            except (ValueError, TypeError, OverflowError):
                delay = 0.1
        if math.isfinite(delay):
            return max(0, delay)
    return 0.1


def fallback_target(target):
    """Retry an interrupted text search without streaming; preserve the search."""
    parts = urllib.parse.urlsplit(target)
    if parts.path != "/search/text":
        return target
    values = urllib.parse.parse_qsl(parts.query, keep_blank_values=True)
    values = [(key, value) for key, value in values if key != "stream"]
    values.append(("stream", "false"))
    return urllib.parse.urlunsplit(
        ("", "", parts.path, urllib.parse.urlencode(values), "")
    )


class HTTPSUpstream:
    def __init__(self, timeout):
        self.deadline = time.monotonic() + timeout
        self.context = ssl.create_default_context()
        self.connection = http.client.HTTPConnection(UPSTREAM, 443, timeout=timeout)
        self.connection._create_connection = self.connect_socket

    def connect_socket(self, address, timeout, source_address=None):
        # DNS has no socket to abort. Bound only its wait; a late resolver cannot
        # start a Vox connection because connection creation stays in this caller.
        done = threading.Event()
        addresses, errors = [], []

        def resolve():
            try:
                addresses.extend(socket.getaddrinfo(*address, type=socket.SOCK_STREAM))
            except OSError as exc:
                errors.append(exc)
            finally:
                done.set()

        threading.Thread(target=resolve, daemon=True).start()
        while not done.wait(0.01):
            if getattr(self, "aborted", False) or time.monotonic() >= self.deadline:
                raise TimeoutError("request deadline")
        if errors:
            raise errors[0]
        for family, kind, protocol, _, target in addresses:
            remaining = self.deadline - time.monotonic()
            if remaining <= 0 or getattr(self, "aborted", False):
                raise TimeoutError("request deadline")
            connection = socket.socket(family, kind, protocol)
            self.socket = connection
            try:
                connection.settimeout(remaining)
                if source_address:
                    connection.bind(source_address)
                connection.connect(target)
                return connection
            except OSError as exc:
                connection.close()
                errors.append(exc)
        if errors:
            raise errors[-1]
        raise OSError("no upstream address")

    def request(self, method, target, body, headers):
        self.connection.connect()
        self.socket = self.connection.sock
        if hasattr(self, "context"):
            self.connection.sock = self.socket = self.context.wrap_socket(
                self.socket, server_hostname=UPSTREAM, do_handshake_on_connect=False
            )
            remaining = self.deadline - time.monotonic()
            if remaining <= 0 or getattr(self, "aborted", False):
                self.close()
                raise TimeoutError("request deadline")
            self.socket.settimeout(remaining)
            self.socket.do_handshake()
        if getattr(self, "aborted", False):
            self.close()
            raise TimeoutError("request aborted")
        self.connection.request(method, target, body=body, headers=headers)
        return self.connection.getresponse()

    def abort(self):
        self.aborted = True
        connection = getattr(self, "socket", None) or self.connection.sock
        if connection is not None:
            try:
                connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass

    def close(self):
        self.abort()
        self.connection.close()


class RelayServer(ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = 128

    def __init__(
        self, address, *, key_file, state_dir, transport_factory=HTTPSUpstream
    ):
        self.options = DEFAULTS.copy()
        key = Path(key_file).read_text(encoding="utf-8").strip()
        if (
            not key
            or len(key) > 4096
            or any(ord(c) <= 32 or ord(c) >= 127 for c in key)
        ):
            raise ValueError("invalid relay key")
        self.key = key
        self.state_dir = Path(state_dir)
        self.gate = Gate()
        self.transport_factory = transport_factory
        super().__init__(address, RelayHandler)

    def handle_error(self, request, client_address):
        pass


class RelayHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "VoxRelay"
    sys_version = ""

    def log_message(self, format, *args):
        pass

    def send_error(self, code, message=None, explain=None):
        self.reply(code)

    def reply(self, status, payload=None, retry_after=None):
        body = json.dumps(
            payload if payload is not None else {"status": status},
            separators=(",", ":"),
        ).encode()
        self.close_connection = True
        self.send_response(status)
        self.send_header("Connection", "close")
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        if retry_after is not None:
            self.send_header("Retry-After", str(retry_after))
        self.end_headers()
        self.wfile.write(body)

    def abort_client(self):
        try:
            self.connection.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass

    def handle_one_request(self):
        self.connection.settimeout(self.server.options["client_timeout"])
        self.validation_timer = threading.Timer(
            self.server.options["client_timeout"], self.abort_client
        )
        self.validation_timer.start()
        try:
            super().handle_one_request()
        finally:
            self.validation_timer.cancel()
            self.close_connection = True

    def __getattr__(self, name):
        if name.startswith("do_"):
            return self.do_request
        raise AttributeError(name)

    def validate(self):
        path, values = validate_target(self.command, self.path)
        headers = list(self.headers.items())
        if sum(len(k) + len(v) + 4 for k, v in headers) > MAX_HEADERS:
            raise Rejected(431)
        names = [name.lower() for name, value in headers]
        if len(names) != len(set(names)) or not set(names) <= REQUEST_HEADERS:
            raise Rejected(400)
        if any(
            any(ord(c) < 32 or ord(c) == 127 for c in value) for name, value in headers
        ):
            raise Rejected(400)
        length = self.headers.get("Content-Length", "0")
        if not re.fullmatch(r"[0-9]{1,9}", length):
            raise Rejected(400)
        length = int(length)
        if length > MAX_BODY:
            raise Rejected(413)
        if self.command == "GET" and length:
            raise Rejected(400)
        body = self.rfile.read(length) if length else None
        if body is not None and len(body) != length:
            raise Rejected(400)
        if self.command == "POST":
            if body is not None:
                if (
                    values
                    or self.headers.get("Content-Type", "").split(";")[0].lower()
                    != "application/json"
                ):
                    raise Rejected(400)
                try:
                    data = json.loads(body, object_pairs_hook=unique_object)
                    if not isinstance(data, dict):
                        raise ValueError("invalid body")
                    validate_fields(data, PREVIEW_FIELDS, path, json_body=True)
                except (ValueError, UnicodeError):
                    raise Rejected(400) from None
            elif not values:
                raise Rejected(400)
        return path, body

    def prepare_request(self):
        try:
            path, body = self.validate()
        except Rejected as exc:
            self.reply(exc.status)
            return
        self.validation_timer.cancel()
        trace_context = self.headers.get("X-Vox-Tool-Trace")
        if trace_context:
            try:
                context = json.loads(trace_context, object_pairs_hook=unique_object)
                fields = {
                    "execution_id",
                    "tool_call_id",
                    "run_id",
                    "thread_id",
                    "child_call_id",
                    "tool_attempt",
                }
                if not isinstance(context, dict) or set(context) != fields:
                    raise ValueError("invalid trace context")
                if (
                    type(context["tool_attempt"]) is not int
                    or not 1 <= context["tool_attempt"] <= 10000
                ):
                    raise ValueError("invalid trace attempt")
                if any(
                    value is not None
                    and (
                        not isinstance(value, str)
                        or not re.fullmatch(r"[A-Za-z0-9_.:/-]{1,200}", value)
                    )
                    for key, value in context.items()
                    if key != "tool_attempt"
                ):
                    raise ValueError("invalid trace identity")
            except (ValueError, TypeError):
                self.reply(400)
                return
        return path, body

    def disconnected(self):
        try:
            return bool(
                select.select([self.connection], [], [], 0)[0]
            ) and not self.connection.recv(1, socket.MSG_PEEK | socket.MSG_DONTWAIT)
        except OSError:
            return True

    def do_request(self):
        prepared = self.prepare_request()
        if prepared is None:
            return
        path, body = prepared
        if path in {"/health", "/status"}:
            self.reply(
                200, {"ok": True} if path == "/health" else self.server.gate.read()
            )
            return
        try:
            self.server.gate.acquire(self.disconnected)
        except Cancelled:
            return
        deadline = time.monotonic() + REQUEST_DEADLINE
        stopped = threading.Event()
        current_lock = threading.Lock()
        current = None

        def watch():
            while not stopped.wait(0.01):
                if time.monotonic() >= deadline or self.disconnected():
                    with current_lock:
                        if current is not None:
                            current.abort()
                    return

        watchdog = threading.Thread(target=watch, daemon=True)
        watchdog.start()
        target = self.path
        headers = {
            "Authorization": "Bearer " + self.server.key,
            "Accept": "application/json",
            "Accept-Encoding": "identity",
            "User-Agent": "VoxRelay/1",
            "Connection": "close",
        }
        if body is not None:
            headers["Content-Type"] = "application/json"
        try:
            while time.monotonic() < deadline and not self.disconnected():
                response = upstream = None
                delay = 0.1
                payload = None
                content_type = "application/json"
                status = None
                try:
                    upstream = self.server.transport_factory(
                        deadline - time.monotonic()
                    )
                    with current_lock:
                        current = upstream
                    response = upstream.request(self.command, target, body, headers)
                    status = response.status
                    delay = retry_delay(response.getheader("Retry-After"))
                    if 200 <= status < 300:
                        chunks = []
                        probe = CompletionProbe(path == "/search/text")
                        while time.monotonic() < deadline and not self.disconnected():
                            chunk = response.read1(65536)
                            if not chunk:
                                if response.length not in (None, 0):
                                    raise http.client.IncompleteRead(b"")
                                break
                            chunks.append(chunk)
                            probe.feed(chunk)
                        if probe.finish() == "success":
                            payload = b"".join(chunks)
                            content_type = response.getheader(
                                "Content-Type", "application/json"
                            )
                        else:
                            target = fallback_target(target)
                except (OSError, ValueError, http.client.HTTPException):
                    target = fallback_target(target)
                finally:
                    # The slot is held until this attempt's connection is closed.
                    # No downstream headers or partial frames have been sent.
                    with current_lock:
                        current = None
                        if upstream is not None:
                            upstream.close()
                    if response is not None:
                        response.close()
                if self.disconnected():
                    return
                if time.monotonic() >= deadline:
                    break
                if payload is not None:
                    self.close_connection = True
                    self.send_response(status)
                    self.send_header("Content-Type", content_type)
                    self.send_header("Content-Length", str(len(payload)))
                    self.send_header("Connection", "close")
                    self.end_headers()
                    self.wfile.write(payload)
                    return
                wait_until = min(deadline, time.monotonic() + delay)
                while time.monotonic() < wait_until and not self.disconnected():
                    stopped.wait(min(0.05, wait_until - time.monotonic()))
            if not self.disconnected():
                self.reply(
                    504,
                    {
                        "error": "request_timeout",
                        "message": "Request exceeded 10 seconds. Reduce the date interval and try again.",
                    },
                )
        finally:
            stopped.set()
            watchdog.join()
            self.server.gate.release()
            self.close_connection = True


def main():
    host = os.environ.get("VOX_RELAY_HOST", "127.0.0.1")
    if not ipaddress.ip_address(host).is_private and host != "0.0.0.0":
        raise ValueError("relay requires a private bind address")
    server = RelayServer(
        (host, int(os.environ.get("VOX_RELAY_PORT", "8083"))),
        key_file=os.environ.get("VOX_RELAY_KEY_FILE", "/run/secrets/vox-api-key"),
        state_dir=os.environ.get(
            "VOX_RELAY_STATE_DIR",
            os.environ.get("VOX_RELAY_ADAPTIVE_STATE_DIR", "/var/lib/vox-relay-state"),
        ),
    )
    try:
        server.serve_forever()
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
