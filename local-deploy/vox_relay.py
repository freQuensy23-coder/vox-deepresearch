import fcntl
import hashlib
import http.client
import ipaddress
import json
import math
import os
import re
import select
import socket
import tempfile
import threading
import time
import urllib.parse
import uuid
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


UPSTREAM = "api.vox-lab.com"
DEFAULTS = dict(spacing=0.25, cooldown=60.0, backoff_cap=900.0, queue_wait=120.0,
                request_deadline=700.0, socket_idle=630.0, client_timeout=10.0, max_clients=16,
                max_inflight=5)
MAX_TARGET = 8192
MAX_HEADERS = 16384
MAX_BODY = 16384
INTEGER_BOUNDS = dict(limit=(1, 10000), offset=(0, 10000), timeout=(10, 600),
                      messages_limit=(1, 100), messages_min=(1, 1), members_min=(0, 0),
                      groups_limit=(1, 50), groups_offset=(0, 1000), new_users_limit=(0, 0))
SEARCH_FIELDS = {"query", "limit", "from_date", "to_date", "order", "message_type",
                 "include_groups_ids", "exclude_groups_ids", "exclude_from_users",
                 "exclude_forwards", "deduplicate", "timeout", "stream"}
PREVIEW_FIELDS = {"groups_search_query", "groups_limit", "groups_offset", "groups_regex",
                  "run_parser", "find_new_users", "new_users_limit"}
REQUEST_HEADERS = {"host", "authorization", "accept", "user-agent", "connection",
                   "content-length", "content-type", "accept-encoding", "x-vox-tool-trace"}


class Rejected(Exception):
    def __init__(self, status):
        self.status = status


class Busy(Exception):
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
            if (value is not False if json_body else value != "false"):
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
                high = 1000 if path == "/search/ai/raw/GROUP" else 500 if path.startswith("/user_tracker/") else high
            if not low <= number <= high:
                raise Rejected(400)
        else:
            if not isinstance(value, str) or len(value) > 4096 or any(ord(c) < 32 or ord(c) == 127 for c in value):
                raise Rejected(400)
            if name == "query" and (not value.strip() or (path == "/search/text" and len(value) >= 50)):
                raise Rejected(400)
            choices = dict(order={"asc", "desc"}, message_type={"all", "post", "comment"},
                           stream={"true", "false"}, exclude_forwards={"true", "false"},
                           deduplicate={"true", "false"})
            if name in choices and value not in choices[name]:
                raise Rejected(400)
            if name in {"from_date", "to_date"}:
                try:
                    datetime.fromisoformat(value.replace("Z", "+00:00"))
                except ValueError:
                    raise Rejected(400) from None
            if name in {"include_groups_ids", "exclude_groups_ids", "exclude_from_users"}:
                if not re.fullmatch(r"-?[1-9][0-9]{0,18}(,-?[1-9][0-9]{0,18})*", value):
                    raise Rejected(400)
                if any(abs(int(number)) > 2**63 - 1 for number in value.split(",")):
                    raise Rejected(400)
    if path == "/reporter/parse" and not {"run_parser", "find_new_users", "new_users_limit"} <= set(values):
        raise Rejected(400)


def validate_target(method, target):
    if len(target.encode("utf-8")) > MAX_TARGET:
        raise Rejected(414)
    if not target.startswith("/") or target.startswith("//") or "#" in target or "\\" in target:
        raise Rejected(400)
    parts = urllib.parse.urlsplit(target)
    if parts.scheme or parts.netloc or "%" in parts.path:
        raise Rejected(400)
    if method not in {"GET", "POST"}:
        raise Rejected(405)
    if re.search(r"%(?![0-9A-Fa-f]{2})", parts.query):
        raise Rejected(400)
    try:
        pairs = urllib.parse.parse_qsl(parts.query, keep_blank_values=True, strict_parsing=True,
                                       encoding="utf-8", errors="strict", max_num_fields=32)
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
    elif re.fullmatch(r"/user_tracker/[1-9][0-9]{0,18}/panels/(groups|top_groups|messages)", path):
        allowed = {"limit"}
    else:
        raise Rejected(403)
    validate_fields(values, allowed, path)
    return path, values


class Gate:
    def __init__(self, directory, spacing, cooldown, backoff_cap, max_inflight=5):
        self.directory = Path(directory)
        self.directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        if self.directory.is_symlink():
            raise ValueError("invalid state directory")
        if type(max_inflight) is not int or not 1 <= max_inflight <= 5:
            raise ValueError("invalid inflight limit")
        self.spacing, self.cooldown, self.backoff_cap = spacing, cooldown, backoff_cap
        self.max_inflight = max_inflight
        try:
            fd = os.open(self.directory / "gate.lock", os.O_CREAT | os.O_EXCL | os.O_RDWR | os.O_NOFOLLOW, 0o600)
            new = True
        except FileExistsError:
            fd = os.open(self.directory / "gate.lock", os.O_RDWR | os.O_NOFOLLOW)
            new = False
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            if new and not (self.directory / "state.json").exists():
                self.write(dict(version=2, last_start=0, cooldown_until=0, failures=0, requests=0,
                                gap=self.spacing, success_streak=0))
            state = self.read()
            if state["version"] == 1:
                state.update(version=2, gap=self.spacing, success_streak=0)
                self.write(state)
            for index in range(self.max_inflight):
                slot = os.open(self.directory / f"slot-{index}.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
                os.close(slot)
        finally:
            os.close(fd)

    def read(self):
        with (self.directory / "state.json").open("rb") as handle:
            raw = handle.read(4097)
        if len(raw) > 4096:
            raise ValueError("oversized state")
        state = json.loads(raw, object_pairs_hook=unique_object)
        if not isinstance(state, dict):
            raise ValueError("invalid state")
        if type(state.get("version")) is not int or state["version"] not in {1, 2}:
            raise ValueError("invalid version")
        fields = {"version", "last_start", "cooldown_until", "failures", "requests"}
        if state["version"] == 2:
            fields |= {"gap", "success_streak"}
            if type(state.get("gap")) not in (int, float) or not math.isfinite(state["gap"]) or not 0 < state["gap"] <= 5:
                raise ValueError("invalid gap")
            if type(state.get("success_streak")) is not int or not 0 <= state["success_streak"] <= 50:
                raise ValueError("invalid success streak")
        if set(state) != fields:
            raise ValueError("invalid state")
        for name, maximum in (("failures", 5), ("requests", 2**63 - 1)):
            if type(state[name]) is not int or not 0 <= state[name] <= maximum:
                raise ValueError("invalid counter")
        for name in ("last_start", "cooldown_until"):
            if type(state[name]) not in (int, float) or not math.isfinite(state[name]) or state[name] < 0:
                raise ValueError("invalid timestamp")
        return state

    def write(self, state):
        temporary = None
        try:
            with tempfile.NamedTemporaryFile(mode="w", dir=self.directory, delete=False, prefix=".state-") as handle:
                temporary = Path(handle.name)
                json.dump(state, handle, allow_nan=False, separators=(",", ":"))
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, self.directory / "state.json")
            temporary = None
            fd = os.open(self.directory, os.O_DIRECTORY | os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)

    def acquire(self, wait):
        deadline = time.monotonic() + wait
        while True:
            fd = os.open(self.directory / "gate.lock", os.O_RDWR | os.O_NOFOLLOW)
            slot = None
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                state = self.read()
                delay = max(state["last_start"] + state["gap"], state["cooldown_until"]) - time.time()
                if delay <= 0:
                    for index in range(self.max_inflight):
                        candidate = os.open(self.directory / f"slot-{index}.lock", os.O_RDWR | os.O_NOFOLLOW)
                        try:
                            fcntl.flock(candidate, fcntl.LOCK_EX | fcntl.LOCK_NB)
                        except BlockingIOError:
                            os.close(candidate)
                            continue
                        slot = candidate
                        break
                    if slot is not None:
                        state["last_start"] = time.time()
                        state["requests"] = min(state["requests"] + 1, 2**63 - 1)
                        self.write(state)
                        result, slot = slot, None
                        return result
            except BlockingIOError:
                pass
            finally:
                if slot is not None:
                    os.close(slot)
                os.close(fd)
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise Busy()
            time.sleep(min(remaining, 0.02))

    def finish(self, failed, retry_after=None, *, rate_limited=False, successful=True):
        fd = os.open(self.directory / "gate.lock", os.O_RDWR | os.O_NOFOLLOW)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            state = self.read()
            now = time.time()
            if rate_limited:
                state["gap"] = min(state["gap"] * 2, 5.0)
                state["success_streak"] = 0
                delay = 2.0
                if retry_after is not None:
                    try:
                        value = float(retry_after.strip())
                        if not math.isfinite(value) or value < 0:
                            raise ValueError("invalid retry delay")
                        delay = value
                    except (ValueError, TypeError, OverflowError):
                        try:
                            parsed = parsedate_to_datetime(retry_after.strip())
                            delay = max(0, parsed.replace(tzinfo=parsed.tzinfo or timezone.utc).timestamp() - now)
                        except (TypeError, ValueError, OverflowError):
                            pass
                state["cooldown_until"] = max(state["cooldown_until"], now + delay)
            elif failed:
                state["success_streak"] = 0
                state["failures"] = min(state["failures"] + 1, 5)
                deadline = now + min(self.cooldown * 2 ** (state["failures"] - 1), self.backoff_cap)
                if retry_after is not None:
                    value = retry_after.strip()
                    if re.fullmatch(r"[0-9]{1,10}", value):
                        deadline = max(deadline, now + int(value))
                    else:
                        try:
                            parsed = parsedate_to_datetime(value)
                            deadline = max(deadline, parsed.replace(tzinfo=parsed.tzinfo or timezone.utc).timestamp())
                        except (TypeError, ValueError, OverflowError):
                            pass
                state["cooldown_until"] = max(state["cooldown_until"], deadline)
            else:
                state["failures"] = 0
                if state["cooldown_until"] <= now:
                    state["cooldown_until"] = 0
                state["success_streak"] = min(state["success_streak"] + 1, 50) if successful else 0
                if state["success_streak"] >= 50 and state["gap"] > self.spacing:
                    state["gap"] = max(state["gap"] * 0.8, self.spacing)
                    state["success_streak"] = 0
            self.write(state)
        finally:
            os.close(fd)

    def release(self, fd):
        os.close(fd)

    def retry_after(self):
        state = self.read()
        return max(1, math.ceil(max(state["last_start"] + state["gap"], state["cooldown_until"]) - time.time()))


class ErrorProbe:
    def __init__(self):
        self.prefix = b""
        self.pending = b""
        self.skipping = False
        self.failed = False

    def inspect(self, raw):
        try:
            frame = json.loads(raw)
        except (ValueError, UnicodeError, RecursionError):
            return
        if isinstance(frame, dict):
            self.failed |= bool(frame.get("error")) or frame.get("ok") is False or frame.get("status") in ("error", "failed")
            self.failed |= bool(frame.get("detail")) and set(frame) <= {"detail", "status", "status_code", "code"}

    def feed(self, chunk):
        self.prefix += chunk[:max(0, 65536 - len(self.prefix))]
        parts = chunk.split(b"\n")
        for index, part in enumerate(parts):
            if not self.skipping:
                if len(self.pending) + len(part) <= 65536:
                    self.pending += part
                else:
                    self.pending = b""
                    self.skipping = True
            if index < len(parts) - 1:
                if not self.skipping:
                    self.inspect(self.pending)
                self.pending = b""
                self.skipping = False

    def finish(self):
        self.inspect(self.prefix)
        if not self.skipping:
            self.inspect(self.pending)
        return self.failed


class HTTPSUpstream:
    def __init__(self, timeout):
        self.connection = http.client.HTTPSConnection(UPSTREAM, 443, timeout=timeout)

    def request(self, method, target, body, headers):
        self.connection.connect()
        self.socket = self.connection.sock
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


class RequestObservation:
    lock = threading.Lock()

    def __init__(self, gate):
        self.gate = gate
        self.request_id = str(uuid.uuid4())
        self.started = time.monotonic()
        self.queued = self.admitted = None
        self.route_class = "unknown"
        self.status = None
        self.origin = "local_queue"
        self.tool_correlation = None
        self.body_stream = None
        self.body_hash = hashlib.sha256()
        self.body_bytes = 0
        self.trace_gaps = []
        self.request_payload = None

    def capture(self, chunk):
        directory = self.gate.directory / "traces"
        directory.mkdir(mode=0o700, exist_ok=True)
        if self.body_stream is None:
            fd = os.open(directory / (self.request_id + ".body"), os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
            self.body_stream = os.fdopen(fd, "wb")
        self.body_stream.write(chunk)
        self.body_stream.flush()
        os.fsync(self.body_stream.fileno())
        self.body_hash.update(chunk)
        self.body_bytes += len(chunk)

    def route(self, path):
        routes = {"/search/text": "search_text", "/search/ai/raw/GROUP": "search_group",
                  "/reporter/parse": "reporter_preview", "/health": "health", "/status": "status"}
        self.route_class = routes.get(path, "unknown")
        for pattern, name in ((r"/groups/-[0-9]+", "group"),
                              (r"/groups/-[0-9]+/messages", "group_messages"),
                              (r"/ai_analytics/GROUP/-[0-9]+", "group_analytics"),
                              (r"/ai_analytics/custom/GROUP/-[0-9]+", "group_custom_analytics"),
                              (r"/user_tracker/[0-9]+/panels/groups", "user_groups"),
                              (r"/user_tracker/[0-9]+/panels/top_groups", "user_top_groups"),
                              (r"/user_tracker/[0-9]+/panels/messages", "user_messages")):
            if re.fullmatch(pattern, path):
                self.route_class = name
                break

    def emit(self, event):
        try:
            now = time.monotonic()
            if event == "queue_entry":
                self.queued = now
            elif event == "admitted":
                self.admitted = now
            row = dict(event=event, request_id=self.request_id, route_class=self.route_class,
                       timestamp=time.time(), status=self.status, origin=self.origin,
                       queued_elapsed=0 if self.queued is None else (self.admitted or now) - self.queued,
                       upstream_elapsed=0 if self.admitted is None else now - self.admitted,
                       total_elapsed=now - self.started)
            if event == "end":
                try:
                    state = self.gate.read()
                    row.update(failures=state["failures"], cooldown_until=state["cooldown_until"])
                except Exception:
                    pass
            with self.lock:
                if self.route_class not in {"health", "status"}:
                    directory = self.gate.directory / "traces"
                    directory.mkdir(mode=0o700, exist_ok=True)
                    durable = {**row, "tool_correlation": self.tool_correlation,
                               "request": self.request_payload,
                               "correlation_trust": "caller_supplied_not_authorization",
                               "upstream_attempt": int(self.admitted is not None),
                               "utc": datetime.now(timezone.utc).isoformat(), "monotonic_ns": time.monotonic_ns(),
                               "trace_gaps": self.trace_gaps,
                               "body_bytes": self.body_bytes, "body_sha256": self.body_hash.hexdigest(),
                               "body_path": self.request_id + ".body" if self.body_stream is not None else None}
                    fd = os.open(directory / (self.request_id + "-" + event + ".json"), os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
                    with os.fdopen(fd, "w") as stream:
                        json.dump(durable, stream, separators=(",", ":"))
                        stream.flush()
                        os.fsync(stream.fileno())
                    fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
                    try:
                        os.fsync(fd)
                    finally:
                        os.close(fd)
                try:
                    print(json.dumps(row, allow_nan=False, separators=(",", ":")), flush=True)
                except Exception:
                    pass
        except Exception:
            self.trace_gaps.append(event)
            try:
                print(json.dumps({"event": "trace_gap", "request_id": self.request_id, "missing_event": event}), flush=True)
            except Exception:
                pass
        finally:
            if event == "end" and self.body_stream is not None:
                self.body_stream.close()


class RelayServer(ThreadingHTTPServer):
    daemon_threads = False
    block_on_close = True
    request_queue_size = 16

    def __init__(self, address, *, key_file, state_dir, transport_factory=HTTPSUpstream, **options):
        self.options = DEFAULTS | options
        if set(self.options) != set(DEFAULTS):
            raise ValueError("unknown option")
        if any(type(v) not in (int, float) or not math.isfinite(v) or v <= 0 for v in self.options.values()):
            raise ValueError("invalid limit")
        if type(self.options["max_clients"]) is not int or self.options["max_clients"] > 16:
            raise ValueError("invalid client limit")
        with Path(key_file).open("rb") as handle:
            key = handle.read(4097).strip()
        if not key or len(key) > 4096 or any(c <= 32 or c >= 127 for c in key):
            raise ValueError("invalid relay key")
        self.key = key.decode("ascii")
        self.state_dir = Path(state_dir)
        self.gate = Gate(self.state_dir, self.options["spacing"], self.options["cooldown"], self.options["backoff_cap"],
                         self.options["max_inflight"])
        self.transport_factory = transport_factory
        self.clients = threading.BoundedSemaphore(self.options["max_clients"])
        self.faulted = False
        self.observations = threading.local()
        super().__init__(address, RelayHandler)

    def process_request(self, request, client_address):
        if not self.clients.acquire(blocking=False):
            observation = RequestObservation(self.gate)
            observation.status, observation.origin = 503, "client_capacity"
            try:
                request.settimeout(0.1)
                request.sendall(b"HTTP/1.1 503 Service Unavailable\r\nConnection: close\r\nRetry-After: 1\r\nContent-Length: 0\r\n\r\n")
            except OSError:
                pass
            self.shutdown_request(request)
            observation.emit("end")
            return
        try:
            super().process_request(request, client_address)
        except BaseException:
            self.clients.release()
            raise

    def process_request_thread(self, request, client_address):
        self.observations.current = RequestObservation(self.gate)
        try:
            super().process_request_thread(request, client_address)
        finally:
            self.observations.current.emit("end")
            del self.observations.current
            self.clients.release()

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

    def send_response(self, code, message=None):
        self.server.observations.current.status = code
        super().send_response(code, message)

    def reply(self, status, payload=None, retry_after=None):
        body = json.dumps(payload if payload is not None else {"status": status}, separators=(",", ":")).encode()
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
        self.validation_timer = threading.Timer(self.server.options["client_timeout"], self.abort_client)
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
        if any(any(ord(c) < 32 or ord(c) == 127 for c in value) for name, value in headers):
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
                if values or self.headers.get("Content-Type", "").split(";")[0].lower() != "application/json":
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
        observation = self.server.observations.current
        observation.route(path)
        observation.request_payload = {"method": self.command, "target": self.path, "body": json.loads(body) if body else None}
        trace_context = self.headers.get("X-Vox-Tool-Trace")
        if trace_context:
            try:
                context = json.loads(trace_context, object_pairs_hook=unique_object)
                fields = {"execution_id", "tool_call_id", "run_id", "thread_id", "child_call_id", "tool_attempt"}
                if not isinstance(context, dict) or set(context) != fields:
                    raise ValueError("invalid trace context")
                if type(context["tool_attempt"]) is not int or not 1 <= context["tool_attempt"] <= 10000:
                    raise ValueError("invalid trace attempt")
                if any(value is not None and (not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_.:/-]{1,200}", value)) for key, value in context.items() if key != "tool_attempt"):
                    raise ValueError("invalid trace identity")
                observation.tool_correlation = context
            except (ValueError, TypeError):
                self.reply(400)
                return
        return path, body

    def do_request(self):
        prepared = self.prepare_request()
        if prepared is None:
            return
        path, body = prepared
        observation = self.server.observations.current
        self.connection.settimeout(self.server.options["socket_idle"])
        if path in {"/health", "/status"}:
            try:
                state = self.server.gate.read()
                self.reply(503 if self.server.faulted else 200,
                           {"ok": not self.server.faulted} if path == "/health" else state)
            except (OSError, ValueError):
                self.reply(503)
            return
        if self.server.faulted:
            self.reply(503, retry_after=60)
            return
        observation.emit("queue_entry")
        try:
            fd = self.server.gate.acquire(self.server.options["queue_wait"])
        except (Busy, OSError, ValueError, OverflowError):
            try:
                delay = self.server.gate.retry_after()
            except (OSError, ValueError, OverflowError):
                delay = 60
            self.reply(503, retry_after=delay)
            return
        observation.origin = "upstream"
        observation.emit("admitted")
        upstream = response = watchdog = None
        stopped, aborted = threading.Event(), threading.Event()
        headers_sent = False
        recorded = False
        try:
            upstream = self.server.transport_factory(min(self.server.options["socket_idle"], self.server.options["request_deadline"]))
            deadline = time.monotonic() + self.server.options["request_deadline"]

            def watch():
                while not stopped.wait(0.02):
                    disconnected = False
                    try:
                        if select.select([self.connection], [], [], 0)[0]:
                            disconnected = not self.connection.recv(1, socket.MSG_PEEK | socket.MSG_DONTWAIT)
                    except OSError:
                        disconnected = True
                    if disconnected or time.monotonic() >= deadline:
                        aborted.set()
                        upstream.abort()
                        self.abort_client()
                        return

            watchdog = threading.Thread(target=watch)
            watchdog.start()
            headers = {"Authorization": "Bearer " + self.server.key, "Accept": "application/json",
                       "Accept-Encoding": "identity", "User-Agent": "VoxRelay/1", "Connection": "close"}
            if body is not None:
                headers["Content-Type"] = "application/json"
            response = upstream.request(self.command, self.path, body, headers)
            status = response.status
            observation.status = status
            observation.emit("upstream_headers")
            if 300 <= status < 400:
                raise OSError("redirect refused")
            failed = status in {429, 502, 503, 504, 524}
            if failed:
                self.server.gate.finish(True, response.getheader("Retry-After"), rate_limited=status == 429)
                recorded = True
            self.send_response(status)
            self.send_header("Connection", "close")
            self.send_header("Content-Type", response.getheader("Content-Type", "application/json"))
            if failed:
                self.send_header("Retry-After", str(self.server.gate.retry_after()))
            self.end_headers()
            headers_sent = True
            probe = ErrorProbe()
            while True:
                chunk = response.read1(65536)
                if not chunk:
                    if response.length not in (None, 0):
                        raise http.client.IncompleteRead(b"")
                    break
                probe.feed(chunk)
                try:
                    observation.capture(chunk)
                except OSError:
                    observation.trace_gaps.append("raw_body")
                if probe.failed and not recorded:
                    self.server.gate.finish(True)
                    recorded = True
                self.wfile.write(chunk)
                self.wfile.flush()
            if aborted.is_set() or time.monotonic() >= deadline:
                raise TimeoutError("request aborted")
            if not recorded:
                self.server.gate.finish(probe.finish(), successful=200 <= status < 300)
                recorded = True
        except (OSError, ValueError, http.client.HTTPException):
            observation.origin = "transport_error"
            if not recorded:
                try:
                    self.server.gate.finish(True)
                    recorded = True
                except (OSError, ValueError, OverflowError):
                    self.server.faulted = True
            if not headers_sent:
                try:
                    self.reply(502, retry_after=60)
                except OSError:
                    pass
        finally:
            stopped.set()
            if watchdog is not None:
                watchdog.join()
            if upstream is not None:
                upstream.close()
            if response is not None:
                response.close()
            self.server.gate.release(fd)
            self.close_connection = True


def main():
    host = os.environ.get("VOX_RELAY_HOST", "127.0.0.1")
    if not ipaddress.ip_address(host).is_private and host != "0.0.0.0":
        raise ValueError("relay requires a private bind address")
    server = RelayServer((host, int(os.environ.get("VOX_RELAY_PORT", "8083"))),
                         key_file=os.environ.get("VOX_RELAY_KEY_FILE", "/run/secrets/vox-api-key"),
                         state_dir=os.environ.get("VOX_RELAY_STATE_DIR", "/var/lib/vox-relay-state"))
    try:
        server.serve_forever()
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
