import fcntl
import importlib.util
import http.client
import ipaddress
import select
import socket
import threading
import json
import math
import os
import re
import sys
import time
from email.utils import parsedate_to_datetime
from datetime import timezone
from pathlib import Path


if "_vox_recovery_legacy" not in sys.modules:
    spec = importlib.util.spec_from_file_location("_vox_recovery_legacy", Path(__file__).with_name("vox_relay.py"))
    legacy = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(legacy)
    sys.modules["_vox_recovery_legacy"] = legacy
legacy = sys.modules["_vox_recovery_legacy"]
POLICY = "staged-generation-only-v1"


def retry_after_metadata(raw, now):
    value = raw if isinstance(raw, str) else None
    deadline = None
    if value is not None and len(value) <= 256:
        value = value.strip()
        try:
            delay = float(value)
            if not math.isfinite(delay) or delay < 0:
                raise ValueError("invalid delay")
            deadline = now + delay
        except (ValueError, OverflowError):
            try:
                parsed = parsedate_to_datetime(value)
                deadline = max(now, parsed.replace(tzinfo=parsed.tzinfo or timezone.utc).timestamp())
            except (ValueError, TypeError, OverflowError):
                pass
        if deadline is not None and (not math.isfinite(deadline) or deadline < 0):
            deadline = None
    return dict(retry_after_raw=None if raw is None else str(raw)[:256],
                retry_after_truncated=isinstance(raw, str) and len(raw) > 256,
                retry_after_valid=deadline is not None, retry_after_until=deadline,
                retry_after_observed_at=now)


class GenerationGate(legacy.Gate):
    def __init__(self, directory, spacing, cooldown, backoff_cap, max_inflight=5):
        if type(max_inflight) is not int or not 1 <= max_inflight <= 5:
            raise ValueError("invalid inflight limit")
        self.directory = Path(directory)
        self.directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        if self.directory.is_symlink():
            raise ValueError("invalid state directory")
        self.spacing, self.cooldown, self.backoff_cap = spacing, cooldown, backoff_cap
        self.max_inflight = max_inflight
        self.owners = {}
        try:
            fd = os.open(self.directory / "gate.lock", os.O_CREAT | os.O_EXCL | os.O_RDWR | os.O_NOFOLLOW, 0o600)
            new = True
        except FileExistsError:
            fd = os.open(self.directory / "gate.lock", os.O_RDWR | os.O_NOFOLLOW)
            new = False
        slots = []
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            if new and not (self.directory / "state.json").exists():
                state = dict(version=2, last_start=0, cooldown_until=0, failures=0, requests=0,
                             gap=spacing, success_streak=0)
            else:
                state = self.read(migrating=True)
            if state["version"] != 3:
                for index in range(5):
                    slot = os.open(self.directory / f"slot-{index}.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
                    slots.append(slot)
                    fcntl.flock(slot, fcntl.LOCK_EX | fcntl.LOCK_NB)
                state.update(version=3, gap=state.get("gap", spacing), success_streak=0,
                             generation=0, fault_revision=0, hard_until=state["cooldown_until"],
                             soft_until=state["cooldown_until"], slot_count=max_inflight)
                self.write(state)
        finally:
            for slot in slots:
                os.close(slot)
            os.close(fd)

    def read(self, migrating=False):
        with (self.directory / "state.json").open("rb") as handle:
            raw = handle.read(4097)
        if len(raw) > 4096:
            raise ValueError("oversized state")
        state = json.loads(raw, object_pairs_hook=legacy.unique_object)
        if not isinstance(state, dict) or type(state.get("version")) is not int:
            raise ValueError("invalid state")
        if migrating and state["version"] in (1, 2):
            return legacy.Gate.read(self)
        fields = {"version", "last_start", "cooldown_until", "failures", "requests", "gap",
                  "success_streak", "generation", "fault_revision", "hard_until", "soft_until", "slot_count"}
        if state["version"] != 3 or set(state) != fields:
            raise ValueError("invalid candidate state")
        for name, maximum in (("failures", 5), ("requests", 2**63 - 1), ("generation", 2**63 - 1),
                              ("fault_revision", 2**63 - 1), ("success_streak", 50), ("slot_count", 5)):
            if type(state[name]) is not int or not 0 <= state[name] <= maximum:
                raise ValueError("invalid counter")
        if state["slot_count"] != self.max_inflight:
            raise ValueError("slot count changed")
        for name in ("last_start", "cooldown_until", "hard_until", "soft_until", "gap"):
            if type(state[name]) not in (int, float) or not math.isfinite(state[name]) or state[name] < 0:
                raise ValueError("invalid timestamp")
        if not 0 < state["gap"] <= 5 or state["cooldown_until"] != max(state["hard_until"], state["soft_until"]):
            raise ValueError("invalid deadline or gap")
        return state

    def acquire(self, wait):
        deadline = time.monotonic() + wait
        while True:
            fd = os.open(self.directory / "gate.lock", os.O_RDWR | os.O_NOFOLLOW)
            slot = None
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                state = self.read()
                if max(state["last_start"] + state["gap"], state["cooldown_until"]) <= time.time():
                    for index in range(self.max_inflight):
                        slot = os.open(self.directory / f"slot-{index}.lock", os.O_RDWR | os.O_NOFOLLOW)
                        try:
                            fcntl.flock(slot, fcntl.LOCK_EX | fcntl.LOCK_NB)
                        except BlockingIOError:
                            os.close(slot)
                            slot = None
                            continue
                        state["last_start"] = time.time()
                        if state["requests"] == 2**63 - 1:
                            raise ValueError("request counter exhausted")
                        state["requests"] += 1
                        self.write(state)
                        self.owners[slot] = dict(generation=state["generation"], fault_revision=state["fault_revision"],
                                                request=state["requests"], recorded=False)
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
                raise legacy.Busy()
            time.sleep(min(remaining, .02))

    def observe_retry_after(self, owner, timing):
        if not timing["retry_after_valid"]:
            return
        fd = os.open(self.directory / "gate.lock", os.O_RDWR | os.O_NOFOLLOW)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            if owner not in self.owners:
                raise ValueError("unknown admission owner")
            state = self.read()
            state["hard_until"] = max(state["hard_until"], timing["retry_after_until"])
            state["cooldown_until"] = max(state["soft_until"], state["hard_until"])
            self.write(state)
        finally:
            os.close(fd)

    def complete(self, owner, *, failed=False, validated=False, cancelled=False, rate_limited=False,
                 retry_after=None, retry_metadata=None):
        fd = os.open(self.directory / "gate.lock", os.O_RDWR | os.O_NOFOLLOW)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            token = self.owners[owner]
            state = self.read()
            before = dict(state)
            if token["recorded"]:
                return dict(before=before, after=before)
            now = time.time()
            if cancelled:
                token["recorded"] = True
                return dict(before=before, after=before)
            timing = retry_metadata if retry_metadata is not None else retry_after_metadata(retry_after, now)
            if timing["retry_after_valid"]:
                state["hard_until"] = max(state["hard_until"], timing["retry_after_until"])
            if failed:
                if state["fault_revision"] == 2**63 - 1:
                    raise ValueError("fault revision exhausted")
                state["fault_revision"] += 1
                state["success_streak"] = 0
                fresh = token["generation"] == state["generation"]
                if fresh:
                    if state["generation"] == 2**63 - 1:
                        raise ValueError("generation counter exhausted")
                    state["generation"] += 1
                if rate_limited:
                    state["gap"] = min(state["gap"] * 2, 5.0)
                    if not timing["retry_after_valid"]:
                        state["soft_until"] = max(state["soft_until"], now + 2)
                else:
                    if fresh or not state["failures"]:
                        state["failures"] = min(state["failures"] + 1, 5)
                    delay = min(self.cooldown * 2 ** (state["failures"] - 1), self.backoff_cap)
                    state["soft_until"] = max(state["soft_until"], now + delay)
            elif validated and token["generation"] == state["generation"] and token["fault_revision"] == state["fault_revision"]:
                state["failures"] = 0
                state["success_streak"] = min(state["success_streak"] + 1, 50)
                if state["success_streak"] >= 50 and state["gap"] > self.spacing:
                    state["gap"] = max(state["gap"] * .8, self.spacing)
                    state["success_streak"] = 0
            state["cooldown_until"] = max(state["soft_until"], state["hard_until"])
            if state != before:
                self.write(state)
            token["recorded"] = True
            return dict(before=before, after=state)
        finally:
            os.close(fd)

    def finish(self, *args, **kwargs):
        raise ValueError("candidate completion requires admission ownership")

    def release(self, fd):
        self.owners.pop(fd)
        super().release(fd)


def reject_json_constant(value):
    raise ValueError("invalid JSON constant")


class CompletionProbe:
    def __init__(self, route_class):
        self.route_class = route_class
        self.pending = b""
        self.overflow = False
        self.invalid = False
        self.terminal = False
        self.finished = False
        self.error = False
        self.partial = False
        self.frames = 0

    def inspect(self, raw):
        if not raw.strip():
            return
        if self.terminal:
            self.invalid = True
        try:
            frame = json.loads(raw, object_pairs_hook=legacy.unique_object, parse_constant=reject_json_constant)
        except (ValueError, UnicodeError, RecursionError):
            self.invalid = True
            return
        self.frames += 1
        if not isinstance(frame, dict):
            self.invalid = True
            return
        self.error |= bool(frame.get("error")) or frame.get("ok") is False or frame.get("status") in ("error", "failed")
        self.error |= bool(frame.get("detail")) and set(frame) <= {"detail", "status", "status_code", "code"}
        self.partial |= bool(frame.get("partial")) or bool(frame.get("truncated")) or frame.get("status") in ("partial", "incomplete")
        if self.route_class == "search_text" and isinstance(frame.get("messages"), list):
            self.terminal = True
            self.finished = frame.get("finished") is True
        elif "point" not in frame:
            self.invalid = True

    def feed(self, chunk):
        parts = chunk.split(b"\n")
        for index, part in enumerate(parts):
            if len(self.pending) + len(part) > 2**20:
                self.overflow = True
                self.invalid = True
                self.pending = b""
            if not self.overflow:
                self.pending += part
            if index < len(parts) - 1:
                if not self.overflow:
                    self.inspect(self.pending)
                self.pending = b""

    def finish(self):
        if self.pending and not self.overflow:
            self.inspect(self.pending)
            self.pending = b""
        return dict(terminal_present=self.terminal, finished=self.finished, semantic_error=self.error,
                    semantic_complete=self.terminal and self.finished and not self.error and not self.invalid and not self.partial,
                    semantic_partial=self.partial,
                    semantic_unvalidated=self.invalid or not self.terminal, semantic_oversized=self.overflow)


class CandidateObservation(legacy.RequestObservation):
    def __init__(self, gate):
        super().__init__(gate)
        self.recovery = dict(current_policy=POLICY, admission_generation=None, failure_generation=None,
                             recovery_owner=None, deadline_reason="none", termination_cause="not_admitted",
                             transport_eof=False, upstream_closed=False, downstream_cancelled=False,
                             recovery_validated=False, exception_class=None, state_before=None, state_after=None)
        self.recovery.update(retry_after_metadata(None, time.time()))

    def emit(self, event):
        try:
            now = time.monotonic()
            if event == "queue_entry":
                self.queued = now
            elif event == "admitted":
                self.admitted = now
            state = self.gate.read()
            self.recovery["failure_generation"] = state["generation"]
            if state["cooldown_until"] > time.time():
                self.recovery["deadline_reason"] = "retry_after" if state["hard_until"] >= state["soft_until"] else "fallback"
            else:
                self.recovery["deadline_reason"] = "none"
            row = dict(event=event, request_id=self.request_id, route_class=self.route_class,
                       timestamp=time.time(), status=self.status, origin=self.origin,
                       queued_elapsed=0 if self.queued is None else (self.admitted or now) - self.queued,
                       upstream_elapsed=0 if self.admitted is None else now - self.admitted,
                       total_elapsed=now-self.started, failures=state["failures"], cooldown_until=state["cooldown_until"],
                       recovery=self.recovery, trace_gaps=self.trace_gaps)
            directory = self.gate.directory / "traces"
            directory.mkdir(mode=0o700, exist_ok=True)
            fd = os.open(directory / (self.request_id + "-" + event + ".json"), os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
            with os.fdopen(fd, "w") as stream:
                json.dump(row, stream, allow_nan=False, separators=(",", ":"))
                stream.flush()
                os.fsync(stream.fileno())
            fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
            with self.lock:
                print(json.dumps(row, allow_nan=False, separators=(",", ":")), flush=True)
        except Exception:
            self.trace_gaps.append(event)


class CandidateRelayServer(legacy.RelayServer):
    def __init__(self, address, *, key_file, state_dir, transport_factory=legacy.HTTPSUpstream, **options):
        self.options = legacy.DEFAULTS | options
        if set(self.options) != set(legacy.DEFAULTS):
            raise ValueError("unknown option")
        if any(type(v) not in (int, float) or not math.isfinite(v) or v <= 0 for v in self.options.values()):
            raise ValueError("invalid limit")
        if type(self.options["max_clients"]) is not int or self.options["max_clients"] > 16:
            raise ValueError("invalid client limit")
        with Path(key_file).open("rb") as stream:
            key = stream.read(4097).strip()
        if not key or len(key) > 4096 or any(c <= 32 or c >= 127 for c in key):
            raise ValueError("invalid relay key")
        self.key = key.decode("ascii")
        self.state_dir = Path(state_dir)
        self.gate = GenerationGate(self.state_dir, self.options["spacing"], self.options["cooldown"],
                                   self.options["backoff_cap"], self.options["max_inflight"])
        self.transport_factory = transport_factory
        self.clients = threading.BoundedSemaphore(self.options["max_clients"])
        self.faulted = False
        self.observations = threading.local()
        legacy.ThreadingHTTPServer.__init__(self, address, CandidateHandler)

    def process_request_thread(self, request, client_address):
        self.observations.current = CandidateObservation(self.gate)
        try:
            legacy.ThreadingHTTPServer.process_request_thread(self, request, client_address)
        finally:
            self.observations.current.emit("end")
            del self.observations.current
            self.clients.release()


class CandidateHandler(legacy.RelayHandler):
    def do_request(self):
        observation = self.server.observations.current
        try:
            path, body = self.validate()
        except legacy.Rejected as exc:
            self.reply(exc.status)
            return
        self.validation_timer.cancel()
        observation.route(path)
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
            owner = self.server.gate.acquire(self.server.options["queue_wait"])
        except (legacy.Busy, OSError, ValueError, OverflowError):
            observation.recovery["termination_cause"] = "local_queue"
            try:
                delay = self.server.gate.retry_after()
            except (OSError, ValueError, OverflowError):
                delay = 60
            self.reply(503, retry_after=delay)
            return
        observation.origin = "upstream"
        observation.recovery["admission_generation"] = self.server.gate.owners[owner]["generation"]
        observation.emit("admitted")
        upstream = response = watchdog = None
        stopped, aborted = threading.Event(), threading.Event()
        headers_sent = recorded = failed = False
        phase = "upstream"
        status = 0
        probe = CompletionProbe(observation.route_class)
        deadline = time.monotonic() + self.server.options["request_deadline"]

        def record(**kwargs):
            transition = self.server.gate.complete(owner, **kwargs)
            observation.recovery.update(state_before=transition["before"], state_after=transition["after"])

        try:
            upstream = self.server.transport_factory(min(self.server.options["socket_idle"], self.server.options["request_deadline"]))

            def watch():
                while not stopped.wait(.02):
                    cause = None
                    try:
                        if select.select([self.connection], [], [], 0)[0]:
                            if not self.connection.recv(1, socket.MSG_PEEK | socket.MSG_DONTWAIT):
                                cause = "downstream_disconnect"
                    except ConnectionResetError:
                        cause = "downstream_disconnect"
                    except OSError:
                        cause = "upstream_or_unknown_transport"
                    if cause is None and time.monotonic() >= deadline:
                        cause = "relay_deadline"
                    if cause is not None:
                        observation.recovery["termination_cause"] = cause
                        observation.recovery["downstream_cancelled"] = cause == "downstream_disconnect"
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
            timing = retry_after_metadata(response.getheader("Retry-After"), time.time())
            observation.recovery.update(timing)
            self.server.gate.observe_retry_after(owner, timing)
            observation.emit("upstream_headers")
            if 300 <= status < 400:
                raise OSError("redirect refused")
            failed = status in {429, 502, 503, 504, 524}
            if failed:
                observation.recovery["termination_cause"] = "upstream_http_error"
                record(failed=True, rate_limited=status == 429, retry_metadata=timing)
                recorded = True
            phase = "downstream"
            self.send_response(status)
            self.send_header("Connection", "close")
            self.send_header("Content-Type", response.getheader("Content-Type", "application/json"))
            if failed:
                self.send_header("Retry-After", str(self.server.gate.retry_after()))
            self.end_headers()
            headers_sent = True
            while True:
                phase = "upstream"
                chunk = response.read1(65536)
                if not chunk:
                    if response.length not in (None, 0):
                        raise http.client.IncompleteRead(b"")
                    observation.recovery["transport_eof"] = True
                    break
                probe.feed(chunk)
                if probe.error and not recorded:
                    observation.recovery["termination_cause"] = "semantic_error"
                    record(failed=True, retry_metadata=timing)
                    recorded = failed = True
                phase = "downstream"
                self.wfile.write(chunk)
                self.wfile.flush()
            if aborted.is_set() or time.monotonic() >= deadline:
                if not aborted.is_set():
                    observation.recovery["termination_cause"] = "relay_deadline"
                raise TimeoutError("request aborted")
            if not recorded:
                observation.recovery["termination_cause"] = "transport_complete"
        except (OSError, ValueError, http.client.HTTPException) as exc:
            failed = True
            observation.origin = "transport_error"
            observation.recovery["exception_class"] = type(exc).__name__
            if not aborted.is_set():
                cancelled = phase == "downstream" and isinstance(exc, (BrokenPipeError, ConnectionResetError))
                observation.recovery["downstream_cancelled"] = cancelled
                observation.recovery["termination_cause"] = "downstream_disconnect" if cancelled else "upstream_or_unknown_transport"
            if not headers_sent:
                try:
                    self.reply(502, retry_after=60)
                except OSError:
                    pass
        finally:
            stopped.set()
            if watchdog is not None:
                watchdog.join()
            close_failed = False
            for resource in (upstream, response):
                if resource is not None:
                    try:
                        resource.close()
                    except Exception:
                        close_failed = True
            observation.recovery["upstream_closed"] = not close_failed
            semantics = probe.finish()
            observation.recovery.update(semantics)
            valid = (not failed and not close_failed and not observation.recovery["downstream_cancelled"]
                     and observation.recovery["transport_eof"] and 200 <= status < 300 and semantics["semantic_complete"])
            observation.recovery["recovery_validated"] = valid
            try:
                if not recorded:
                    record(failed=failed or close_failed or semantics["semantic_error"], validated=valid,
                           cancelled=observation.recovery["downstream_cancelled"] and not close_failed,
                           retry_metadata={key: observation.recovery[key] for key in retry_after_metadata(None, 0)})
            except (OSError, ValueError, OverflowError):
                self.server.faulted = True
            if close_failed:
                self.server.faulted = True
                observation.recovery["termination_cause"] = "upstream_close_failed"
            else:
                self.server.gate.release(owner)
            self.close_connection = True


def main():
    enabled = os.environ.get("VOX_RELAY_RECOVERY_CANDIDATE", "0")
    if enabled == "0":
        return legacy.main()
    if enabled != "1":
        raise ValueError("invalid recovery candidate opt-in")
    host = os.environ.get("VOX_RELAY_HOST", "127.0.0.1")
    if not ipaddress.ip_address(host).is_private and host != "0.0.0.0":
        raise ValueError("relay requires a private bind address")
    server = CandidateRelayServer((host, int(os.environ.get("VOX_RELAY_PORT", "8083"))),
                                  key_file=os.environ.get("VOX_RELAY_KEY_FILE", "/run/secrets/vox-api-key"),
                                  state_dir=os.environ.get("VOX_RELAY_STATE_DIR", "/var/lib/vox-relay-state"))
    try:
        server.serve_forever()
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
