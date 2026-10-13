"""Opt-in adaptive Vox relay. Never changes or migrates the baseline relay state."""

import http.client
import ipaddress
import math
import os
import select
import socket
import threading
import time
import urllib.parse
from datetime import datetime
from http.server import ThreadingHTTPServer
from pathlib import Path

from adaptive_gate import AdaptiveGate, Busy, Cancelled
from vox_relay import HTTPSUpstream, RelayHandler, RelayServer
from vox_json_probe import CompletionProbe


DEFAULTS = dict(
    queue_wait=None,
    request_deadline=2100.0,
    socket_idle=1890.0,
    client_timeout=30.0,
    max_clients=64,
)


def request_class(route, target):
    """Coarse bounded shape, never query text or identifiers, for latency cohorts."""
    if route != "search_text":
        return route
    values = dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(target).query))
    included = (
        values.get("include_groups_ids", "").split(",")
        if values.get("include_groups_ids")
        else []
    )
    scope = "single" if len(included) == 1 else "multi" if included else "all"
    span = "open"
    if values.get("from_date") and values.get("to_date"):
        try:
            start = datetime.fromisoformat(values["from_date"].replace("Z", "+00:00"))
            end = datetime.fromisoformat(values["to_date"].replace("Z", "+00:00"))
            days = abs((end - start).total_seconds()) / 86400
            span = "short" if days <= 7 else "medium" if days <= 90 else "long"
        except ValueError:
            pass
        except TypeError:
            # Mixed timezone-aware/naive dates are accepted by the existing validator.
            pass
    limit = int(values.get("limit", "200"))
    size = "small" if limit <= 50 else "medium" if limit <= 500 else "large"
    return f"{route}/{scope}/{span}/{size}"


class AdaptiveRelayServer(RelayServer):
    request_queue_size = 64

    def __init__(
        self,
        address,
        *,
        key_file,
        state_dir,
        transport_factory=HTTPSUpstream,
        gate_options=None,
        **options,
    ):
        self.options = DEFAULTS | options
        if set(self.options) != set(DEFAULTS):
            raise ValueError("unknown option")
        if any(
            type(v) not in (int, float) or not math.isfinite(v) or v <= 0
            for k, v in self.options.items()
            if k != "queue_wait" or v is not None
        ):
            raise ValueError("invalid limit")
        if (
            type(self.options["max_clients"]) is not int
            or self.options["max_clients"] > 4096
        ):
            raise ValueError("invalid client limit")
        with Path(key_file).open("rb") as handle:
            key = handle.read(4097).strip()
        if not key or len(key) > 4096 or any(c <= 32 or c >= 127 for c in key):
            raise ValueError("invalid relay key")
        self.key = key.decode("ascii")
        self.state_dir = Path(state_dir)
        self.gate = AdaptiveGate(self.state_dir, **(gate_options or {}))
        self.transport_factory = transport_factory
        self.clients = threading.BoundedSemaphore(self.options["max_clients"])
        self.faulted = False
        self.observations = threading.local()
        ThreadingHTTPServer.__init__(self, address, AdaptiveRelayHandler)


class AdaptiveRelayHandler(RelayHandler):
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
        observation = self.server.observations.current
        gate = self.server.gate
        self.connection.settimeout(self.server.options["socket_idle"])
        if path in {"/health", "/status"}:
            try:
                state = gate.read()
                healthy = not self.server.faulted and not state.get("faulted", False)
                self.reply(
                    200 if healthy else 503,
                    {"ok": healthy} if path == "/health" else state,
                )
            except (OSError, ValueError):
                self.reply(503)
            return
        if self.server.faulted:
            self.reply(503, retry_after=1)
            return
        observation.emit("queue_entry")
        try:
            lease = gate.acquire(
                self.server.options["queue_wait"],
                request_class=request_class(observation.route_class, self.path),
                cancelled=self.disconnected,
            )
        except Cancelled:
            observation.origin = "client_cancelled"
            return
        except (Busy, OSError, ValueError, OverflowError):
            try:
                delay = gate.retry_after()
            except (OSError, ValueError, OverflowError):
                delay = 1
            self.reply(503, retry_after=delay)
            return
        observation.origin = "upstream"
        observation.emit("admitted")
        upstream = response = watchdog = None
        stopped = threading.Event()
        cancelled = threading.Event()
        timed_out = threading.Event()
        headers_sent = False
        outcome = "failure"
        upstream_failed = False
        close_ok = True
        try:
            upstream = self.server.transport_factory(
                min(
                    self.server.options["socket_idle"],
                    self.server.options["request_deadline"],
                )
            )
            deadline = time.monotonic() + self.server.options["request_deadline"]

            def watch():
                while not stopped.wait(0.02):
                    if self.disconnected():
                        cancelled.set()
                    elif time.monotonic() >= deadline:
                        timed_out.set()
                    else:
                        continue
                    try:
                        upstream.abort()
                    finally:
                        self.abort_client()
                    return

            watchdog = threading.Thread(target=watch)
            watchdog.start()
            headers = {
                "Authorization": "Bearer " + self.server.key,
                "Accept": "application/json",
                "Accept-Encoding": "identity",
                "User-Agent": "VoxRelay/1",
                "Connection": "close",
            }
            if body is not None:
                headers["Content-Type"] = "application/json"
            response = upstream.request(self.command, self.path, body, headers)
            status = response.status
            observation.status = status
            observation.emit("upstream_headers")
            retry_after = response.getheader("Retry-After")
            gate.headers(lease, status, retry_after=retry_after)
            upstream_failed = status == 429 or status >= 500
            if 300 <= status < 400:
                raise OSError("redirect refused")
            try:
                self.send_response(status)
                self.send_header("Connection", "close")
                self.send_header(
                    "Content-Type",
                    response.getheader("Content-Type", "application/json"),
                )
                if (
                    retry_after is not None
                    and len(retry_after) <= 1024
                    and all(32 <= ord(c) < 127 for c in retry_after)
                ):
                    self.send_header("Retry-After", retry_after)
                self.end_headers()
            except OSError:
                cancelled.set()
                raise
            headers_sent = True
            probe = CompletionProbe(
                path == "/search/text", notify=lambda: gate.progress(lease)
            )
            while True:
                chunk = response.read1(65536)
                if not chunk:
                    if response.length not in (None, 0):
                        raise http.client.IncompleteRead(b"")
                    break
                probe.feed(chunk)
                upstream_failed |= 200 <= status < 300 and probe.failed
                try:
                    observation.capture(chunk)
                except OSError:
                    observation.trace_gaps.append("raw_body")
                try:
                    self.wfile.write(chunk)
                    self.wfile.flush()
                except OSError:
                    cancelled.set()
                    raise
            if timed_out.is_set() or time.monotonic() >= deadline:
                raise TimeoutError("request deadline")
            outcome = (
                probe.finish()
                if 200 <= status < 300
                else "failure"
                if upstream_failed
                else "neutral"
            )
            upstream_failed |= 200 <= status < 300 and probe.failed
        except (OSError, ValueError, http.client.HTTPException):
            if cancelled.is_set() and not upstream_failed and not timed_out.is_set():
                outcome = "cancelled"
                observation.origin = "client_cancelled"
            else:
                outcome = "failure"
                observation.origin = "transport_error"
            if not headers_sent and not cancelled.is_set():
                try:
                    self.reply(502)
                except OSError:
                    pass
        finally:
            stopped.set()
            if watchdog is not None:
                watchdog.join()
            for resource in (response, upstream):
                if resource is not None:
                    try:
                        resource.close()
                    except Exception:
                        close_ok = False
                        self.server.faulted = True
            self.close_connection = True
            if cancelled.is_set() and not upstream_failed and not timed_out.is_set():
                outcome = "cancelled"
            try:
                gate.finish(lease, outcome=outcome, transport_closed=close_ok)
            except (OSError, ValueError, OverflowError):
                self.server.faulted = True
            finally:
                # Publish completion after upstream close, before the client can
                # observe EOF and begin another batch using stale admission state.
                self.abort_client()


def main():
    if os.environ.get("VOX_RELAY_ADAPTIVE") != "1":
        raise ValueError("adaptive relay requires explicit VOX_RELAY_ADAPTIVE=1")
    host = os.environ.get("VOX_RELAY_HOST", "127.0.0.1")
    if not ipaddress.ip_address(host).is_private and host != "0.0.0.0":
        raise ValueError("relay requires a private bind address")
    server = AdaptiveRelayServer(
        (host, int(os.environ.get("VOX_RELAY_PORT", "8083"))),
        key_file=os.environ.get("VOX_RELAY_KEY_FILE", "/run/secrets/vox-api-key"),
        state_dir=os.environ.get(
            "VOX_RELAY_ADAPTIVE_STATE_DIR", "/var/lib/vox-relay-adaptive-state"
        ),
    )
    try:
        server.serve_forever()
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
