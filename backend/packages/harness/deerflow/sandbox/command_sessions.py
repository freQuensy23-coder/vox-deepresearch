"""Owner-bound command handles; foreground waiting never owns process lifetime."""

from __future__ import annotations

import math
import threading
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

TERMINAL = {"completed", "timed_out", "cancelled", "failed"}
OUTPUT_LIMIT = 20000


def wait_budget(value: float, maximum: float = 10) -> float:
    if type(value) not in (int, float) or not math.isfinite(value) or not 0 <= value <= maximum:
        raise ValueError(f"command wait must be between 0 and {maximum:g} seconds")
    return float(value)


@dataclass
class Entry:
    owner: str
    secrets: tuple[str, ...]
    handle: Any = None
    ready: threading.Event = field(default_factory=threading.Event)
    lock: threading.Lock = field(default_factory=threading.Lock)
    cancelled: bool = False
    terminal: dict | None = None
    pending_text: dict[str, str] = field(default_factory=dict)


class CommandSessions:
    """Per-sandbox registry. IDs are opaque and never grant cross-owner access."""

    def __init__(self):
        self._lock = threading.Lock()
        self._entries: dict[str, Entry] = {}
        self._closed = False

    def start(self, factory: Callable, *, owner: str, env: dict | None, wait: float) -> dict:
        wait = wait_budget(wait)
        if not owner:
            raise ValueError("managed commands require an execution owner")
        started = time.monotonic()
        sid = uuid.uuid4().hex
        entry = Entry(owner, tuple(sorted((v for v in (env or {}).values() if v), key=len, reverse=True)))
        with self._lock:
            if self._closed:
                raise RuntimeError("managed command registry is closed")
            self._entries[sid] = entry
        try:
            entry.handle = factory()
        except BaseException:
            with self._lock:
                self._entries.pop(sid, None)
            raise
        finally:
            entry.ready.set()
        return self._poll(sid, entry, max(0.0, wait - (time.monotonic() - started)), False)

    def poll(self, sid: str, *, owner: str, wait: float = 0, cancel: bool = False) -> dict:
        wait = wait_budget(wait, 60)
        with self._lock:
            entry = self._entries[sid]
            if entry.owner != owner:
                raise PermissionError("command belongs to another execution")
        return self._poll(sid, entry, wait, cancel)

    def _poll(self, sid: str, entry: Entry, wait: float, cancel: bool) -> dict:
        with entry.lock:
            if entry.terminal is not None:
                return {**entry.terminal, "stdout": "", "stderr": ""}
            if not entry.ready.is_set():
                return {"session_id": sid, "status": "starting", "stdout": "", "stderr": "", "exit_code": None}
            if entry.handle is None:
                raise RuntimeError("command could not be started")
            if cancel or entry.cancelled:
                entry.handle.stop()
                entry.handle.close()
                result = {"session_id": sid, "status": "cancelled", "exit_code": None, "stdout": "", "stderr": ""}
            else:
                result = {**entry.handle.poll(wait), "session_id": sid}
            for key in ("stdout", "stderr"):
                output = entry.pending_text.pop(key, "") + (result.get(key) or "")
                for secret in entry.secrets:
                    output = output.replace(secret, "[REDACTED]")
                # Retain a suffix so a secret split across two polls cannot leak.
                tail = max((len(secret) - 1 for secret in entry.secrets), default=0)
                if tail and result["status"] not in TERMINAL:
                    entry.pending_text[key] = output[-tail:]
                    output = output[:-tail]
                result[key] = output[:OUTPUT_LIMIT]
                if len(output) > OUTPUT_LIMIT:
                    result["output_truncated"] = True
            if result["status"] in TERMINAL:
                entry.handle.close()
                entry.handle = None
                entry.secrets = ()
                entry.terminal = {**result, "stdout": "", "stderr": ""}
            return result

    def release(self, owner: str) -> None:
        with self._lock:
            entries = [(sid, entry) for sid, entry in self._entries.items() if entry.owner == owner]
            for _, entry in entries:
                entry.cancelled = True
        errors = []
        for sid, entry in entries:
            # A creation already in flight must publish its handle before cleanup.
            entry.ready.wait()
            try:
                if entry.handle is not None:
                    self._poll(sid, entry, 0, True)
            except Exception as exc:
                errors.append(exc)
                continue
            with self._lock:
                self._entries.pop(sid, None)
        if errors:
            raise RuntimeError("could not confirm cancellation of all background commands") from errors[0]

    def close(self):
        with self._lock:
            self._closed = True
            owners = {entry.owner for entry in self._entries.values()}
        errors = []
        for owner in owners:
            try:
                self.release(owner)
            except Exception as exc:
                errors.append(exc)
        if errors:
            raise RuntimeError("background command cleanup failed") from errors[0]
