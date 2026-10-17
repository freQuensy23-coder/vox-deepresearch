from __future__ import annotations

import asyncio
import base64
import dataclasses
import hashlib
import json
import logging
import os
import re
import threading
import time
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import UTC, datetime
from functools import wraps
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

from langchain_core.callbacks import BaseCallbackHandler

logger = logging.getLogger(__name__)
_PROCESS_INSTANCE_ID = uuid4().hex
_active_attempt: ContextVar[Any] = ContextVar("local_llm_trace_attempt", default=None)
_provider_call: ContextVar[Any] = ContextVar("local_llm_trace_call", default=None)
_invocation_calls: ContextVar[Any] = ContextVar("local_llm_trace_invocation", default=None)
_trace_scope: ContextVar[Any] = ContextVar("local_llm_trace_scope", default={})
_calls = {}
_calls_lock = threading.RLock()
_trace_gaps = []
_gap_lock = threading.RLock()
_SECRET_KEYS = {
    "authorization",
    "proxy_authorization",
    "api_key",
    "apikey",
    "access_token",
    "refresh_token",
    "password",
    "secret",
    "client_secret",
    "cookie",
    "set_cookie",
    "headers",
    "default_headers",
    "extra_headers",
    "http_client",
    "http_async_client",
    "callbacks",
    "rate_limiter",
}


def _clean(value, secrets=()):
    if isinstance(value, UUID):
        return str(value)
    if hasattr(value, "get_secret_value"):
        return "[REDACTED]"
    if hasattr(value, "model_dump"):
        return _clean(value.model_dump(serialize_as_any=True), secrets)
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {field.name: _clean(getattr(value, field.name), secrets) for field in dataclasses.fields(value)}
    if isinstance(value, bytes):
        return {"encoding": "base64", "data": base64.b64encode(value).decode("ascii")}
    if isinstance(value, dict):
        return {str(k): _clean(v, secrets) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_clean(v, secrets) for v in value]
    if isinstance(value, str):
        for secret in secrets:
            if secret:
                value = value.replace(secret, "[REDACTED]")
        return value
    if value is None or isinstance(value, (bool, int, float)):
        return value
    return {"unserializable_type": type(value).__name__}


def _settings(value):
    if not isinstance(value, dict):
        return _clean(value)
    return {str(k): "[TRANSPORT EXCLUDED]" if str(k).lower().replace("-", "_") in _SECRET_KEYS else _clean(v) for k, v in value.items()}


def trace_health(root=None):
    from deerflow.config.runtime_paths import runtime_home

    root = Path(root or os.environ.get("DEER_FLOW_LLM_TRACE_DIR") or runtime_home() / "llm-traces")
    persisted = set()
    tool_starts = set()
    tool_ends = set()
    unreadable = 0
    if root.is_symlink():
        unreadable = 1
    elif root.exists():
        for record in _session_records(root):
            try:
                if record is None:
                    raise ValueError("Unreadable trace record")
                if record["event"] == "tool_start":
                    tool_starts.add(record["call_id"])
                if record["event"] in {"tool_end", "tool_interrupted"}:
                    tool_ends.add(record["call_id"])
                if record["event"] == "trace_gap":
                    persisted.add((record["process_instance_id"], record["payload"]["call_id"], record["payload"]["utc"]))
            except (ValueError, KeyError):
                unreadable += 1
    with _gap_lock:
        pending = sum(not gap["persisted"] for gap in _trace_gaps)
        gaps = max(len(_trace_gaps), len(persisted))
        unfinished = len(tool_starts - tool_ends)
        return {
            "complete": not gaps and not unreadable and not unfinished,
            "gap_count": gaps,
            "pending_gap_count": pending,
            "unreadable_record_count": unreadable,
            "unfinished_tool_execution_count": unfinished,
            "process_instance_id": _PROCESS_INSTANCE_ID,
        }


def _private_directory(path):
    path = path.absolute()
    for parent in reversed((path, *path.parents)):
        if parent.is_symlink():
            raise OSError("Trace directory symlink rejected")
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    if path.stat().st_uid != os.getuid():
        raise OSError("Trace directory owner mismatch")
    path.chmod(0o700)
    return path


_SAFE_SESSION = re.compile(r"[A-Za-z0-9_.-]{1,128}")
_append_locks: dict[Path, threading.Lock] = {}
_append_locks_guard = threading.Lock()


def session_file(bucket, thread_id):
    """One JSONL trace file per session (thread): readable thread ids are kept, anything else is hashed."""
    name = str(thread_id) if thread_id is not None else "no-thread"
    if not _SAFE_SESSION.fullmatch(name) or name.startswith("."):
        name = hashlib.sha256(name.encode()).hexdigest()
    return bucket / f"{name}.jsonl"


def _write_record(path, record):
    """Append one record as a JSON line. One file per session instead of one per record: on a
    network volume every file costs a whole storage block and every fsync a round trip. The
    caller has created and checked the session directory. Threads of this process take turns
    per file; there is no flock: on the network volume a lock left by a dead client is never
    released and would block every later write."""
    line = (json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8")
    with _append_locks_guard:
        lock = _append_locks.setdefault(path, threading.Lock())
    with lock:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND | os.O_NOFOLLOW, 0o600)
        try:
            view = memoryview(line)
            while view:
                view = view[os.write(descriptor, view) :]
        finally:
            os.close(descriptor)


def _session_records(root):
    """Every record of every session file under ROOT; None for an unreadable line or file."""
    for path in Path(root).glob("*/*.jsonl"):
        if path.is_symlink():
            yield None
            continue
        try:
            with path.open(encoding="utf-8") as stream:
                for line in stream:
                    if line.strip():
                        try:
                            yield json.loads(line)
                        except ValueError:
                            yield None
        except OSError:
            yield None


def _correlation(metadata=None):
    from deerflow.runtime.user_context import get_effective_user_id

    values = {**_trace_scope.get(), **(metadata or {})}
    journal = None
    try:
        from langgraph.config import get_config

        config = get_config()
        configurable = config.get("configurable", {})
        runtime = configurable.get("__pregel_runtime")
        context = getattr(runtime, "context", {}) or config.get("context", {})
        journal = context.get("__run_journal")
        values = {**context, **configurable, **config.get("metadata", {}), **values}
    except (RuntimeError, AttributeError):
        pass
    return {
        "owner_id": get_effective_user_id(),
        "run_id": str(journal.run_id) if getattr(journal, "run_id", None) is not None else values.get("run_id"),
        "thread_id": str(journal.thread_id) if getattr(journal, "thread_id", None) is not None else values.get("thread_id", values.get("langfuse_session_id")),
        "child_call_id": values.get("task_id", values.get("subagent_id")),
        "checkpoint_ns": values.get("checkpoint_ns", values.get("langgraph_checkpoint_ns")),
        "operation": values.get("operation", values.get("lc_source", values.get("langgraph_node"))),
        "request_trace_id": values.get("deerflow_trace_id"),
    }


@contextmanager
def trace_scope(**values):
    token = _trace_scope.set({**_trace_scope.get(), **values})
    try:
        yield
    finally:
        _trace_scope.reset(token)


class CallTrace:
    def __init__(self, call_id=None, parent_call_id=None, metadata=None):
        self.call_id = str(call_id or uuid4())
        self.parent_call_id = str(parent_call_id) if parent_call_id is not None else None
        self.correlation = _correlation(metadata)
        self.path = None
        self.lock = threading.RLock()
        self.started_monotonic_ns = time.monotonic_ns()
        self.wire_attempt = 0

    def emit(self, event, payload=None, *, attempt=None, secrets=()):
        try:
            with self.lock:
                if self.path is None:
                    from deerflow.config.runtime_paths import runtime_home

                    root = _private_directory(Path(os.environ.get("DEER_FLOW_LLM_TRACE_DIR") or runtime_home() / "llm-traces"))
                    owner = hashlib.sha256(str(self.correlation["owner_id"]).encode()).hexdigest()
                    self.path = session_file(_private_directory(root / owner), self.correlation["thread_id"])
                record = {
                    "schema_version": 1,
                    "event": event,
                    "call_id": self.call_id,
                    "parent_call_id": self.parent_call_id,
                    **self.correlation,
                    "attempt": attempt,
                    "utc": datetime.now(UTC).isoformat(),
                    "monotonic_ns": time.monotonic_ns(),
                    "started_monotonic_ns": self.started_monotonic_ns,
                    "pid": os.getpid(),
                    "process_instance_id": _PROCESS_INSTANCE_ID,
                    "payload": _clean(payload),
                }
                with _gap_lock:
                    for gap in _trace_gaps:
                        writer = gap["writer"]
                        if not gap["persisted"] and writer.path is not None:
                            try:
                                _write_record(
                                    writer.path,
                                    {**record, **writer.correlation, "call_id": writer.call_id, "parent_call_id": writer.parent_call_id, "event": "trace_gap", "payload": {k: v for k, v in gap.items() if k not in {"persisted", "writer"}}},
                                )
                                gap["persisted"] = True
                            except Exception:
                                pass
                _write_record(self.path, record)
                return True
        except Exception as error:
            with _gap_lock:
                _trace_gaps.append({"call_id": self.call_id, "missing_event": event, "attempt": attempt, "error_type": type(error).__name__, "utc": datetime.now(UTC).isoformat(), "persisted": False, "writer": self})
            logger.error("LLM trace write failed call=%s event=%s error_type=%s", self.call_id, event, type(error).__name__)
            return False


def error_payload(error, secrets=()):
    response = getattr(error, "response", None)
    payload = {"type": type(error).__name__, "message": _clean(str(error), secrets)}
    if response is not None:
        payload["status_code"] = getattr(response, "status_code", None)
        try:
            payload["body"] = _clean(response.json(), secrets)
        except Exception:
            try:
                payload["body"] = _clean(response.text, secrets)
            except Exception:
                payload["body_unavailable"] = True
    return payload


class LocalLLMTraceCallback(BaseCallbackHandler):
    def __init__(self, settings=None):
        self.settings = settings or {}

    def on_chat_model_start(self, serialized, messages, *, run_id, parent_run_id=None, metadata=None, **kwargs):
        trace = CallTrace(run_id, parent_run_id, metadata)
        with _calls_lock:
            _calls[str(run_id)] = trace
        invocation = _invocation_calls.get()
        if invocation is not None:
            invocation.append(trace)
        trace.emit("call_start", {"messages": messages, "model_settings": _settings(self.settings), "invocation_params": _settings(kwargs.get("invocation_params")), "options": _settings(kwargs.get("options")), "tags": kwargs.get("tags")})

    def on_llm_end(self, response, *, run_id, **kwargs):
        with _calls_lock:
            trace = _calls.pop(str(run_id), None)
        if trace is not None:
            trace.emit("call_end", response)

    def on_llm_new_token(self, token, *, run_id, chunk=None, **kwargs):
        with _calls_lock:
            trace = _calls.get(str(run_id))
        if trace is not None:
            trace.emit("call_chunk", {"token": token, "chunk": chunk})

    def on_llm_error(self, error, *, run_id, **kwargs):
        with _calls_lock:
            trace = _calls.pop(str(run_id), None)
        if trace is not None:
            trace.emit("call_cancelled" if isinstance(error, (asyncio.CancelledError, GeneratorExit)) else "call_error", {"error": error_payload(error), "partial_response": kwargs.get("response")})

    def on_retry(self, retry_state, *, run_id, **kwargs):
        with _calls_lock:
            trace = _calls.get(str(run_id))
        if trace is not None:
            trace.emit("retry", {"attempt_number": retry_state.attempt_number})


@contextmanager
def provider_call(run_manager, messages):
    run_id = getattr(run_manager, "run_id", None)
    with _calls_lock:
        trace = _calls.get(str(run_id))
        invocation = _invocation_calls.get()
        if trace is None and invocation:
            trace = _calls.get(invocation[-1].call_id)
    standalone = trace is None
    if standalone:
        trace = CallTrace(run_id, getattr(run_manager, "parent_run_id", None), getattr(run_manager, "metadata", None))
        trace.emit("call_start", {"messages": [messages], "boundary": "provider"})
    token = _provider_call.set(trace)
    try:
        yield trace
    except BaseException as error:
        if standalone:
            trace.emit("call_cancelled" if isinstance(error, (asyncio.CancelledError, GeneratorExit)) else "call_error", error_payload(error))
        raise
    else:
        if standalone:
            trace.emit("call_end", {"response_in_attempt_end": True})
    finally:
        _provider_call.reset(token)


def current_provider_trace():
    return _provider_call.get() or CallTrace()


@contextmanager
def provider_attempt(trace, attempt, payload, secrets=()):
    token = _active_attempt.set((trace, attempt, secrets))
    trace.emit("attempt_start", payload, attempt=attempt, secrets=secrets)
    try:
        yield
    except BaseException as error:
        trace.emit("attempt_cancelled" if isinstance(error, (asyncio.CancelledError, GeneratorExit)) else "attempt_error", error_payload(error, secrets), attempt=attempt, secrets=secrets)
        raise
    finally:
        _active_attempt.reset(token)


def provider_event(payload):
    active = _active_attempt.get()
    if active is not None:
        trace, attempt, secrets = active
        trace.emit("provider_event", payload, attempt=attempt, secrets=secrets)


def provider_sse_line(line):
    active = _active_attempt.get()
    if active is not None:
        trace, attempt, secrets = active
        trace.emit("provider_sse", {"line": line}, attempt=attempt)


def mark_interrupted_calls(root, *, process_instance_id, confirmed_stopped=False):
    if not confirmed_stopped:
        raise ValueError("Confirm the writer process has stopped before recovery")
    count = 0
    for path in Path(root).glob("*/*.jsonl"):
        if path.is_symlink():
            raise OSError("Trace record symlink rejected")
        starts, finished = {}, set()
        with path.open(encoding="utf-8") as stream:
            for line in stream:
                if not line.strip():
                    continue
                record = json.loads(line)
                if record.get("process_instance_id") != process_instance_id:
                    continue
                if record["event"] in {"call_start", "tool_start"}:
                    starts[record["call_id"]] = record
                if record["event"] in {"call_end", "call_error", "call_cancelled", "call_interrupted", "tool_end", "tool_interrupted"}:
                    finished.add(record["call_id"])
        for call_id, start in starts.items():
            if call_id in finished:
                continue
            _write_record(
                path,
                {
                    **start,
                    "event": "tool_interrupted" if start["event"] == "tool_start" else "call_interrupted",
                    "utc": datetime.now(UTC).isoformat(),
                    "monotonic_ns": time.monotonic_ns(),
                    "payload": {"reason": "writer_confirmed_stopped_without_terminal_record", "status": "interrupted", "end_utc": None, "duration_ns": None, "execution_end_unknown": True},
                },
            )
            count += 1
    return count


def instrument_cancellation(model):
    if getattr(model, "_local_trace_instrumented", False):
        return
    original_generate = model.agenerate
    original_stream = model.astream
    original_sync_stream = model.stream

    async def cancelled(calls, error):
        for trace in calls:
            with _calls_lock:
                active = _calls.pop(trace.call_id, None)
            if active is not None:
                await asyncio.shield(asyncio.to_thread(trace.emit, "call_cancelled", error_payload(error)))

    @wraps(original_generate)
    async def generate(*args, **kwargs):
        calls = []
        token = _invocation_calls.set(calls)
        try:
            return await original_generate(*args, **kwargs)
        except (asyncio.CancelledError, GeneratorExit) as error:
            await cancelled(calls, error)
            raise
        finally:
            _invocation_calls.reset(token)

    @wraps(original_stream)
    async def stream(*args, **kwargs):
        calls = []
        iterator = original_stream(*args, **kwargs)
        try:
            while True:
                token = _invocation_calls.set(calls)
                try:
                    chunk = await anext(iterator)
                except StopAsyncIteration:
                    break
                finally:
                    _invocation_calls.reset(token)
                yield chunk
        except (asyncio.CancelledError, GeneratorExit) as error:
            await cancelled(calls, error)
            raise
        finally:
            await iterator.aclose()

    object.__setattr__(model, "agenerate", generate)
    object.__setattr__(model, "astream", stream)

    @wraps(original_sync_stream)
    def sync_stream(*args, **kwargs):
        calls = []
        iterator = original_sync_stream(*args, **kwargs)
        try:
            while True:
                token = _invocation_calls.set(calls)
                try:
                    chunk = next(iterator)
                except StopIteration:
                    break
                finally:
                    _invocation_calls.reset(token)
                yield chunk
        except GeneratorExit as error:
            for trace in calls:
                with _calls_lock:
                    active = _calls.pop(trace.call_id, None)
                if active is not None:
                    trace.emit("call_cancelled", error_payload(error))
            raise
        finally:
            iterator.close()

    object.__setattr__(model, "stream", sync_stream)
    object.__setattr__(model, "_local_trace_instrumented", True)


def instrument_provider_wire(model):
    import base64
    import inspect

    import httpx

    if model.__class__.__module__.endswith("openai_codex_provider"):
        return "codex_payload_and_sse"
    clients = []
    for name in ("root_client", "root_async_client", "_client", "_async_client", "client", "async_client", "http_client", "http_async_client"):
        candidate = getattr(model, name, None)
        for value in (candidate, getattr(candidate, "_client", None)):
            if isinstance(value, (httpx.Client, httpx.AsyncClient)) and all(value is not item for item in clients):
                clients.append(value)
    if not clients:
        return "logical_calls_only_no_httpx_transport"

    def body(data):
        try:
            return json.loads(data)
        except (ValueError, UnicodeDecodeError):
            return {"base64": base64.b64encode(data).decode("ascii")}

    def begin(request):
        trace = current_provider_trace()
        with trace.lock:
            trace.wire_attempt = getattr(trace, "wire_attempt", 0) + 1
            attempt = trace.wire_attempt
        trace.emit("wire_attempt_start", {"method": request.method, "path": request.url.path, "body": body(request.content)}, attempt=attempt)
        return trace, attempt

    class SyncStream(httpx.SyncByteStream):
        def __init__(self, stream, trace, attempt, status):
            self.stream, self.trace, self.attempt, self.status = stream, trace, attempt, status
            self.complete = False

        def __iter__(self):
            try:
                for chunk in self.stream:
                    self.trace.emit("wire_response_chunk", {"base64": base64.b64encode(chunk).decode("ascii")}, attempt=self.attempt)
                    yield chunk
                self.complete = True
                self.trace.emit("wire_attempt_end", {"status_code": self.status}, attempt=self.attempt)
            except BaseException as error:
                self.trace.emit("wire_attempt_error", {"type": type(error).__name__}, attempt=self.attempt)
                raise

        def close(self):
            try:
                self.stream.close()
            finally:
                if not self.complete:
                    self.trace.emit("wire_attempt_closed", {"status_code": self.status, "fully_consumed": False}, attempt=self.attempt)

    class AsyncStream(httpx.AsyncByteStream):
        def __init__(self, stream, trace, attempt, status):
            self.stream, self.trace, self.attempt, self.status = stream, trace, attempt, status
            self.complete = False

        async def __aiter__(self):
            try:
                async for chunk in self.stream:
                    await asyncio.to_thread(self.trace.emit, "wire_response_chunk", {"base64": base64.b64encode(chunk).decode("ascii")}, attempt=self.attempt)
                    yield chunk
                self.complete = True
                await asyncio.to_thread(self.trace.emit, "wire_attempt_end", {"status_code": self.status}, attempt=self.attempt)
            except BaseException as error:
                await asyncio.shield(asyncio.to_thread(self.trace.emit, "wire_attempt_cancelled" if isinstance(error, asyncio.CancelledError) else "wire_attempt_error", {"type": type(error).__name__}, attempt=self.attempt))
                raise

        async def aclose(self):
            try:
                await self.stream.aclose()
            finally:
                if not self.complete:
                    await asyncio.to_thread(self.trace.emit, "wire_attempt_closed", {"status_code": self.status, "fully_consumed": False}, attempt=self.attempt)

    def wrap_send(original, asynchronous):
        def finish(response, trace, attempt):
            if hasattr(response, "_content"):
                trace.emit("wire_attempt_end", {"status_code": response.status_code, "body": body(response.content)}, attempt=attempt)
            else:
                response.stream = (AsyncStream if asynchronous else SyncStream)(response.stream, trace, attempt, response.status_code)
            return response

        if asynchronous:

            async def send(request, *args, **kwargs):
                trace, attempt = await asyncio.to_thread(begin, request)
                try:
                    return await asyncio.to_thread(finish, await original(request, *args, **kwargs), trace, attempt)
                except BaseException as error:
                    await asyncio.shield(asyncio.to_thread(trace.emit, "wire_attempt_cancelled" if isinstance(error, asyncio.CancelledError) else "wire_attempt_error", {"type": type(error).__name__}, attempt=attempt))
                    raise
        else:

            def send(request, *args, **kwargs):
                trace, attempt = begin(request)
                try:
                    return finish(original(request, *args, **kwargs), trace, attempt)
                except BaseException as error:
                    trace.emit("wire_attempt_error", {"type": type(error).__name__}, attempt=attempt)
                    raise

        return send

    for client in clients:
        if not getattr(client, "_local_trace_wire", False):
            client.send = wrap_send(client.send, isinstance(client, httpx.AsyncClient))
            client._local_trace_wire = True

    def wrap_provider(original, kind):
        signature = inspect.signature(original)

        def context(args, kwargs):
            arguments = signature.bind_partial(*args, **kwargs).arguments
            return provider_call(arguments.get("run_manager"), arguments.get("messages", []))

        if kind == "async":

            async def invoke(*args, **kwargs):
                with context(args, kwargs):
                    return await original(*args, **kwargs)
        elif kind == "async_stream":

            async def invoke(*args, **kwargs):
                with context(args, kwargs):
                    async for chunk in original(*args, **kwargs):
                        yield chunk
        elif kind == "stream":

            def invoke(*args, **kwargs):
                with context(args, kwargs):
                    yield from original(*args, **kwargs)
        else:

            def invoke(*args, **kwargs):
                with context(args, kwargs):
                    return original(*args, **kwargs)

        return invoke

    for name, kind in (("_generate", "sync"), ("_agenerate", "async"), ("_stream", "stream"), ("_astream", "async_stream")):
        object.__setattr__(model, name, wrap_provider(getattr(model, name), kind))
    return "httpx_requests_and_consumed_responses"
