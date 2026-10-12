from __future__ import annotations

import asyncio
import json
import time
from functools import wraps
from contextvars import ContextVar
from datetime import UTC, datetime
from typing import Any

from langchain.agents.middleware import AgentMiddleware

from deerflow.models.local_trace import CallTrace, error_payload

_active_tool: ContextVar[Any] = ContextVar("local_tool_execution", default=None)


def _status(result=None, error=None):
    if isinstance(error, (asyncio.CancelledError, GeneratorExit)):
        return "cancelled"
    if isinstance(error, TimeoutError):
        return "timeout"
    if error is not None:
        return "error"
    if getattr(result, "status", None) == "error":
        return "error"
    update = getattr(result, "update", None)
    if isinstance(update, dict) and any(getattr(m, "status", None) == "error" for m in update.get("messages", [])):
        return "error"
    return "success"


class ToolExecution:
    def __init__(self, request):
        context = getattr(getattr(request, "runtime", None), "context", None)
        context = context if isinstance(context, dict) else {}
        journal = context.get("__run_journal")
        metadata = {k: context[k] for k in ("run_id", "thread_id", "task_id", "subagent_id") if context.get(k) is not None}
        for name in ("run_id", "thread_id"):
            if getattr(journal, name, None) is not None:
                metadata[name] = str(getattr(journal, name))
        parent = _active_tool.get()
        self.trace = CallTrace(parent_call_id=parent.trace.call_id if parent else None, metadata=metadata)
        self.attempts = 0
        self.started = time.monotonic_ns()
        self.identity = {
            "execution_id": self.trace.call_id,
            "tool_call_id": request.tool_call.get("id"),
            "tool_name": request.tool_call.get("name"),
            "args": request.tool_call.get("args"),
            "start_utc": datetime.now(UTC).isoformat(),
            "start_monotonic_ns": self.started,
            "parent_execution_id": parent.trace.call_id if parent else None,
        }
        if not self.trace.emit("tool_start", self.identity):
            raise RuntimeError("Tool trace start could not be persisted; execution not started")

    def begin_attempt(self, request):
        with self.trace.lock:
            self.attempts += 1
            attempt = self.attempts
        started = time.monotonic_ns()
        if not self.trace.emit(
            "tool_attempt_start",
            {**self.identity, "args": request.tool_call.get("args"), "tool_name": request.tool_call.get("name"), "attempt_start_utc": datetime.now(UTC).isoformat(), "attempt_start_monotonic_ns": started},
            attempt=attempt,
        ):
            raise RuntimeError("Tool attempt trace start could not be persisted; execution not started")
        return attempt, started

    def finish(self, result=None, error=None, attempt=None, started=None):
        ended = time.monotonic_ns()
        status = _status(result, error)
        if attempt is None and not self.attempts and error is None:
            status = "blocked"
        self.trace.emit(
            "tool_attempt_end" if attempt is not None else "tool_end",
            {
                **self.identity,
                "end_utc": datetime.now(UTC).isoformat(),
                "end_monotonic_ns": ended,
                "duration_ns": ended - (started if started is not None else self.started),
                "status": status,
                "execution_duration_ns": ended - started if attempt is not None else None,
                "internal_retry_visibility": "opaque_except_owned_vox_relay_requests",
                "attempt_count": self.attempts,
                "short_circuited": not self.attempts,
                "result": result,
                "error": error_payload(error) if error is not None else None,
                "result_boundary": "raw_tool_node" if attempt is not None else "model_visible_tool_wrapper",
            },
            attempt=attempt,
        )


class LocalToolTraceMiddleware(AgentMiddleware):
    def wrap_tool_call(self, request, handler):
        execution = ToolExecution(request)
        token = _active_tool.set(execution)
        try:
            result = handler(request)
        except BaseException as error:
            execution.finish(error=error)
            raise
        else:
            execution.finish(result=result)
            return result
        finally:
            _active_tool.reset(token)

    async def awrap_tool_call(self, request, handler):
        execution = ToolExecution(request)
        token = _active_tool.set(execution)
        try:
            result = await handler(request)
        except BaseException as error:
            execution.finish(error=error)
            raise
        else:
            execution.finish(result=result)
            return result
        finally:
            _active_tool.reset(token)


class LocalRawToolTraceMiddleware(AgentMiddleware):
    def wrap_tool_call(self, request, handler):
        execution = _active_tool.get()
        if execution is None:
            raise RuntimeError("Raw tool tracing requires an outer execution span")
        attempt, started = execution.begin_attempt(request)
        try:
            result = handler(request)
        except BaseException as error:
            execution.finish(error=error, attempt=attempt, started=started)
            raise
        execution.finish(result=result, attempt=attempt, started=started)
        return result

    async def awrap_tool_call(self, request, handler):
        execution = _active_tool.get()
        if execution is None:
            raise RuntimeError("Raw tool tracing requires an outer execution span")
        attempt, started = execution.begin_attempt(request)
        try:
            result = await handler(request)
        except BaseException as error:
            execution.finish(error=error, attempt=attempt, started=started)
            raise
        execution.finish(result=result, attempt=attempt, started=started)
        return result


def tool_trace_environment():
    execution = _active_tool.get()
    if execution is None:
        return {}
    return {"VOX_TOOL_TRACE_CONTEXT": json.dumps({
        **{k: execution.trace.correlation.get(k) for k in ("run_id", "thread_id", "child_call_id")},
        "execution_id": execution.trace.call_id,
        "tool_call_id": execution.identity["tool_call_id"],
        "tool_attempt": execution.attempts,
    }, separators=(",", ":"))}


def _observe_guard(method, name, asynchronous):
    def observe(result, called):
        execution = _active_tool.get()
        if not called and execution is not None:
            execution.trace.emit("tool_guard_result", {
                **execution.identity, "guard": name, "status": "blocked",
                "execution_duration_ns": None, "result": result,
                "result_boundary": "raw_guard_return",
            })

    @wraps(method)
    def sync(request, handler):
        called = False

        def inner(req):
            nonlocal called
            called = True
            return handler(req)

        result = method(request, inner)
        observe(result, called)
        return result

    @wraps(method)
    async def asynchronous_call(request, handler):
        called = False

        async def inner(req):
            nonlocal called
            called = True
            return await handler(req)

        result = await method(request, inner)
        observe(result, called)
        return result

    return asynchronous_call if asynchronous else sync


def _observe_proposals(method, name, asynchronous):
    def snapshot(state):
        messages = state.get("messages", [])
        return [(message.id, dict(call)) for message in messages[-1:] for call in getattr(message, "tool_calls", [])]

    def observe(before, result, runtime):
        if not isinstance(result, dict) or "messages" not in result:
            return
        replacements = {getattr(message, "id", None): message for message in result["messages"]}
        for message_id, call in before:
            if message_id not in replacements:
                continue
            replacement = replacements[message_id]
            if any(other.get("id") == call.get("id") for other in getattr(replacement, "tool_calls", [])):
                continue
            context = getattr(runtime, "context", None) or {}
            trace = CallTrace(metadata=context)
            trace.emit("tool_proposal_blocked", {
                "tool_call_id": call.get("id"), "tool_name": call.get("name"), "args": call.get("args"),
                "guard": name, "status": "blocked", "executed": False,
                "execution_duration_ns": None, "result": replacement,
                "result_boundary": "model_guard_return",
            })

    @wraps(method)
    def sync(state, runtime):
        before = snapshot(state)
        result = method(state, runtime)
        observe(before, result, runtime)
        return result

    @wraps(method)
    async def asynchronous_call(state, runtime):
        before = snapshot(state)
        result = await method(state, runtime)
        observe(before, result, runtime)
        return result

    return asynchronous_call if asynchronous else sync


def with_local_tool_tracing(middlewares):
    for middleware in middlewares:
        if getattr(middleware, "_local_trace_observed", False):
            continue
        for name, observer in (("wrap_tool_call", _observe_guard), ("awrap_tool_call", _observe_guard),
                               ("after_model", _observe_proposals), ("aafter_model", _observe_proposals)):
            if getattr(type(middleware), name) is not getattr(AgentMiddleware, name):
                setattr(middleware, name, observer(getattr(middleware, name), type(middleware).__name__, name in {"awrap_tool_call", "aafter_model"}))
        middleware._local_trace_observed = True
    return [LocalToolTraceMiddleware(), *middlewares, LocalRawToolTraceMiddleware()]
