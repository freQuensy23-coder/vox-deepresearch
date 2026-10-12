from dataclasses import dataclass, replace
from types import SimpleNamespace

import pytest
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

from deerflow.agents.middlewares.loop_detection_middleware import LoopDetectionMiddleware


@dataclass
class WarningRequest:
    messages: list
    runtime: object

    def override(self, **updates):
        return replace(self, **updates)


def distinct_call(index):
    return {"name": "bash", "id": f"call_{index}", "args": {"command": f"read-source-{index}"}, "type": "tool_call"}


def warned_middleware():
    middleware = LoopDetectionMiddleware()
    runtime = SimpleNamespace(context={"thread_id": "frequency-thread", "run_id": "frequency-run"})
    for index in range(30):
        message = AIMessage(content="", tool_calls=[distinct_call(index)])
        assert middleware.after_model({"messages": [message]}, runtime) is None
        assert message.tool_calls == [distinct_call(index)]
    return middleware, runtime, message


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.asyncio
async def test_frequency_warning_is_nonterminal_at_thirty_distinct_calls(asynchronous):
    middleware, runtime, message = warned_middleware()
    messages = [HumanMessage(content="Inspect distinct sources"), message, ToolMessage(content="saved", tool_call_id="call_29")]
    request = WarningRequest(messages=messages, runtime=runtime)
    captured = []

    def handler(outgoing):
        captured.append(outgoing)
        return AIMessage(content="", tool_calls=[distinct_call(30)])

    async def async_handler(outgoing):
        return handler(outgoing)

    if asynchronous:
        response = await middleware.awrap_model_call(request, async_handler)
    else:
        response = middleware.wrap_model_call(request, handler)
    warning = captured[0].messages[-1]
    assert warning.name == "loop_warning"
    text = warning.content.lower()
    assert "stop calling tools" not in text
    assert "produce your final answer now" not in text
    assert "[loop detected]" not in text
    assert "progress" in text
    assert "preserve" in text
    assert "continue" in text
    assert "budget" in text
    assert captured[0].messages[:-1] == messages
    assert request.messages == messages
    assert middleware.after_model({"messages": [response]}, runtime) is None
    assert response.tool_calls == [distinct_call(30)]
    assert runtime.context.get("stop_reason") is None
    assert middleware.consume_stop_reason("frequency-run") is None


def test_frequency_hard_stop_remains_at_fifty_distinct_calls():
    middleware, runtime, _ = warned_middleware()
    for index in range(30, 49):
        assert middleware.after_model({"messages": [AIMessage(content="", tool_calls=[distinct_call(index)])]}, runtime) is None
    stopped = middleware.after_model({"messages": [AIMessage(content="", tool_calls=[distinct_call(49)])]}, runtime)
    assert stopped["messages"][0].tool_calls == []
    assert "[FORCED STOP]" in stopped["messages"][0].content
    assert runtime.context["stop_reason"] == "loop_capped"
    assert middleware.consume_stop_reason("frequency-run") == "loop_capped"


def test_identical_call_hard_stop_remains_at_five():
    middleware = LoopDetectionMiddleware()
    runtime = SimpleNamespace(context={"thread_id": "repeat-thread", "run_id": "repeat-run"})
    for _ in range(4):
        assert middleware.after_model({"messages": [AIMessage(content="", tool_calls=[distinct_call(0)])]}, runtime) is None
    stopped = middleware.after_model({"messages": [AIMessage(content="", tool_calls=[distinct_call(0)])]}, runtime)
    assert stopped["messages"][0].tool_calls == []
    assert middleware.consume_stop_reason("repeat-run") == "loop_capped"
