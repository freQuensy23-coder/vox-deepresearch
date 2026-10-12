import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, call

import httpx
import pytest
from langchain_core.messages import AIMessage, HumanMessage

from deerflow.models import openai_codex_provider
from deerflow.runtime import goal
from deerflow.runtime.events.store.memory import MemoryRunEventStore
from deerflow.runtime.journal import RunJournal


@pytest.fixture
def codex_evaluator(monkeypatch):
    requests = []
    outcomes = []
    client_type = httpx.Client

    def handle(request):
        requests.append(json.loads(request.content))
        outcome = outcomes.pop(0)
        if isinstance(outcome, int):
            return httpx.Response(outcome)
        event = {
            "type": "response.completed",
            "response": {
                "output": [{"type": "message", "content": [{"type": "output_text", "text": outcome}]}],
                "usage": {"input_tokens": 20, "output_tokens": 5, "total_tokens": 25, "input_tokens_details": {"cached_tokens": 8}},
            },
        }
        return httpx.Response(200, text="data: " + json.dumps(event) + "\n\n")

    monkeypatch.setattr(openai_codex_provider.CodexChatModel, "_load_codex_auth", lambda self: SimpleNamespace(access_token="offline-token", account_id="offline-account"))
    monkeypatch.setattr(openai_codex_provider.httpx, "Client", lambda **kwargs: client_type(transport=httpx.MockTransport(handle), **kwargs))
    sleep = AsyncMock()
    monkeypatch.setattr(goal.asyncio, "sleep", sleep)
    model = openai_codex_provider.CodexChatModel(model="offline-critic")
    return SimpleNamespace(model=model, requests=requests, outcomes=outcomes, sleep=sleep)


async def evaluate(model, **kwargs):
    return await goal.evaluate_goal_completion(goal.build_goal_state("Report"), [HumanMessage(content="Report"), AIMessage(content="Verified")], model=model, **kwargs)


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [502, 503, 504])
async def test_evaluator_retries_transient_http_then_valid_verdict(codex_evaluator, status):
    codex_evaluator.outcomes.extend([status, '{"satisfied": false, "blocker": "goal_not_met_yet"}'])
    verdict = await evaluate(codex_evaluator.model)
    assert verdict["satisfied"] is False
    assert verdict["blocker"] == "goal_not_met_yet"
    assert len(codex_evaluator.requests) == 2
    assert codex_evaluator.requests[0] == codex_evaluator.requests[1]
    codex_evaluator.sleep.assert_awaited_once_with(1)


@pytest.mark.asyncio
async def test_evaluator_persistent_503_is_bounded_and_raises(codex_evaluator):
    codex_evaluator.outcomes.extend([503] * 4)
    records = []
    with pytest.raises(httpx.HTTPStatusError) as caught:
        await evaluate(codex_evaluator.model, usage_callback=records.extend)
    assert caught.value.response.status_code == 503
    assert len(codex_evaluator.requests) == 3
    assert codex_evaluator.sleep.await_args_list == [call(1), call(2)]
    assert len({record["source_run_id"] for record in records}) == 3
    assert all(record["usage_missing"] == 1 for record in records)


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [400, 401, 403, 404])
async def test_evaluator_nontransient_http_is_not_retried(codex_evaluator, status):
    codex_evaluator.outcomes.extend([status, '{"satisfied": true}'])
    with pytest.raises(httpx.HTTPStatusError) as caught:
        await evaluate(codex_evaluator.model)
    assert caught.value.response.status_code == status
    assert len(codex_evaluator.requests) == 1
    codex_evaluator.sleep.assert_not_awaited()


@pytest.mark.asyncio
async def test_evaluator_cancellation_during_backoff_stops_retry(codex_evaluator):
    codex_evaluator.outcomes.extend([503, '{"satisfied": true}'])
    codex_evaluator.sleep.side_effect = asyncio.CancelledError
    with pytest.raises(asyncio.CancelledError):
        await evaluate(codex_evaluator.model)
    assert len(codex_evaluator.requests) == 1
    codex_evaluator.sleep.assert_awaited_once_with(1)


@pytest.mark.asyncio
async def test_evaluator_cancellation_during_request_is_preserved():
    model = SimpleNamespace(ainvoke=AsyncMock(side_effect=asyncio.CancelledError))
    records = []
    with pytest.raises(asyncio.CancelledError):
        await evaluate(model, usage_callback=records.extend)
    model.ainvoke.assert_awaited_once()
    assert len(records) == 1


@pytest.mark.asyncio
async def test_evaluator_retry_usage_counts_distinct_attempts_once(codex_evaluator):
    codex_evaluator.outcomes.extend([503, '{"satisfied": true}'])
    events = MemoryRunEventStore()
    journal = RunJournal("run", "thread", events)
    records = []

    def sink(values):
        json.dumps(values)
        records.extend(values)
        journal.record_external_llm_usage_records(values)
        journal.record_external_llm_usage_records(values)

    try:
        verdict = await evaluate(codex_evaluator.model, usage_callback=sink)
        assert verdict["satisfied"] is True
        completion = journal.get_completion_data()
        assert completion["llm_call_count"] == 2
        assert completion["middleware_tokens"] == completion["total_tokens"] == 25
        assert completion["total_input_tokens"] == 20
        assert completion["total_output_tokens"] == 5
        bucket = completion["token_usage_by_model"]["offline-critic"]
        assert bucket["missing_usage_calls"] == 1
        assert bucket["cache_read_tokens"] == 8
        assert len({record["source_run_id"] for record in records}) == 2
        assert [record["usage_missing"] for record in records] == [1, 0]
    finally:
        await journal.close()
    assert await events.list_messages("thread") == []


@pytest.mark.asyncio
async def test_evaluator_does_not_retry_invalid_verdict_after_503(codex_evaluator):
    codex_evaluator.outcomes.extend([503, "not a verdict", '{"satisfied": true}'])
    records = []
    with pytest.raises(ValueError, match="JSON object"):
        await evaluate(codex_evaluator.model, usage_callback=records.extend)
    assert len(codex_evaluator.requests) == 2
    assert len(records) == 2
    assert records[-1]["total_tokens"] == 25


@pytest.mark.asyncio
async def test_evaluator_does_not_retry_observer_failure(codex_evaluator, monkeypatch):
    from deerflow.extensions import notify

    codex_evaluator.outcomes.extend(['{"satisfied": true}'])
    error = httpx.HTTPStatusError("observer unavailable", request=httpx.Request("POST", "https://offline.invalid"), response=httpx.Response(503))

    async def fail_after_provider(*args, invoke, **kwargs):
        await invoke()
        raise error

    monkeypatch.setattr(notify, "observe_system_model_call", fail_after_provider)
    with pytest.raises(httpx.HTTPStatusError) as caught:
        await evaluate(codex_evaluator.model, extensions=object())
    assert caught.value is error
    assert len(codex_evaluator.requests) == 1
    codex_evaluator.sleep.assert_not_awaited()
