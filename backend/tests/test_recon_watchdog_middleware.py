import json
import os

import pytest
from langchain.tools import ToolRuntime
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

from deerflow.agents.middlewares.recon_watchdog_middleware import MIN_ON_TOPIC, RECON_NUDGE, ReconWatchdogMiddleware, stop_recon_phase_tool


class Clock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


def bash(command, call_id="c1"):
    return AIMessage(content="", tool_calls=[{"name": "bash", "args": {"command": command}, "id": call_id}])


def test_vox_search_or_whale_starts_the_timer():
    clock = Clock()
    watchdog = ReconWatchdogMiddleware(idle_seconds=300, clock=clock)
    assert watchdog.after_model({"messages": [bash("python3 $V/jev.py filter 'x'")]}, None) is None
    assert watchdog.after_model({"messages": [bash("python3 $V/vox.py search 'OnlyFans' --since 2025-10-15")]}, None) == {"recon_last_vox_at": 1000.0}
    assert watchdog.after_model({"messages": [bash("python3 $V/vox.py whale 42")]}, None) == {"recon_last_vox_at": 1000.0}


def test_nudges_after_idle_period_and_restarts_timer():
    clock = Clock()
    watchdog = ReconWatchdogMiddleware(idle_seconds=300, clock=clock)
    state = {"messages": [HumanMessage("go")], "recon_last_vox_at": 1000.0}
    clock.now = 1299.0
    assert watchdog.before_model(state, None) is None
    clock.now = 1300.0
    update = watchdog.before_model(state, None)
    assert update["recon_last_vox_at"] == 1300.0
    assert update["messages"][0].additional_kwargs[RECON_NUDGE] is True
    assert "stop_recon_phase" in update["messages"][0].content


def test_silent_before_first_vox_call_and_after_stop():
    clock = Clock()
    clock.now = 10_000.0
    watchdog = ReconWatchdogMiddleware(idle_seconds=300, clock=clock)
    assert watchdog.before_model({"messages": [HumanMessage("go")]}, None) is None
    assert watchdog.before_model({"messages": [HumanMessage("go")], "recon_last_vox_at": 1000.0, "recon_stopped": True}, None) is None


def stop(state):
    runtime = ToolRuntime(state=state, context={}, config={}, stream_writer=lambda _: None, tools=[], tool_call_id="t1", store=None)
    return stop_recon_phase_tool.func(reason="all mapped", runtime=runtime)


@pytest.fixture
def research(tmp_path):
    """A thread workspace whose research folder the test fills: registry and on-topic count."""
    (tmp_path / "research").mkdir()

    def fill(keywords=(("вахта", "searched"),), whales=(("1", "fetched"),), on_topic=MIN_ON_TOPIC + 1):
        registry = {"keywords": [{"word": w, "status": st} for w, st in keywords], "whales": [{"user_id": int(u), "user_alias": f"u{u}", "status": st} for u, st in whales]}
        (tmp_path / "research/registry.json").write_text(json.dumps(registry), encoding="utf-8")
        (tmp_path / "research/messages_filtered.json").write_text(json.dumps({"messages": [{}] * on_topic}), encoding="utf-8")
        return {"thread_data": {"workspace_path": str(tmp_path)}}

    return fill


def test_stop_recon_phase_sets_state_when_recon_is_done(research):
    command = stop(research())
    assert command.update["recon_stopped"] is True
    assert isinstance(command.update["messages"][0], ToolMessage)
    assert stop({}).update["recon_stopped"] is True  # a thread without a research folder


def test_stop_recon_phase_refuses_while_keywords_or_whales_are_left(research):
    command = stop(research(keywords=(("вахта", "searched"), ("межвахта", "todo"))))
    assert "recon_stopped" not in command.update and "межвахта" in command.update["messages"][0].content
    assert "шаг с китами не делался" in stop(research(whales=())).update["messages"][0].content
    assert "u7" in stop(research(whales=(("7", "todo"),))).update["messages"][0].content


def test_few_on_topic_messages_block_the_stop_until_a_new_round_adds_none(research):
    first = stop(research(on_topic=100))
    assert "recon_stopped" not in first.update and "нужно больше 2000" in first.update["messages"][0].content
    refused_at = first.update["recon_stop_refused_at"]
    grown = stop({**research(keywords=(("вахта", "searched"), ("межвахта", "searched")), on_topic=150), "recon_stop_refused_at": refused_at})
    assert "recon_stopped" not in grown.update
    dry = stop({**research(keywords=(("вахта", "searched"), ("межвахта", "searched")), on_topic=100), "recon_stop_refused_at": refused_at})
    assert dry.update["recon_stopped"] is True


def test_busy_vox_holds_the_reminder(tmp_path):
    clock = Clock()
    watchdog = ReconWatchdogMiddleware(idle_seconds=300, clock=clock)
    heartbeat = tmp_path / "research/vox/heartbeat"
    heartbeat.parent.mkdir(parents=True)
    heartbeat.touch()
    state = {"messages": [HumanMessage("go")], "recon_last_vox_at": 1000.0, "thread_data": {"workspace_path": str(tmp_path)}}
    clock.now = 1300.0
    os.utime(heartbeat, (1250.0, 1250.0))
    assert watchdog.before_model(state, None) is None
    os.utime(heartbeat, (900.0, 900.0))
    nudge = watchdog.before_model(state, None)["messages"][0]
    assert "ТОЛЬКО если уверен, что рекон реально завершён" in nudge.content


def test_reply_without_tool_calls_cannot_end_the_run_during_recon():
    clock = Clock()
    watchdog = ReconWatchdogMiddleware(idle_seconds=300, clock=clock)
    final = AIMessage(content="Продолжаю рекон.")
    update = watchdog.after_model({"messages": [final], "recon_last_vox_at": 900.0}, None)
    assert update["jump_to"] == "model"
    assert "stop_recon_phase" in update["messages"][0].content
    assert watchdog.after_model({"messages": [final], "recon_last_vox_at": 900.0, "recon_stopped": True}, None) is None
    assert watchdog.after_model({"messages": [final]}, None) is None
