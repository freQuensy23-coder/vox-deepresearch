from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

from deerflow.agents.middlewares.recon_watchdog_middleware import RECON_NUDGE, ReconWatchdogMiddleware, stop_recon_phase_tool


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


def test_stop_recon_phase_sets_state():
    command = stop_recon_phase_tool.invoke({"type": "tool_call", "name": "stop_recon_phase", "id": "t1", "args": {"reason": "all mapped"}})
    assert command.update["recon_stopped"] is True
    assert isinstance(command.update["messages"][0], ToolMessage)


def test_reply_without_tool_calls_cannot_end_the_run_during_recon():
    clock = Clock()
    watchdog = ReconWatchdogMiddleware(idle_seconds=300, clock=clock)
    final = AIMessage(content="Продолжаю рекон.")
    update = watchdog.after_model({"messages": [final], "recon_last_vox_at": 900.0}, None)
    assert update["jump_to"] == "model"
    assert "stop_recon_phase" in update["messages"][0].content
    assert watchdog.after_model({"messages": [final], "recon_last_vox_at": 900.0, "recon_stopped": True}, None) is None
    assert watchdog.after_model({"messages": [final]}, None) is None
