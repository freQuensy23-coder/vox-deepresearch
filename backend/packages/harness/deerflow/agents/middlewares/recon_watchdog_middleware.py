"""Keep the lead agent sending Vox requests during Telegram recon.

The watchdog starts with the first ``vox.py search`` / ``vox.py whale`` bash
command of the thread. If no new one follows within ``idle_seconds``, the next
model call gets a reminder to keep reconning wide. While recon is on, a reply
without tool calls does not end the run: it would kill the background searches,
so the agent is reminded and sent back to work. The agent turns the watchdog off
with ``stop_recon_phase`` once the whole market is mapped.
"""

import time
from typing import Annotated, NotRequired

from langchain.agents import AgentState
from langchain.agents.middleware import AgentMiddleware, hook_config
from langchain.tools import InjectedToolCallId, tool
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
from langgraph.types import Command

from deerflow.config.recon_watchdog_config import ReconWatchdogConfig

RECON_NUDGE = "deerflow_recon_nudge"

NUDGE_TEXT = (
    "Слышь, почему ты не реконишь? Больше {minutes} мин без новых запросов к Vox. "
    "Надо слать запросы постоянно и реконить вширь: новые слова, синонимы, жаргон, киты. "
    "Ожидание других команд — не отговорка: запускай vox.py через bash(background=True) "
    "и продолжай. Если фаза рекона полностью завершена, ты замапил ВЕСЬ рынок и просто ждёшь "
    "подагентов или готовишь отчёт — выключи напоминание через stop_recon_phase."
)

FINISH_TEXT = (
    "Рекон не выключен, поэтому завершать работу нельзя: конец хода остановит все фоновые "
    "поиски. Продолжай: запускай новые запросы, проверяй сессии, разбирай собранное. "
    "Если весь рынок замаплен — сначала вызови stop_recon_phase, потом отвечай."
)


class ReconWatchdogState(AgentState):
    recon_last_vox_at: NotRequired[float]
    recon_stopped: NotRequired[bool]


def _is_vox_request(call) -> bool:
    if call.get("name") != "bash":
        return False
    command = str((call.get("args") or {}).get("command") or "")
    return "vox.py" in command and (" search" in command or " whale" in command)


@tool("stop_recon_phase", parse_docstring=True)
def stop_recon_phase_tool(reason: str, tool_call_id: Annotated[str, InjectedToolCallId]) -> Command:
    """Turn off the reminder to keep sending Vox requests. Call it only when the
    recon phase is fully done: the WHOLE market is mapped and you are only waiting
    for subagents or writing the report.

    Args:
        reason: Why recon is complete: what was covered and why new queries add nothing.
    """
    return Command(update={"recon_stopped": True, "messages": [ToolMessage(f"Recon phase stopped: {reason}", tool_call_id=tool_call_id)]})


class ReconWatchdogMiddleware(AgentMiddleware[ReconWatchdogState]):
    state_schema = ReconWatchdogState
    tools = [stop_recon_phase_tool]

    def __init__(self, idle_seconds: int, clock=time.time):
        super().__init__()
        self.idle_seconds = idle_seconds
        self.clock = clock

    @classmethod
    def from_config(cls, config: ReconWatchdogConfig) -> "ReconWatchdogMiddleware":
        return cls(idle_seconds=config.idle_seconds)

    @hook_config(can_jump_to=["model"])
    def after_model(self, state, runtime):
        messages = state.get("messages") or []
        last = messages[-1] if messages else None
        if not isinstance(last, AIMessage):
            return None
        if any(_is_vox_request(call) for call in last.tool_calls):
            return {"recon_last_vox_at": self.clock()}
        if not last.tool_calls and state.get("recon_last_vox_at") is not None and not state.get("recon_stopped"):
            nudge = HumanMessage(content=FINISH_TEXT, additional_kwargs={RECON_NUDGE: True})
            return {"messages": [nudge], "recon_last_vox_at": self.clock(), "jump_to": "model"}
        return None

    def before_model(self, state, runtime):
        last_vox = state.get("recon_last_vox_at")
        if last_vox is None or state.get("recon_stopped"):
            return None
        messages = state.get("messages") or []
        if messages and isinstance(messages[-1], AIMessage) and messages[-1].tool_calls:
            return None
        now = self.clock()
        if now - last_vox < self.idle_seconds:
            return None
        nudge = HumanMessage(content=NUDGE_TEXT.format(minutes=self.idle_seconds // 60), additional_kwargs={RECON_NUDGE: True})
        # Restart the timer so the reminder repeats only after another idle period.
        return {"messages": [nudge], "recon_last_vox_at": now}

    @hook_config(can_jump_to=["model"])
    async def aafter_model(self, state, runtime):
        return self.after_model(state, runtime)

    async def abefore_model(self, state, runtime):
        return self.before_model(state, runtime)
