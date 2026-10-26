"""Keep the lead agent sending Vox requests during Telegram recon.

The watchdog starts with the first ``vox.py search`` / ``vox.py whale`` bash
command of the thread. If no new one follows within ``idle_seconds`` and Vox is
idle too (``vox.py`` keeps ``research/vox/heartbeat`` fresh while a command runs),
the next model call gets a reminder to keep reconning wide. While recon is on, a
reply without tool calls does not end the run: it would kill the background
searches, so the agent is reminded and sent back to work. The agent turns the
watchdog off with ``stop_recon_phase`` once the whole market is mapped; the tool
refuses while the research folder shows recon unfinished.
"""

import asyncio
import json
import time
from pathlib import Path
from typing import NotRequired

from langchain.agents import AgentState
from langchain.agents.middleware import AgentMiddleware, hook_config
from langchain.tools import ToolRuntime, tool
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
from langgraph.types import Command

from deerflow.config.recon_watchdog_config import ReconWatchdogConfig

RECON_NUDGE = "deerflow_recon_nudge"
HEARTBEAT = "research/vox/heartbeat"
REGISTRY = "research/registry.json"
SELECTION = "research/messages_filtered.json"
MIN_ON_TOPIC = 2000

NUDGE_TEXT = (
    "Слышь, почему ты не реконишь? Больше {minutes} мин без новых запросов к Vox. "
    "Надо слать запросы постоянно и реконить вширь: новые слова, синонимы, жаргон, киты. "
    "Ожидание других команд — не отговорка: запускай vox.py через bash(background=True) "
    "и продолжай. Если фаза рекона полностью завершена — фактуры хватает для ответа на каждую часть "
    "вопроса пользователя и новые круги не дают новых групп и слов — выключи напоминание "
    "через stop_recon_phase, но ТОЛЬКО если уверен, что рекон реально завершён."
)

FINISH_TEXT = (
    "Рекон не выключен, поэтому завершать работу нельзя: конец хода остановит все фоновые "
    "поиски. Продолжай: запускай новые запросы, проверяй сессии, разбирай собранное. "
    "Если фактуры хватает для ответа и новых групп и слов нет — сначала вызови stop_recon_phase, потом отвечай."
)

REFUSAL_TEXT = "Рекон не закончен, напоминание не выключено. Осталось:\n{left}\nПродолжай рекон и вызови stop_recon_phase, когда этого не останется."


class ReconWatchdogState(AgentState):
    recon_last_vox_at: NotRequired[float]
    recon_stopped: NotRequired[bool]
    recon_stop_refused_at: NotRequired[dict]


def _is_vox_request(call) -> bool:
    if call.get("name") != "bash":
        return False
    command = str((call.get("args") or {}).get("command") or "")
    return "vox.py" in command and (" search" in command or " whale" in command)


def _recon_left(workspace, refused_at):
    """What recon still has to do according to the research folder, and the yield to remember for the next try.

    Few on-topic messages block the stop until a round of new keywords after the last
    refusal added none: then the topic has no more to give."""
    registry_path = Path(workspace) / REGISTRY if workspace else None
    if registry_path is None or not registry_path.is_file():
        return [], None
    registry = json.loads(registry_path.read_text(encoding="utf-8"))
    keywords, whales = registry.get("keywords", []), registry.get("whales", [])
    left = []
    todo = [k["word"] for k in keywords if k.get("status") == "todo"]
    if todo:
        left.append(f"непрогнанные ключевые слова ({len(todo)}): " + ", ".join(todo[:30]) + (" …" if len(todo) > 30 else ""))
    if not whales:
        left.append("шаг с китами не делался: подагент по whales-agent.md, затем vox.py whale для каждого кита")
    elif todo_whales := [str(w.get("user_alias") or w["user_id"]) for w in whales if w.get("status") == "todo"]:
        left.append(f"киты без сообщений ({len(todo_whales)}): " + ", ".join(todo_whales[:30]))
    selection = Path(workspace) / SELECTION
    on_topic = len(json.loads(selection.read_text(encoding="utf-8"))["messages"]) if selection.is_file() else 0
    searched = sum(k.get("status") == "searched" for k in keywords)
    yield_now = {"on_topic": on_topic, "searched": searched}
    if on_topic < MIN_ON_TOPIC:
        exhausted = refused_at is not None and searched > refused_at["searched"] and on_topic <= refused_at["on_topic"]
        if not exhausted:
            left.append(f"по теме {on_topic} сообщений, нужно больше {MIN_ON_TOPIC}: новые слова и киты, затем jev.py filter")
    return left, yield_now


@tool("stop_recon_phase", parse_docstring=True)
def stop_recon_phase_tool(reason: str, runtime: ToolRuntime) -> Command:
    """Turn off the reminder to keep sending Vox requests. Call it only when the
    recon phase is fully done: the collected messages answer every part of the
    user's question and the last round found no new groups, keywords or whales.
    It refuses while keywords or whales are left or few messages are on topic.

    Args:
        reason: Why recon is complete: which parts of the question the data answers and why new queries add nothing.
    """
    state = runtime.state or {}
    left, yield_now = _recon_left((state.get("thread_data") or {}).get("workspace_path"), state.get("recon_stop_refused_at"))
    if left:
        refusal = ToolMessage(REFUSAL_TEXT.format(left="\n".join(f"- {item}" for item in left)), tool_call_id=runtime.tool_call_id)
        return Command(update={"recon_stop_refused_at": yield_now, "messages": [refusal]})
    return Command(update={"recon_stopped": True, "messages": [ToolMessage(f"Recon phase stopped: {reason}", tool_call_id=runtime.tool_call_id)]})


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

    def _vox_busy(self, state, now) -> bool:
        workspace = (state.get("thread_data") or {}).get("workspace_path")
        if not workspace:
            return False
        try:
            return now - (Path(workspace) / HEARTBEAT).stat().st_mtime < self.idle_seconds
        except OSError:
            return False

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
        if now - last_vox < self.idle_seconds or self._vox_busy(state, now):
            return None
        nudge = HumanMessage(content=NUDGE_TEXT.format(minutes=self.idle_seconds // 60), additional_kwargs={RECON_NUDGE: True})
        # Restart the timer so the reminder repeats only after another idle period.
        return {"messages": [nudge], "recon_last_vox_at": now}

    @hook_config(can_jump_to=["model"])
    async def aafter_model(self, state, runtime):
        return self.after_model(state, runtime)

    async def abefore_model(self, state, runtime):
        return await asyncio.to_thread(self.before_model, state, runtime)
