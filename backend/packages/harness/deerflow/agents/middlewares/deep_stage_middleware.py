"""Start the deep stage of Telegram research once the first report is ready.

The first report of the telegram-research skill is ``outputs/report.md`` of a
thread whose workspace holds ``research/messages_filtered.json``. When the lead
agent ends its turn (a reply without tool calls) and both files exist, the thread
gets the user message ``DEEP_STAGE_TEXT`` and the agent goes on with it. This
happens once per thread (not at all if someone already sent that message), and
never while the recon watchdog still holds the run.
"""

import asyncio
from pathlib import Path
from typing import NotRequired

from langchain.agents import AgentState
from langchain.agents.middleware import AgentMiddleware, hook_config
from langchain_core.messages import AIMessage, HumanMessage

DEEP_STAGE_TEXT = "Теперь сделай более глубокое исследование"
REPORT = "report.md"
SELECTION = "research/messages_filtered.json"


class DeepStageState(AgentState):
    deep_stage_requested: NotRequired[bool]


def _report_ready(thread_data) -> bool:
    outputs, workspace = (thread_data or {}).get("outputs_path"), (thread_data or {}).get("workspace_path")
    return bool(outputs and workspace) and (Path(outputs) / REPORT).is_file() and (Path(workspace) / SELECTION).is_file()


def _turn_ends(state) -> bool:
    messages = state.get("messages") or []
    last = messages[-1] if messages else None
    if not isinstance(last, AIMessage) or last.tool_calls or state.get("deep_stage_requested"):
        return False
    if any(isinstance(m, HumanMessage) and m.text.strip() == DEEP_STAGE_TEXT for m in messages):
        return False
    # The recon watchdog sends a reply without tool calls back to work while recon is on.
    return state.get("recon_last_vox_at") is None or bool(state.get("recon_stopped"))


class DeepStageMiddleware(AgentMiddleware[DeepStageState]):
    state_schema = DeepStageState

    def _request(self):
        return {"messages": [HumanMessage(content=DEEP_STAGE_TEXT)], "deep_stage_requested": True, "jump_to": "model"}

    @hook_config(can_jump_to=["model"])
    def after_model(self, state, runtime):
        if _turn_ends(state) and _report_ready(state.get("thread_data")):
            return self._request()
        return None

    @hook_config(can_jump_to=["model"])
    async def aafter_model(self, state, runtime):
        if _turn_ends(state) and await asyncio.to_thread(_report_ready, state.get("thread_data")):
            return self._request()
        return None
