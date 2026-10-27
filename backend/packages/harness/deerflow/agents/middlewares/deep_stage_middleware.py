"""Check Telegram research reports at the end of each turn and start the deep stage.

The first report of the telegram-research skill is ``outputs/report.md`` of a
thread whose workspace holds ``research/messages_filtered.json``. When the lead
agent ends its turn (a reply without tool calls), the report is checked against
the report form with the skill's ``scripts/check.py``. A report that fails, or is
missing once recon is over, sends the agent back with the list of problems; the
deep report
(``outputs/deep-report.md``) is checked the same way once the deep stage was
asked for. A passing first report gets the thread the user message
``DEEP_STAGE_TEXT`` and the agent goes on with it. This happens once per thread
(not at all if someone already sent that message), and never while the recon
watchdog still holds the run. After ``MAX_RETURNS`` failed checks in a row the
turn ends anyway, so a report the agent cannot fix does not loop forever.

Whoever sends ``DEEP_STAGE_TEXT`` in a research thread, the next model call also
gets ``HINT_TEXT``: the skill's deep stage section, which an older thread's
context may not hold.
"""

import asyncio
import importlib.util
import logging
from pathlib import Path
from typing import NotRequired

from langchain.agents import AgentState
from langchain.agents.middleware import AgentMiddleware, hook_config
from langchain_core.messages import AIMessage, HumanMessage

logger = logging.getLogger(__name__)

DEEP_STAGE_TEXT = "Теперь сделай более глубокое исследование"
DEEP_STAGE_HINT = "deerflow_deep_stage_hint"
HINT_TEXT = "Это глубокий этап telegram-research. Перечитай раздел «Глубокое исследование» в /mnt/skills/public/telegram-research/SKILL.md и выполни его сам, без уточняющих вопросов."
REPORT_CHECK = "deerflow_report_check"
CHECK_SCRIPT = "public/telegram-research/scripts/check.py"
CHECK_TEXT = (
    "{name} не прошёл проверку формы отчёта, закончить работу нельзя. Исправь каждый пункт "
    "(разделы возвращай их авторам) и проверь снова: "
    "cd /mnt/user-data/workspace/research && python3 /mnt/skills/public/telegram-research/scripts/check.py /mnt/user-data/outputs/{name}\n"
    "{problems}"
)
MAX_RETURNS = 10
REPORT = "report.md"
DEEP_REPORT = "deep-report.md"
RESEARCH = "research"
SELECTION = "research/messages_filtered.json"


class DeepStageState(AgentState):
    deep_stage_requested: NotRequired[bool]
    report_check_returns: NotRequired[int]


def _selection_ready(thread_data) -> bool:
    workspace = (thread_data or {}).get("workspace_path")
    return bool(workspace) and (Path(workspace) / SELECTION).is_file()


def _asks_for_deep_stage(state) -> bool:
    messages = state.get("messages") or []
    return bool(messages) and isinstance(messages[-1], HumanMessage) and messages[-1].text.strip() == DEEP_STAGE_TEXT


def _deep_stage_asked(state) -> bool:
    return bool(state.get("deep_stage_requested")) or any(isinstance(m, HumanMessage) and m.text.strip() == DEEP_STAGE_TEXT for m in state.get("messages") or [])


def _turn_ends(state) -> bool:
    messages = state.get("messages") or []
    last = messages[-1] if messages else None
    if not isinstance(last, AIMessage) or last.tool_calls:
        return False
    # The recon watchdog sends a reply without tool calls back to work while recon is on.
    return state.get("recon_last_vox_at") is None or bool(state.get("recon_stopped"))


class DeepStageMiddleware(AgentMiddleware[DeepStageState]):
    state_schema = DeepStageState

    def __init__(self, check_script: Path | None = None):
        super().__init__()
        self.check_script = check_script
        self._check = None

    def _problems(self, report: Path, research: Path) -> list[str]:
        if self.check_script is None or not self.check_script.is_file():
            return []
        if self._check is None:
            spec = importlib.util.spec_from_file_location("telegram_research_check", self.check_script)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            self._check = module.problems
        return self._check(report, research)

    def _request(self):
        return {"messages": [HumanMessage(content=DEEP_STAGE_TEXT)], "deep_stage_requested": True, "jump_to": "model"}

    def _hint(self):
        return {"messages": [HumanMessage(content=HINT_TEXT, additional_kwargs={DEEP_STAGE_HINT: True})]}

    def _at_turn_end(self, state):
        thread_data = state.get("thread_data") or {}
        outputs, workspace = thread_data.get("outputs_path"), thread_data.get("workspace_path")
        if not (outputs and workspace) or not (Path(workspace) / SELECTION).is_file():
            return None
        deep = _deep_stage_asked(state)
        report = Path(outputs) / (DEEP_REPORT if deep else REPORT)
        try:
            problems = self._problems(report, Path(workspace) / RESEARCH)
        except Exception:
            logger.exception("Report check failed to run for %s", report)
            problems = []
        if not problems and not report.is_file():
            return None
        returns = state.get("report_check_returns", 0)
        if problems:
            if returns >= MAX_RETURNS:
                return None
            text = CHECK_TEXT.format(name=report.name, problems="\n".join(f"- {p}" for p in problems))
            return {"messages": [HumanMessage(content=text, additional_kwargs={REPORT_CHECK: True})], "report_check_returns": returns + 1, "jump_to": "model"}
        if not deep:
            return {**self._request(), "report_check_returns": 0}
        return {"report_check_returns": 0} if returns else None

    def before_model(self, state, runtime):
        if _asks_for_deep_stage(state) and _selection_ready(state.get("thread_data")):
            return self._hint()
        return None

    async def abefore_model(self, state, runtime):
        if _asks_for_deep_stage(state) and await asyncio.to_thread(_selection_ready, state.get("thread_data")):
            return self._hint()
        return None

    @hook_config(can_jump_to=["model"])
    def after_model(self, state, runtime):
        return self._at_turn_end(state) if _turn_ends(state) else None

    @hook_config(can_jump_to=["model"])
    async def aafter_model(self, state, runtime):
        return await asyncio.to_thread(self._at_turn_end, state) if _turn_ends(state) else None
