import asyncio
from pathlib import Path
from typing import NotRequired

import pytest
from langchain.agents import AgentState, create_agent
from langchain.agents.middleware import AgentMiddleware
from langchain_core.language_models.fake_chat_models import FakeMessagesListChatModel
from langchain_core.messages import AIMessage, HumanMessage

from deerflow.agents.middlewares.deep_stage_middleware import CHECK_SCRIPT, DEEP_STAGE_HINT, DEEP_STAGE_TEXT, MAX_RETURNS, REPORT_CHECK, DeepStageMiddleware
from deerflow.agents.middlewares.recon_watchdog_middleware import ReconWatchdogMiddleware
from deerflow.agents.thread_state import ThreadDataState
from deerflow.config.app_config import AppConfig


@pytest.fixture
def thread(tmp_path):
    """A thread folder with the research selection; the first report is written by the test."""
    workspace, outputs = tmp_path / "workspace", tmp_path / "outputs"
    (workspace / "research").mkdir(parents=True)
    outputs.mkdir()
    (workspace / "research/messages_filtered.json").write_text('{"messages": []}')
    return {"workspace_path": str(workspace), "outputs_path": str(outputs)}


SKILLS = Path(__file__).resolve().parents[2] / "skills"
BODY = "Агентства берут долю и ведут страницу модели по договору. " * 160


def good_report(thread, name="report.md"):
    """A report in the form, with the facts table and questions.md it is checked against."""
    research = Path(thread["workspace_path"], "research")
    (research / "report").mkdir(exist_ok=True)
    facts = "## Технические данные\n\n| Показатель | Значение |\n|---|---:|\n| Сообщений в пуле | 10 |\n"
    (research / "report/facts.md").write_text(facts, encoding="utf-8")
    titles = [f"Как работает часть рынка номер {chr(1040 + i)}" for i in range(10)]
    (research / "questions.md").write_text("".join(f"## {i:02d}-part | {t}\n- вопрос\n\n" for i, t in enumerate(titles, 1)), encoding="utf-8")
    sections = "".join(f"## {t}\n\n{BODY}\n\n### Чего не видно в данных\n\n- Сроков выплат никто не публикует.\n\n" for t in titles)
    Path(thread["outputs_path"], name).write_text(f"{facts}\n## Общее резюме\n\n1. **Агентства берут половину дохода.**\n\n{sections}", encoding="utf-8")


def state(thread, last, **extra):
    return {"messages": [HumanMessage("Как устроен рынок вахтовой работы?"), last], "thread_data": thread, **extra}


def test_finished_report_sends_the_deep_stage_request(thread):
    middleware = DeepStageMiddleware()
    final = AIMessage(content="Отчёт готов.")
    assert middleware.after_model(state(thread, final), None) is None
    Path(thread["outputs_path"], "report.md").write_text("# Отчёт")
    update = middleware.after_model(state(thread, final), None)
    assert update["jump_to"] == "model" and update["deep_stage_requested"] is True
    message = update["messages"][0]
    assert isinstance(message, HumanMessage) and message.content == DEEP_STAGE_TEXT and not message.additional_kwargs


def test_the_request_is_sent_once_per_thread(thread):
    Path(thread["outputs_path"], "report.md").write_text("# Отчёт")
    final = AIMessage(content="Глубокий отчёт готов.")
    assert DeepStageMiddleware().after_model(state(thread, final, deep_stage_requested=True), None) is None


def test_no_request_when_someone_already_asked_for_it(thread):
    Path(thread["outputs_path"], "report.md").write_text("# Отчёт")
    asked = {"messages": [HumanMessage("Как устроен рынок вахтовой работы?"), HumanMessage(DEEP_STAGE_TEXT), AIMessage(content="Глубокий отчёт готов.")], "thread_data": thread}
    assert DeepStageMiddleware().after_model(asked, None) is None


def test_no_request_mid_turn_or_while_recon_holds_the_run(thread):
    Path(thread["outputs_path"], "report.md").write_text("# Отчёт")
    middleware = DeepStageMiddleware()
    working = AIMessage(content="", tool_calls=[{"name": "bash", "args": {"command": "ls"}, "id": "c1"}])
    assert middleware.after_model(state(thread, working), None) is None
    final = AIMessage(content="Готово.")
    assert middleware.after_model(state(thread, final, recon_last_vox_at=1.0), None) is None
    assert middleware.after_model(state(thread, final, recon_last_vox_at=1.0, recon_stopped=True), None)["deep_stage_requested"] is True


def test_reports_outside_telegram_research_are_ignored(tmp_path):
    outputs = tmp_path / "outputs"
    outputs.mkdir()
    (outputs / "report.md").write_text("# Some other report")
    thread = {"workspace_path": str(tmp_path / "workspace"), "outputs_path": str(outputs)}
    assert DeepStageMiddleware().after_model(state(thread, AIMessage(content="Готово.")), None) is None
    assert DeepStageMiddleware().after_model({"messages": [AIMessage(content="Готово.")]}, None) is None


def test_async_hook_matches_sync(thread):
    Path(thread["outputs_path"], "report.md").write_text("# Отчёт")
    update = asyncio.run(DeepStageMiddleware().aafter_model(state(thread, AIMessage(content="Отчёт готов.")), None))
    assert update["messages"][0].content == DEEP_STAGE_TEXT


def test_disabled_unless_configured():
    assert AppConfig.model_fields["deep_stage"].default_factory().enabled is False


class _Model(FakeMessagesListChatModel):
    def bind_tools(self, tools, **kwargs):
        return self


class _ThreadDataState(AgentState):
    thread_data: NotRequired[ThreadDataState]


class _ThreadData(AgentMiddleware[_ThreadDataState]):
    """Stands in for ThreadDataMiddleware: declares the thread_data channel."""

    state_schema = _ThreadDataState


def test_agent_graph_goes_on_with_the_deep_stage_request(thread):
    Path(thread["outputs_path"], "report.md").write_text("# Отчёт")
    model = _Model(responses=[AIMessage(content="Отчёт готов."), AIMessage(content="Глубокий отчёт готов.")])
    graph = create_agent(model=model, tools=[], middleware=[_ThreadData(), ReconWatchdogMiddleware(idle_seconds=300), DeepStageMiddleware()])
    result = graph.invoke({"messages": [HumanMessage("Как устроен рынок вахтовой работы?")], "thread_data": thread, "recon_last_vox_at": 1.0, "recon_stopped": True}, config={"recursion_limit": 20})
    contents = [(type(m).__name__, m.content) for m in result["messages"]]
    assert contents[1:3] == [("AIMessage", "Отчёт готов."), ("HumanMessage", DEEP_STAGE_TEXT)]
    assert result["messages"][3].additional_kwargs[DEEP_STAGE_HINT] is True and "SKILL.md" in result["messages"][3].content
    assert contents[4:] == [("AIMessage", "Глубокий отчёт готов.")]
    assert result["deep_stage_requested"] is True


def test_a_deep_stage_request_gets_the_skill_hint_once(thread):
    middleware = DeepStageMiddleware()
    asked = {"messages": [HumanMessage(DEEP_STAGE_TEXT)], "thread_data": thread}
    hint = middleware.before_model(asked, None)["messages"][0]
    assert hint.additional_kwargs[DEEP_STAGE_HINT] is True and "Глубокое исследование" in hint.content
    assert middleware.before_model({**asked, "messages": [HumanMessage(DEEP_STAGE_TEXT), hint]}, None) is None
    assert middleware.before_model({"messages": [HumanMessage(DEEP_STAGE_TEXT)], "thread_data": {"workspace_path": "/nowhere"}}, None) is None
    assert asyncio.run(middleware.abefore_model(asked, None))["messages"][0].content == hint.content


def checked():
    return DeepStageMiddleware(check_script=SKILLS / CHECK_SCRIPT)


def test_a_report_out_of_form_goes_back_with_its_problems_and_no_deep_stage(thread):
    Path(thread["outputs_path"], "report.md").write_text("# Отчёт\n\n## Источники\n", encoding="utf-8")
    update = checked().after_model(state(thread, AIMessage(content="Отчёт готов.")), None)
    message = update["messages"][0]
    assert update["jump_to"] == "model" and update["report_check_returns"] == 1 and "deep_stage_requested" not in update
    assert message.additional_kwargs[REPORT_CHECK] is True and "check.py /mnt/user-data/outputs/report.md" in message.content
    assert "report/facts.md нет" in message.content


def test_a_report_in_form_starts_the_deep_stage(thread):
    good_report(thread)
    update = checked().after_model(state(thread, AIMessage(content="Отчёт готов."), report_check_returns=2), None)
    assert update["messages"][0].content == DEEP_STAGE_TEXT and update["deep_stage_requested"] is True and update["report_check_returns"] == 0


def test_the_deep_report_is_checked_once_the_deep_stage_was_asked_for(thread):
    good_report(thread)
    update = checked().after_model(state(thread, AIMessage(content="Глубокий отчёт готов."), deep_stage_requested=True), None)
    assert "deep-report.md: файла нет" in update["messages"][0].content
    good_report(thread, "deep-report.md")
    update = checked().after_model(state(thread, AIMessage(content="Глубокий отчёт готов."), deep_stage_requested=True), None)
    assert "разобрано историй: 0, нужно не меньше 100" in update["messages"][0].content


def test_the_check_gives_up_after_max_returns(thread):
    Path(thread["outputs_path"], "report.md").write_text("# Отчёт", encoding="utf-8")
    assert checked().after_model(state(thread, AIMessage(content="Отчёт готов."), report_check_returns=MAX_RETURNS), None) is None


def test_the_async_check_matches_sync(thread):
    Path(thread["outputs_path"], "report.md").write_text("# Отчёт", encoding="utf-8")
    update = asyncio.run(checked().aafter_model(state(thread, AIMessage(content="Отчёт готов.")), None))
    assert update["messages"][0].additional_kwargs[REPORT_CHECK] is True
