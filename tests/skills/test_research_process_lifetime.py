from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_research_skill_requires_managed_background_collection():
    text = (ROOT / "skills/public/telegram-research/references/vox-tools.md").read_text(
        encoding="utf-8"
    )
    for required in (
        "background=true",
        "10 секунд",
        "status=completed",
        "status=running",
        "session_id",
        "wait_seconds=60",
        "cancel=true",
        "--resume",
        "manifest",
    ):
        assert required in text
    assert "Завершение команды не означает завершение исследования" in text


def test_reference_tracks_commands_until_the_agent_finishes():
    text = (ROOT / "skills/public/telegram-research/references/vox-tools.md").read_text(
        encoding="utf-8"
    )
    assert "останавливает оставшиеся команды" in text
    assert "перед итоговым ответом собери нужные" in text
    assert "Для этого режима не добавляй `&` или" in text
    assert "без отдельного общего таймаута" in text
