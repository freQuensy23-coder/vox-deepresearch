from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def test_research_skill_requires_bounded_foreground_collection():
    text = (ROOT / 'skills/public/telegram-research/SKILL.md').read_text()
    assert '### Жизненный цикл сборщика' in text
    section = text.split('### Жизненный цикл сборщика', 1)[1].split('\n### ', 1)[0]
    for required in ('foreground', '--max-pages 1', '--resume', 'nohup', '900', 'sleep', 'manifest'):
        assert required in section
    assert 'не запускай' in section
    assert 'не означает завершение исследования' in section


def test_reference_warns_that_detached_process_outlives_graph():
    text = (ROOT / 'skills/public/telegram-research/references/vox-tools.md').read_text()
    assert 'Detached shell processes can outlive the agent graph' in text
    assert 'Do not use shell backgrounding' in text
    assert '--max-pages 1' in text
