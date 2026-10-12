import importlib.util
from pathlib import Path

import pytest


@pytest.fixture
def observe():
    path = Path(__file__).with_name('research_observations.py')
    assert path.exists(), 'terminal observation adapter missing'
    spec = importlib.util.spec_from_file_location('research_observations', path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.observe


def records():
    return {'thread_id': 't', 'run_id': 'r'}, {'thread_id': 't', 'run_id': 'r', 'status': 'success', 'goal_verdict': {'satisfied': True}}, {'goal': None}, {'values': {'messages': [{'id': 'a', 'type': 'ai', 'content': 'final', 'tool_calls': []}]}}


def test_cleared_goal_requires_saved_terminal_satisfied_verdict(observe):
    identity, run, goal, state = records()
    value = observe(identity, run, goal, state)
    assert value['goal']['status'] == 'satisfied'
    assert value['goal']['evidence_origin'] == 'run.goal_verdict'
    assert value['final_candidate']['message_id'] == 'a'
    assert value['semantic_review_required'] is True


def test_cleared_goal_without_verdict_is_not_satisfied(observe):
    identity, run, goal, state = records()
    del run['goal_verdict']
    assert observe(identity, run, goal, state)['goal']['status'] == 'unverified'


@pytest.mark.parametrize('status', ['error', 'running', 'interrupted'])
def test_non_success_has_no_final_candidate(observe, status):
    identity, run, goal, state = records()
    run['status'] = status
    value = observe(identity, run, goal, state)
    assert value['goal']['status'] != 'satisfied'
    assert value['final_candidate'] is None


def test_active_goal_not_overridden_by_stale_verdict(observe):
    identity, run, goal, state = records()
    goal['goal'] = {'status': 'active', 'continuation_count': 0}
    assert observe(identity, run, goal, state)['goal']['status'] == 'active'


def test_tools_and_progress_are_not_concatenated_as_final(observe):
    identity, run, goal, state = records()
    state['values']['messages'] = [{'type': 'ai', 'content': 'progress', 'tool_calls': [{'name': 'bash'}]}, {'type': 'tool', 'content': 'result'}]
    assert observe(identity, run, goal, state)['final_candidate'] is None


def test_hidden_continuation_and_reasoning_are_not_report_text(observe):
    identity, run, goal, state = records()
    state['values']['messages'][0]['content'] = [{'type': 'thinking', 'text': 'private'}, {'type': 'text', 'text': 'visible'}]
    value = observe(identity, run, goal, state)
    assert value['final_candidate']['text'] == 'visible'


def test_wrong_run_is_rejected(observe):
    identity, run, goal, state = records()
    run['run_id'] = 'other'
    with pytest.raises(ValueError):
        observe(identity, run, goal, state)
