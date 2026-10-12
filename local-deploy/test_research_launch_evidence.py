import importlib.util
import json
from pathlib import Path

import pytest


@pytest.fixture
def capture():
    path = Path(__file__).with_name('research_launch_evidence.py')
    assert path.exists(), 'prelaunch evidence capture missing'
    spec = importlib.util.spec_from_file_location('research_launch_evidence', path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.capture_empty_start


def test_captures_empty_thread_before_run_id_exists(capture, tmp_path):
    output = tmp_path / 'before.json'
    result = capture('thread-exact', tmp_path / 'new-data', [], [], output)
    assert result['thread_id'] == 'thread-exact'
    assert 'run_id' not in result
    assert result['entries'] == result['prior_run_ids'] == result['prior_message_ids'] == []
    assert json.loads(output.read_text()) == result


@pytest.mark.parametrize('runs,messages', [([{'run_id': 'old'}], []), ([], [{'id': 'message'}]), ({}, []), ([], {})])
def test_refuses_existing_or_unparsed_history(capture, tmp_path, runs, messages):
    with pytest.raises(ValueError):
        capture('thread-exact', tmp_path / 'new-data', runs, messages, tmp_path / 'before.json')
    assert not (tmp_path / 'before.json').exists()


def test_refuses_nonempty_evidence(capture, tmp_path):
    data = tmp_path / 'data'
    data.mkdir()
    (data / 'raw.json').write_text('{}')
    with pytest.raises(ValueError):
        capture('thread-exact', data, [], [], tmp_path / 'before.json')


def test_refuses_symlinked_directory(capture, tmp_path):
    target = tmp_path / 'target'
    target.mkdir()
    data = tmp_path / 'data'
    data.symlink_to(target, target_is_directory=True)
    with pytest.raises(ValueError):
        capture('thread-exact', data, [], [], tmp_path / 'before.json')


def test_does_not_overwrite_receipt(capture, tmp_path):
    output = tmp_path / 'before.json'
    output.write_text('preserved')
    with pytest.raises(FileExistsError):
        capture('thread-exact', tmp_path / 'new-data', [], [], output)
    assert output.read_text() == 'preserved'
