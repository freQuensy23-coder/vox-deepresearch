import importlib.util
from pathlib import Path

import pytest


spec = importlib.util.spec_from_file_location('relay_backoff_contract', Path(__file__).with_name('vox_relay.py'))
assert spec is not None and spec.loader is not None
relay = importlib.util.module_from_spec(spec)
spec.loader.exec_module(relay)


def test_production_backoff_ladder_persists_and_success_resets(tmp_path, monkeypatch):
    now = [1800000000.0]
    monkeypatch.setattr(relay.time, 'time', lambda: now[0])
    gate = relay.Gate(tmp_path / 'state', 5, 60, 900)
    delays = []
    for expected in (60, 120, 240, 480, 900, 900):
        fd = gate.acquire(1)
        gate.finish(True)
        state = gate.read()
        delays.append(state['cooldown_until'] - now[0])
        gate.release(fd)
        gate = relay.Gate(tmp_path / 'state', 5, 60, 900)
        assert gate.read() == state
        assert delays[-1] == expected
        now[0] = state['cooldown_until'] + 1
    fd = gate.acquire(1)
    gate.finish(False)
    gate.release(fd)
    assert gate.read()['failures'] == 0
    assert gate.read()['cooldown_until'] == 0
    assert gate.read()['requests'] == 7


def test_retry_after_beyond_cap_survives_new_gate(tmp_path, monkeypatch):
    now = 1800000000.0
    monkeypatch.setattr(relay.time, 'time', lambda: now)
    gate = relay.Gate(tmp_path / 'state', 5, 60, 900)
    fd = gate.acquire(1)
    gate.finish(True, '3600')
    gate.release(fd)
    recreated = relay.Gate(tmp_path / 'state', 5, 60, 900)
    assert recreated.retry_after() == 3600
    with pytest.raises(relay.Busy):
        recreated.acquire(0.01)
    assert recreated.read()['requests'] == 1


def test_busy_wait_does_not_consume_request_or_failure_budget(tmp_path, monkeypatch):
    now = 1800000000.0
    monkeypatch.setattr(relay.time, 'time', lambda: now)
    gate = relay.Gate(tmp_path / 'state', 5, 60, 900)
    fd = gate.acquire(1)
    gate.finish(True)
    gate.release(fd)
    before = gate.read()
    for _ in range(3):
        with pytest.raises(relay.Busy):
            gate.acquire(0.01)
    assert gate.read() == before
