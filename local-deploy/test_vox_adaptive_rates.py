import concurrent.futures
import http.client
import json
import os
import socket
import subprocess
import sys
import time
from email.utils import formatdate
from pathlib import Path
from types import SimpleNamespace

import pytest

from test_vox_relay import MODULE, await_starts, load_relay, relays, request, upstream


def test_production_rate_defaults_only():
    expected = dict(spacing=0.25, cooldown=60.0, backoff_cap=900.0, queue_wait=120.0,
                    request_deadline=700.0, socket_idle=630.0, client_timeout=10.0,
                    max_clients=16, max_inflight=5)
    assert load_relay().DEFAULTS == expected


def test_five_full_streams_share_admission_with_sixth_queued(relays, upstream):
    server = relays(spacing=0.25, max_inflight=5, queue_wait=5, request_deadline=6, socket_idle=6)
    connections = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
        try:
            for _ in range(5):
                connection = http.client.HTTPConnection(*server.server_address, timeout=6)
                connection.request('GET', '/search/text?query=hold&stream=true')
                response = connection.getresponse()
                assert response.readline() == b'{"point":0}\n'
                connections.append((connection, response))
            sixth = pool.submit(request, server)
            time.sleep(0.35)
            assert len(upstream.starts) == 5
            assert upstream.maximum == 5
            assert not sixth.done()
            assert all(b - a >= 0.24 for a, b in zip(upstream.starts, upstream.starts[1:]))
        finally:
            upstream.release.set()
            for connection, response in connections:
                assert response.read() == b'{"messages":[]}\n'
                connection.close()
        assert sixth.result(timeout=3)[0] == 200
    assert upstream.maximum == 5
    assert upstream.starts[5] >= min(upstream.ends)


@pytest.mark.parametrize('retry_after,minimum', [(None, 2.0), ('invalid', 2.0), ('0.2', 0.2), ('1', 1.0), ('date', 0.8)])
def test_429_uses_source_wait_not_non429_backoff(relays, upstream, retry_after, minimum):
    upstream.status = 429
    upstream.retry_after = formatdate(time.time() + 2, usegmt=True) if retry_after == 'date' else retry_after
    server = relays(spacing=0.25, cooldown=60, max_inflight=5, queue_wait=3)
    before = time.time()
    status, headers, _ = request(server)
    assert status == 429
    state = server.gate.read()
    assert state['gap'] == 0.5
    assert state['success_streak'] == 0
    assert state['failures'] == 0
    assert before + minimum <= state['cooldown_until'] < before + 3
    assert 1 <= int(headers['Retry-After']) <= 3
    assert len(upstream.starts) == 1
    server.shutdown()
    server.server_close()
    restarted = relays(spacing=0.25, cooldown=60, max_inflight=5, queue_wait=3)
    assert restarted.gate.read() == state
    assert request(restarted)[0] == 200
    assert upstream.starts[1] - upstream.starts[0] >= max(minimum, 0.5) - 0.02


def test_success_streak_gap_cap_floor_and_overlap_cooldown(tmp_path):
    module = load_relay()
    gate = module.Gate(tmp_path / 'state', 0.25, 60, 900)
    for expected in (0.5, 1.0, 2.0, 4.0, 5.0, 5.0):
        gate.finish(True, '3', rate_limited=True)
        assert gate.read()['gap'] == expected
    cooldown = gate.read()['cooldown_until']
    for _ in range(49):
        gate.finish(False)
    assert gate.read()['gap'] == 5
    assert gate.read()['success_streak'] == 49
    assert gate.read()['cooldown_until'] == cooldown
    gate.finish(False)
    assert gate.read()['gap'] == 4
    assert gate.read()['success_streak'] == 0
    for _ in range(700):
        gate.finish(False)
    assert gate.read()['gap'] == 0.25
    gate.finish(True)
    assert gate.read()['success_streak'] == 0
    assert gate.read()['failures'] == 1
    assert gate.read()['cooldown_until'] >= time.time() + 59


def test_fifty_actual_http_successes_reduce_gap(relays, upstream):
    upstream.status, upstream.retry_after = 429, '0'
    server = relays(spacing=0.25, max_inflight=5, queue_wait=3)
    assert request(server)[0] == 429
    for _ in range(49):
        assert request(server)[0] == 200
    assert server.gate.read()['gap'] == 0.5
    assert server.gate.read()['success_streak'] == 49
    assert request(server)[0] == 200
    assert server.gate.read()['gap'] == 0.4
    assert server.gate.read()['success_streak'] == 0
    assert len(upstream.starts) == 51
    assert all(b - a >= 0.49 for a, b in zip(upstream.starts, upstream.starts[1:]))


def test_v1_migration_preserves_existing_cooldown_and_counters(tmp_path):
    module = load_relay()
    directory = tmp_path / 'state'
    directory.mkdir()
    (directory / 'gate.lock').touch()
    old = dict(version=1, last_start=time.time(), cooldown_until=time.time()+123, failures=3, requests=42)
    (directory / 'state.json').write_text(json.dumps(old))
    gate = module.Gate(directory, 0.25, 60, 900)
    state = gate.read()
    assert state['version'] == 2
    assert state['gap'] == 0.25
    assert state['success_streak'] == 0
    for name in ('last_start', 'cooldown_until', 'failures', 'requests'):
        assert state[name] == old[name]
    assert json.loads((directory / 'state.json').read_text()) == state


def test_parallel_success_does_not_cancel_another_requests_cooldown(relays, upstream):
    server = relays(spacing=0.25, max_inflight=5, cooldown=60)
    connection = http.client.HTTPConnection(*server.server_address, timeout=3)
    connection.request('GET', '/search/text?query=hold')
    response = connection.getresponse()
    assert response.readline() == b'{"point":0}\n'
    server.gate.finish(True, '2', rate_limited=True)
    before = server.gate.read()['cooldown_until']
    upstream.release.set()
    response.read()
    connection.close()
    assert server.gate.read()['cooldown_until'] == before
    assert server.gate.read()['gap'] == 0.5


def test_real_cli_concurrent_no_duplicate_gate_no_retries(relays, upstream, tmp_path):
    server = relays(spacing=0.25, max_inflight=5, queue_wait=5, request_deadline=6, socket_idle=6)
    script = MODULE.parents[1] / 'skills/public/telegram-research/scripts/vox.py'
    env = dict(os.environ, HOME=str(tmp_path), VOX_RELAY_URL=f'http://127.0.0.1:{server.server_port}', VOX_ADMISSION_DIR=str(tmp_path/'unused-gate'))
    env.pop('VOX_API_KEY', None)
    processes = []
    try:
        for index in range(5):
            processes.append(subprocess.Popen([sys.executable, str(script), 'search-page', 'hold', '--output', str(tmp_path/f'{index}.json')], env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True))
        await_starts(upstream, 5)
        assert upstream.maximum == 5
        assert upstream.starts[4] - upstream.starts[0] < 2
        upstream.release.set()
        for index, process in enumerate(processes):
            stdout, stderr = process.communicate(timeout=5)
            assert process.returncode == 0, stderr
            assert json.loads((tmp_path/f'{index}.json').read_text())['messages'] == []
        assert not (tmp_path/'unused-gate').exists()
        assert len(upstream.starts) == 5
    finally:
        upstream.release.set()
        for process in processes:
            if process.poll() is None:
                process.kill()
                process.communicate(timeout=3)


def test_two_processes_share_five_slots_and_release_on_death(tmp_path, upstream):
    key = tmp_path/'key'
    key.write_text('synthetic-only-key')
    code = (
        'import http.client,importlib.util;'
        f's=importlib.util.spec_from_file_location("relay",{str(MODULE)!r});'
        'm=importlib.util.module_from_spec(s);s.loader.exec_module(m);'
        'T=type("T",(m.HTTPSUpstream,),{"__init__":lambda self,timeout:'
        f'setattr(self,"connection",http.client.HTTPConnection({upstream.address[0]!r},{upstream.address[1]},timeout=timeout))}});'
        f'v=m.RelayServer(("127.0.0.1",0),key_file={str(key)!r},state_dir={str(tmp_path/"state")!r},'
        'transport_factory=T,spacing=.25,max_inflight=5,queue_wait=4,request_deadline=6,socket_idle=6);'
        'print(v.server_port,flush=True);v.serve_forever()'
    )
    processes, connections = [], []
    try:
        for _ in range(2):
            process = subprocess.Popen([sys.executable, '-c', code], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            processes.append(process)
            line = process.stdout.readline()
            assert line.strip().isdigit(), line
            process.address = ('127.0.0.1', int(line))
        for index in range(5):
            connection = http.client.HTTPConnection(*processes[index%2].address, timeout=5)
            connection.request('GET','/search/text?query=silent')
            response = connection.getresponse()
            assert response.readline() == b'{"point":0}\n'
            connections.append((connection,response))
        assert upstream.maximum == 5
        with concurrent.futures.ThreadPoolExecutor() as pool:
            sixth = pool.submit(request, SimpleNamespace(server_address=processes[1].address))
            time.sleep(0.35)
            assert len(upstream.starts) == 5
            processes[0].kill()
            processes[0].communicate(timeout=3)
            assert sixth.result(timeout=3)[0] == 200
        assert upstream.maximum <= 5
        assert len(upstream.starts) == 6
        assert all(b-a >= 0.24 for a,b in zip(upstream.starts,upstream.starts[1:]))
    finally:
        for connection, response in connections:
            connection.close()
            response.close()
        for process in processes:
            if process.poll() is None:
                process.kill()
            process.communicate(timeout=3)
