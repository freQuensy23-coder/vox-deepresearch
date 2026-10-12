import concurrent.futures
import time

from test_vox_relay import relays, request, upstream


def test_two_clients_use_real_five_second_spacing(relays, upstream):
    server = relays(spacing=5.0, queue_wait=8.0, request_deadline=10.0)
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: request(server, timeout=12)[0], range(2)))
    assert results == [200, 200]
    assert len(upstream.starts) == 2
    assert upstream.maximum == 1
    assert upstream.starts[1] - upstream.starts[0] >= 4.95
    assert upstream.starts[1] >= upstream.ends[0]


def test_http503_waits_real_sixty_seconds_before_next_upstream(relays, upstream):
    server = relays(spacing=5.0, cooldown=60.0, backoff_cap=900.0, queue_wait=65.0)
    upstream.status = 503
    first_status, headers, _ = request(server)
    assert first_status == 503
    assert int(headers['Retry-After']) >= 59
    state = server.gate.read()
    assert state['failures'] == 1
    remaining = state['cooldown_until'] - time.time()
    assert remaining > 58
    time.sleep(remaining + 0.02)
    assert request(server)[0] == 200
    assert len(upstream.starts) == 2
    assert upstream.starts[1] - upstream.ends[0] >= 59.9
    assert upstream.maximum == 1
    assert server.gate.read()['failures'] == 0
