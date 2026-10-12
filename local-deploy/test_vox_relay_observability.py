import http.client
import json
import threading
import time
import uuid

import pytest

from test_vox_relay import await_starts, relays, request, upstream


def records(capsys):
    out, err = capsys.readouterr()
    assert err == ""
    return [json.loads(line) for line in out.splitlines()]


def completed(server):
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline:
        if server.clients._value == server.options["max_clients"]:
            return
        time.sleep(0.005)
    pytest.fail("relay clients did not finish")


def test_slow_stream_logs_end_after_close_and_preserves_exclusivity(relays, upstream, capsys):
    server = relays(queue_wait=1)
    closed = []
    original = server.transport_factory

    class Tracked(original):
        def close(self):
            super().close()
            closed.append(time.time())

    server.transport_factory = Tracked
    first = http.client.HTTPConnection(*server.server_address, timeout=3)
    first.request("GET", "/search/text?query=hold")
    response = first.getresponse()
    assert response.readline() == b'{"point":0}\n'
    results = []
    worker = threading.Thread(target=lambda: results.append(request(server)))
    worker.start()
    time.sleep(0.12)
    before = records(capsys)
    assert len([r for r in before if r["event"] == "upstream_headers"]) == 1
    assert not any(r["event"] == "end" for r in before)
    assert len(upstream.starts) == 1
    upstream.release.set()
    response.read()
    first.close()
    worker.join(3)
    completed(server)
    all_records = before + records(capsys)
    assert results[0][0] == 200
    assert upstream.maximum == 1
    ids = {r["request_id"] for r in all_records}
    assert len(ids) == 2
    ends = []
    for request_id in ids:
        assert str(uuid.UUID(request_id)) == request_id
        group = [r for r in all_records if r["request_id"] == request_id]
        assert [r["event"] for r in group] == ["queue_entry", "admitted", "upstream_headers", "end"]
        assert all(r["route_class"] == "search_text" for r in group)
        end = group[-1]
        assert end["origin"] == "upstream" and end["status"] == 200
        assert end["total_elapsed"] >= end["queued_elapsed"] + end["upstream_elapsed"]
        assert end["timestamp"] >= min(closed)
        assert end["failures"] == 0 and end["cooldown_until"] == 0
        ends.append(end)
    assert max(r["queued_elapsed"] for r in ends) >= 0.1
    assert max(r["upstream_elapsed"] for r in ends) >= 0.1
    assert len(closed) == 2
    assert all(end["timestamp"] >= close for end, close in zip(sorted(ends, key=lambda r: r["timestamp"]), sorted(closed)))


def test_local_queue_503_is_distinct_from_upstream_503(relays, upstream, capsys):
    upstream.status = 503
    server = relays(cooldown=1, queue_wait=0.05)
    assert request(server)[0] == 503
    assert request(server)[0] == 503
    completed(server)
    rows = records(capsys)
    ends = [r for r in rows if r["event"] == "end"]
    assert [r["origin"] for r in ends] == ["upstream", "local_queue"]
    assert all(r["status"] == 503 and r["failures"] == 1 for r in ends)
    assert ends[0]["cooldown_until"] == ends[1]["cooldown_until"]
    assert ends[1]["queued_elapsed"] >= 0.04
    assert ends[1]["upstream_elapsed"] == 0
    assert len(upstream.starts) == 1
    assert len(rows) == 6


@pytest.mark.parametrize("path,method,body,route", [
    ("/search/text?query=private-topic", "GET", None, "search_text"),
    ("/groups/-9876543212345", "GET", None, "group"),
    ("/user_tracker/9876543212345/panels/messages", "GET", None, "user_messages"),
    ("/reporter/parse", "POST", json.dumps({"groups_search_query": "private-body", "run_parser": False, "find_new_users": False, "new_users_limit": 0}), "reporter_preview"),
])
def test_logs_are_allowlisted_and_payload_free(relays, capsys, path, method, body, route):
    server = relays()
    assert request(server, path, method, body, {"Authorization": "Bearer private-caller-key", "Content-Type": "application/json"})[0] == 200
    completed(server)
    rows = records(capsys)
    assert len(rows) == 4
    allowed = {"event", "request_id", "route_class", "timestamp", "queued_elapsed", "upstream_elapsed", "total_elapsed", "status", "origin", "failures", "cooldown_until"}
    assert all(set(r) <= allowed and r["route_class"] == route for r in rows)
    text = json.dumps(rows)
    for secret in ("private-topic", "9876543212345", "private-body", "private-caller-key", "synthetic-relay-owned-key", "messages", "Authorization", "127.0.0.1"):
        assert secret not in text.replace("user_messages", "")


def test_capacity_and_transport_origins(relays, upstream, capsys):
    server = relays(max_clients=1)
    first = http.client.HTTPConnection(*server.server_address, timeout=3)
    first.request("GET", "/search/text?query=hold")
    response = first.getresponse()
    await_starts(upstream, 1)
    assert request(server)[0] == 503
    upstream.release.set()
    response.read()
    first.close()
    completed(server)
    rows = records(capsys)
    capacity = [r for r in rows if r.get("origin") == "client_capacity"]
    assert len(capacity) == 1 and capacity[0]["status"] == 503
    assert capacity[0]["route_class"] == "unknown"

    def broken(timeout):
        raise OSError("private-transport-error")

    server.transport_factory = broken
    assert request(server)[0] == 502
    completed(server)
    rows = records(capsys)
    assert rows[-1]["origin"] == "transport_error"
    assert rows[-1]["status"] == 502
    assert "private-transport-error" not in json.dumps(rows)


def test_logging_failure_does_not_break_request(relays, upstream, monkeypatch):
    server = relays()
    module = server.process_request.__globals__
    attempts = []

    def broken(*args, **kwargs):
        attempts.append(True)
        raise OSError("log unavailable")

    monkeypatch.setitem(module, "print", broken)
    assert request(server)[0] == 200
    completed(server)
    assert request(server)[0] == 200
    completed(server)
    assert upstream.maximum == 1
    assert len(attempts) == 8


def test_error_body_and_missing_state_never_log_payload(relays, capsys):
    server = relays()
    assert request(server, "/search/text?query=error200")[0] == 200
    completed(server)
    rows = records(capsys)
    assert rows[-1]["failures"] == 1
    assert rows[-1]["cooldown_until"] > rows[-1]["timestamp"]
    assert rows[-1]["origin"] == "upstream"
    assert "synthetic backend failure" not in json.dumps(rows)
    (server.state_dir / "state.json").unlink()
    assert request(server)[0] == 503
    completed(server)
    rows = records(capsys)
    assert rows[-1]["origin"] == "local_queue"
    assert "failures" not in rows[-1]


def test_end_is_emitted_after_downstream_shutdown(relays, capsys):
    server = relays()
    shutdowns = []
    original = server.shutdown_request

    def tracked(connection):
        original(connection)
        shutdowns.append(time.time())

    server.shutdown_request = tracked
    assert request(server)[0] == 200
    completed(server)
    rows = records(capsys)
    assert len(shutdowns) == 1
    assert rows[-1]["timestamp"] >= shutdowns[0]


def test_production_spacing_with_client_timeout_above_queue(relays, upstream, capsys):
    server = relays(spacing=5.0, queue_wait=8.0)
    results = []

    def fetch():
        connection = http.client.HTTPConnection(*server.server_address, timeout=10)
        try:
            connection.request("GET", "/search/text?query=ordinary")
            response = connection.getresponse()
            response.read()
            results.append(response.status)
        finally:
            connection.close()

    threads = [threading.Thread(target=fetch) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(12)
    completed(server)
    assert results == [200, 200]
    assert upstream.maximum == 1
    assert upstream.starts[1] - upstream.starts[0] >= 4.95
    ends = [r for r in records(capsys) if r["event"] == "end"]
    assert len(ends) == 2
    assert max(r["queued_elapsed"] for r in ends) >= 4.95
