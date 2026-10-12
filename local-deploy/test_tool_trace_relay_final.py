import json

from test_vox_relay import relays, request, upstream
from test_vox_relay_observability import completed


def test_correlated_request_is_durable_without_headers(relays, upstream):
    server = relays()
    context = {"execution_id": "execution-1", "tool_call_id": "call-1", "run_id": "run-1", "thread_id": "thread-1", "child_call_id": "child-1", "tool_attempt": 1}
    status, _, _ = request(server, headers={"X-Vox-Tool-Trace": json.dumps(context)})
    assert status == 200
    completed(server)
    files = list((server.state_dir / "traces").glob("*.json"))
    assert files
    rows = [json.loads(path.read_text()) for path in files]
    assert all(row["tool_correlation"] == context for row in rows)
    assert {row["event"] for row in rows} == {"queue_entry", "admitted", "upstream_headers", "end"}
    assert all(row["upstream_attempt"] in (0, 1) for row in rows)
    assert all(path.stat().st_mode & 0o777 == 0o600 for path in files)
    assert len(list((server.state_dir / "traces").glob("*.body"))) == 1
