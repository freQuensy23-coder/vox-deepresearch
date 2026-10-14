import json
from test_vox_relay import relays, request, upstream  # noqa: F401
from test_vox_relay_observability import completed


def test_caller_trace_context_is_preserved_in_private_receipts_and_not_forwarded(
    relays,  # noqa: F811
    upstream,  # noqa: F811
):
    server = relays()
    context = {
        "execution_id": "execution-1",
        "tool_call_id": "call-1",
        "run_id": "run-1",
        "thread_id": "thread-1",
        "child_call_id": "child-1",
        "tool_attempt": 1,
    }
    assert request(server, headers={"X-Vox-Tool-Trace": json.dumps(context)})[0] == 200
    completed(server)
    assert all(
        "x-vox-tool-trace" != name.lower()
        for _, _, headers, _ in upstream.requests
        for name in headers
    )
    files = list((server.state_dir / "traces").glob("*.json"))
    rows = [json.loads(path.read_text(encoding="utf-8")) for path in files]
    assert all(row["tool_correlation"] == context for row in rows)
    assert {row["event"] for row in rows} == {
        "queue_entry",
        "admitted",
        "upstream_headers",
        "end",
    }
    assert all(path.stat().st_mode & 0o777 == 0o600 for path in files)
    assert len(list((server.state_dir / "traces").glob("*.body"))) == 1
