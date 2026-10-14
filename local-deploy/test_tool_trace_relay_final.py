import json
from test_vox_relay import relays, request, upstream  # noqa: F401
from test_vox_relay_observability import completed


def test_caller_trace_context_is_not_forwarded_or_written_to_queue_state(
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
    assert not server.state_dir.exists()
