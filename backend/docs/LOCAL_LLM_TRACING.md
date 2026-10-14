# Local LLM traces

Activated locally on 2026-10-11 in `local/vox-deepresearch-gateway:llm-tracing-20261011`. Deployment and real application-call receipts: `.deployment/llm-trace-activation/verification.json` and `deployed-readback.json`. Editing this checkout alone does not update the running gateway.

`create_chat_model` installs an owner-scoped local callback independently of external tracing and `attach_tracing`. It records untruncated dispatched messages, invocation parameters and tool schemas, returned messages/tool calls/usage, streaming chunks and final aggregation, errors and cancellation. Lead, children, goal evaluation and summary models use this factory. Goal evaluation carries its native run ID; children carry their server-generated task ID; manual compaction binds its thread explicitly.

Default location: `runtime_home()/llm-traces`. An operator may set `DEER_FLOW_LLM_TRACE_DIR` to a private, durable gateway-only directory. Never mount it into research sandboxes or serve it as an artifact directory. Owner buckets and call directories use SHA256 identifiers, directories are 0700 and records 0600. Each event is separately atomically published after file fsync, followed by directory fsync. There is no automatic truncation, expiry or deletion. Plan disk capacity and private backups before activation.

Each record includes call/parent IDs, owner/run/thread/child/checkpoint attribution when applicable, UTC and monotonic timestamps, process instance and PID. Terminal events distinguish completion, error, cancellation and confirmed interruption. Streaming cancellation does not imply that a provider's executor-thread HTTP request has stopped: later provider events can still be recorded.

Transport configuration fields and headers are excluded. Ordinary message text, tool arguments, tool-schema fields and provider JSON body fields named `password`, `secret`, `headers` or containing `Bearer` remain literal. Traces are confidential raw local data, not suitable for public logs or publication. Do not disclose private model reasoning from provider data.

Codex records each retry's actual constructed request body, parsed SSE events, consumed SSE lines, reconstructed final response, HTTP error body and terminal errors. HTTPX-backed SDK providers additionally record each actual HTTP send and its body, response body or consumed byte chunks (base64), status, transport errors and premature closure. These observe SDK retries without changing retry budgets. HTTP headers are never stored. Response data not consumed because the SDK closes a stream is explicitly marked `fully_consumed=false`, not invented.

`model_settings.local_trace_wire_coverage` declares `codex_payload_and_sse`, `httpx_requests_and_consumed_responses`, or `logical_calls_only_no_httpx_transport`. Non-HTTPX/custom providers still get logical-call callbacks but their internal wire attempts are not guaranteed. Do not claim universal wire coverage for an arbitrary provider. The configured Codex path has its explicit provider boundary.

Writes fail softly to preserve the underlying model result. Every failure makes process trace health incomplete and queues a payload-free `trace_gap` indicator, persisted on the next writable event. Historical persisted gap records remain visible to `trace_health()` after process restart. If storage remains unavailable until process death, no software can guarantee a durable gap indicator; safe activation requires writable durable storage and monitoring errors. `complete=true` is a storage-gap status, not an assertion of universal provider coverage or completed research.

Inspect health locally in the backend environment:

```python
from deerflow.models.local_trace import trace_health
print(trace_health())
```

After independently confirming the old writer process has stopped, unfinished calls can be marked without deleting evidence:

```python
from deerflow.models.local_trace import mark_interrupted_calls
mark_interrupted_calls(trace_root, process_instance_id=old_instance_id, confirmed_stopped=True)
```

Offline proof runner: `.deployment/llm-trace-proof/run.py` uses the installed gateway image, network disabled, read-only sources and disposable synthetic fixtures. It preserves every test attempt's output and command receipt. Run after sourcing `/home/private/bin/docker-rootless-env`:

```sh
python .deployment/llm-trace-proof/run.py tests/test_local_llm_trace.py
```

## Activation gate

Wait for all research and child activity to stop. Preserve the current image and all three Compose overlays. Build a new image from the reviewed source snapshot without changing model configuration, credentials or sandbox mounts. Confirm the trace directory is private, durable and not sandbox-visible, and budget disk growth. On native deployments, restart only the Gateway with the reviewed source and existing environment; on container deployments, preserve the existing operator overlays. Read back image/source hashes, effective configuration and readiness. Verify fresh traces and health with an explicitly authorized synthetic call, then inspect the exact new run/thread/call records. Do not restart an active run or imply old calls can be reconstructed.
