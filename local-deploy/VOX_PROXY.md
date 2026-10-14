# Vox proxy contract

`vox_relay.py` is the only Vox proxy; `scripts/serve.sh` and `runpod_native.py`
start it, and `skills/public/telegram-research/scripts/vox.py` is its only client.
It forwards only two Vox APIs — `/search/text` and
`/user_tracker/{user_id}/panels/messages` (limit up to 100000; public chats
only) — and refuses every other path and method. No adaptive flag, controller, file locks, calibration, request spacing, or
separate metadata lane is used. Queue admission has no filesystem state.

1. Vox HTTP errors, transport failures, and incomplete/error-bearing search
   responses are handled inside the proxy. No upstream failure body, headers,
   status, or partial stream is forwarded to the agent.
2. Exactly one upstream request executes at a time, including its retries.
3. The proxy retries internally. It honors numeric `Retry-After` (including
   fractions) and HTTP dates. An interrupted/error-bearing text search falls
   back to `stream=false` with its original query, dates, and other parameters.
   It buffers and validates the complete successful response before sending
   downstream headers. Searches require a terminal `stop_reason: "finished"`
   with a messages array; HTTP 200 progress or a partial result is insufficient.
4. Callers wait in an in-memory FIFO queue without a queue-size or queue-wait
   limit. Disconnected callers leave the queue. Cancelled active calls close
   their upstream connection before another caller is admitted.
5. **Each individual HTTP request to Vox has a `call_timeout` limit (50 s)**, including
   DNS, TCP, TLS, response headers, and body. Queue waiting and retry waits are
   outside this clock. Every retry gets its own `call_timeout`. There is no overall
   retry deadline or attempt-count cap: fast errors are handled until a successful
   answer arrives or the caller disconnects. If one actual Vox call exceeds
   `call_timeout`, its connection is closed and the proxy returns its own HTTP 504:
   `{"error":"request_timeout","message":"Request exceeded 50 seconds. Reduce the date interval and try again."}`.
   This is the sole upstream-execution failure exposed to agents, not an
   upstream Vox error. `vox.py` shows this advice; narrowing the period is the agent's decision.

The proxy retains its existing request validation and relay-owned upstream key.
Settings are the `vox_relay` section of `config.yaml` (`port`, `call_timeout`,
`state_dir` for private receipts); the key is `VOX_API_KEY` in the root `.env`. Existing events, raw bodies, and caller correlation are
preserved; an asynchronous writer keeps filesystem I/O outside the HTTP request
and queue slot. Raw upstream failures remain private receipts and are never
forwarded to the agent. Run one proxy process; its FIFO and one upstream slot are
shared by all callers of that process. `/health` and `/status` are local and do
not contact Vox.

Local verification uses real loopback HTTP connections and synthetic upstream
responses, including 32 callers, retries, `Retry-After`, interrupted streams,
client cancellation, the per-call deadline, and the actual Vox CLI:

```sh
python -m pytest local-deploy/test_vox_simple_relay.py local-deploy/test_vox_relay.py local-deploy/test_tool_trace_relay_final.py -q
```
