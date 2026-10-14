# Dynamic Vox admission

`vox_adaptive_relay.py` is an opt-in relay using `adaptive_gate.py`. It was
enabled on `openclaw-dp` on 13 October 2026 (runtime release `038d076b`). [Real Vox measurements](evals/dynamic-admission-live-2026-10-12.md)
record a 27.7% median elapsed-time reduction on the tested workload. The committed
test suites and synthetic comparison runners use localhost fixtures and fake
credentials; they do not contact Vox or `openclaw-dp`.

The controller regulates concurrent requests using observed response times,
meaningful progress, completed responses, and errors. It does not assume a Vox
requests-per-second quota. The shared search budget starts at five and can grow
to 32; metadata starts at two and can grow to four. Healthy new batches use the
operator's initial budget, bounded by the currently available shared capacity.
These are local controller settings, not discovered upstream limits. There is no fixed delay between
healthy request starts.

## Request lifecycle

- Comparable search requests have separate latency estimates, grouped by route,
  group scope, date span, and requested result size. Query text and group IDs
  are not stored in controller state.
- The first assigned cohort establishes a response-time distribution while
  admission continues within the initial budget. Early fast completions do not
  exclude still-running slow members from this initial measurement. After a
  reduction, this cohort is bounded by the available shared and class capacity;
  a one-slot budget does not require five sequential successes before growth
  can resume. Every already-assigned cohort member must still finish. Successful
  initial-cohort or class-exclusive requests under bounded shared load update
  a window of up to 16 observations. Its median estimates typical cost;
  its empirical 90th percentile accounts for natural variation. A single fast
  response is not a permanent latency ceiling. Loaded concurrency experiments,
  errors and cancellations do not train this cost window. The
  controller then measures rolling windows of completed upstream work, including
  requests still running across a capacity change and excluding caller queue
  wait. It retains higher concurrency when completion throughput improves
  without material latency inflation, and returns to the last useful capacity
  when the extra concurrency hurts. Stalls and failures reduce admission before
  all pending responses finish. Existing requests keep their permits until
  their connections close.
- A capacity experiment is evaluated only after that concurrency was actually
  allocated. Insufficient shared slots are distinct from upstream slowdown.
  When healthy work fills the shared budget and eligible demand remains, the
  controller can test one additional shared slot, then confirm or revert it.
  Active classes count toward demand even when their own queues are empty.
- An isolated slow request keeps its permit without serializing its entire
  class. Latency-based reduction requires correlated slow observations;
  reducing the shared budget also requires actual full shared load. HTTP failures
  and explicit Retry-After retain their separate recovery rules.
- After a latency slowdown, one new cost probe can use a free slot while older
  slow requests finish. All old and new requests count toward the reduced
  overall capacity; the controller does not expand that shared budget while
  older pressured requests remain. Eligible fast classes can pass blocked
  queue entries. Order is
  preserved among eligible requests. A drained class starts again within the
  initial budget before attempting higher concurrency. A recovery probe establishes the new
  operating baseline. Errors, cancellations, and unverified completions cannot
  establish a healthy cost probe.
- Metadata has separate permits. An explicit upstream `Retry-After` applies
  across both lanes because the upstream's scope is unknown. Numeric seconds
  and HTTP dates are supported. Recovery admits one probe at a time.
- An HTTP 200 alone does not establish a completed search. The stream must
  contain the observed Vox terminal shape: `stop_reason: "finished"` and a
  `messages` array, without an error or partial-result marker. The relay forwards
  the original bytes. `vox_json_probe.py` validates JSON incrementally and keeps
  only bounded root-field summaries, so large message arrays still establish
  completion. Message text is never retained by the probe. Excessive nesting
  is neutral for learning; malformed or incomplete JSON is a failure.
- Caller cancellation is distinct from an upstream failure. The relay does not
  retry requests. Failure to close an upstream transport retains its permit and
  faults the relay instead of silently admitting replacement work.
- Admission waits until the caller disconnects unless an explicit finite
  `queue_wait` is supplied. Waiting does not consume the upstream request
  deadline: that clock starts only after admission. The Vox CLI uses no separate
  socket-idle cutoff through the local relay; the relay owns the upstream
  deadline, and stopping the CLI closes its socket and cancels the queued entry.
  Direct API calls retain their existing socket timeouts. A faulted transport
  fails admission immediately rather than leaving an unbounded waiter stuck.
- Cooperating local processes share stable file locks and atomic JSON state.
  Process death releases OS locks; elapsed time alone never releases a permit.
  The dynamic relay uses its own state directory and does not migrate or reset
  the existing relay's state.

The executable requires `VOX_RELAY_ADAPTIVE=1`. Its separate state path is
`VOX_RELAY_ADAPTIVE_STATE_DIR`; existing key-file, bind-address, and port settings
are retained. The production release uses an explicit Compose override to start
`vox_adaptive_relay.py` and mounts `adaptive_gate.py`, `vox_json_probe.py`, and a
separate v6 state directory. The previous relay state is preserved; its cooldown
was checked as expired before switching. Gateway and relay readiness checks passed.

The current controller state format is version 6. Older adaptive state files are
rejected rather than silently reinterpreted. Local comparisons use fresh state
directories; an observed upstream Retry-After remains binding across comparison
variants and must not be bypassed by changing directories.

## Reproduce the local checks

From the repository root, using a Python environment with pytest:

```sh
python -m pytest local-deploy/test_adaptive_gate.py local-deploy/test_vox_adaptive_relay.py local-deploy/test_vox_adaptive_queue_wait.py local-deploy/test_vox_json_probe.py tests/skills/test_telegram_research.py -q
```

The comparison runner needs an immutable copy of the previous relay and a new
output directory:

```sh
mkdir -p .local/vox-adaptive-comparison
git show d211d78c:local-deploy/vox_relay.py > .local/vox-adaptive-comparison/baseline.py
python local-deploy/benchmark_adaptive_relay.py --baseline .local/vox-adaptive-comparison/baseline.py --output .local/vox-adaptive-comparison/results
```

It runs three counterbalanced trials of the original defaults, dynamic
admission, and a diagnostic fixed-five variant with almost no start spacing.
Each trial includes healthy service, congestion, recovery, and interleaved
metadata requests. It records semantic completion, queue and service latency,
concurrency, and source hashes before and after execution. The diagnostic
separates removing the old 250 ms spacing from the effect of adapting concurrency.

Additional predeclared workloads exercise sustained healthy traffic, mixed
query costs, and a capacity drop inside a continuously populated queue:

```sh
python local-deploy/benchmark_adaptive_scenarios.py --baseline .local/vox-adaptive-comparison/baseline.py --output .local/vox-adaptive-comparison/scenarios
```

Both runners save source snapshots, including the JSON probe, and hashes
alongside their results. The [historical synthetic report](evals/dynamic-admission-local-2026-10-12.md)
records the earlier V8 implementation's schedule, measurements and failures;
those timings are not measurements of the current code. The current combined
suite passes 287 cases, including the real `vox.py search` CLI through a
local relay and fixture upstream: streamed results, date filters, full text,
cancellable admission, and large final frames.

This is a controlled synthetic comparison. It cannot establish Vox's actual
capacity, prove a hard upstream quota, or demonstrate an improvement on real
Vox workloads. Historical observations without 429 responses also do not prove
that Vox has no rate limit.

## Transport deadlines

Relay defaults are 2100 seconds for the upstream request, 1890 seconds of
upstream socket inactivity, and 30 seconds to read the client request. These are
three times the preceding defaults. The fixed relay queue allowance is 360 seconds;
adaptive admission retains its unbounded, cancellable queue. API search timeouts
(maximum 600 seconds), cooldowns, and Retry-After remain unchanged.
