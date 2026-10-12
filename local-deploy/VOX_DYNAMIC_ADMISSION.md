# Dynamic Vox admission — local implementation

`vox_adaptive_relay.py` is an opt-in relay using `adaptive_gate.py`. This change
has not been deployed. Its tests and comparison runner use localhost HTTP
fixtures and fake credentials; they do not contact Vox or `openclaw-dp`.

The controller regulates concurrent requests using observed response times,
meaningful progress, completed responses, and errors. It does not assume a Vox
requests-per-second quota. The shared search budget starts at five and can grow
to 32; metadata starts at two and can grow to four. Each new or drained request
class first admits one cost probe. These are local controller
settings, not discovered upstream limits. There is no fixed delay between
healthy request starts.

## Request lifecycle

- Comparable search requests have separate latency estimates, grouped by route,
  group scope, date span, and requested result size. Query text and group IDs
  are not stored in controller state.
- A completed cost probe establishes the class's current response time. The
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
- After a latency slowdown, one new cost probe can use a free slot while older
  slow requests finish. All old and new requests count toward the reduced
  overall capacity; the controller does not expand that shared budget while
  older pressured requests remain. Eligible fast classes can pass blocked
  queue entries. Order is
  preserved among eligible requests. A drained class checks its cost again
  before reusing higher concurrency. A successful probe establishes the new
  operating baseline. Errors, cancellations, and unverified completions cannot
  establish a healthy cost probe.
- Metadata has separate permits. An explicit upstream `Retry-After` applies
  across both lanes because the upstream's scope is unknown. Numeric seconds
  and HTTP dates are supported. Recovery admits one probe at a time.
- An HTTP 200 alone does not establish a completed search. The stream must
  contain the observed Vox terminal shape: `stop_reason: "finished"` and a
  `messages` array, without an error or partial-result marker. The relay forwards
  the original bytes. Frames larger than the bounded parser can inspect are
  neutral for learning; they are still forwarded in full.
- Caller cancellation is distinct from an upstream failure. The relay does not
  retry requests. Failure to close an upstream transport retains its permit and
  faults the relay instead of silently admitting replacement work.
- Cooperating local processes share stable file locks and atomic JSON state.
  Process death releases OS locks; elapsed time alone never releases a permit.
  The dynamic relay uses its own state directory and does not migrate or reset
  the existing relay's state.

The executable requires `VOX_RELAY_ADAPTIVE=1`. Its separate state path is
`VOX_RELAY_ADAPTIVE_STATE_DIR`; existing key-file, bind-address, and port settings
are retained. This opt-in is not a deployment instruction. No Compose or
production settings are changed by this implementation.

## Reproduce the local checks

From the repository root, using a Python environment with pytest:

```sh
python -m pytest local-deploy/test_adaptive_gate.py local-deploy/test_vox_adaptive_relay.py local-deploy/test_vox_relay.py local-deploy/test_vox_relay_observability.py local-deploy/test_vox_relay_backoff_contract.py local-deploy/test_vox_relay_production_timing.py -q
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

Both runners save source snapshots and hashes alongside their results. The
[measurement report](evals/dynamic-admission-local-2026-10-12.md) records the
actual trial schedule, results, and intermediate failures. The adaptive relay's
81 local tests also exercise the real `vox.py search-page` CLI through a local
relay and fixture upstream, covering streamed and ordinary JSON, date filters,
full message text, and saved raw responses.

This is a controlled synthetic comparison. It cannot establish Vox's actual
capacity, prove a hard upstream quota, or demonstrate an improvement on real
Vox workloads. Historical observations without 429 responses also do not prove
that Vox has no rate limit.
