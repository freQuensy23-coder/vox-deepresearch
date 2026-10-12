# Local dynamic-admission verification — 12 October 2026

**Scope: synthetic HTTP on 127.0.0.1 only. No Vox API, SSH, or production access. This verifies the local controller; it does not prove a speed increase on Vox itself.**

The final candidate completed the original workload in **7.529 s median**, versus **37.922 s** in the previously recorded run of the identical original defaults. The three historical runs of the identical original relay had a median of **37.761 s**; these are a historical reference, not newly paired observations. The relevant diagnostic is the same original relay with only its 0.25 s spacing reduced to 0.001 s: **7.221 s median**. The comparisons below show the adaptive contribution separately from removing that spacing.

**All 3,702 final-run requests completed semantically and returned the matching request ID.** Source files were frozen, copied into each run directory before imports, and verified by SHA-256 before and after execution. Archived bytes were independently checked. [Compact evidence](dynamic-admission-local-2026-10-12.json) includes source identities, all timing summaries, parameters, and prior failed runs.

**81 functional tests passed on the same V8 implementation**: 51 controller tests and 30 HTTP/CLI integration tests. They cover admission, realized allocation, pressure recovery, explicit Retry-After, cancellation, malformed/incomplete streams, transport cleanup, and the existing Vox CLI interface. These are local tests; they do not contact Vox.

## Three paired trials per workload

All numbers are medians of three trials. The original-default anchor above was measured previously and is not counted as a new paired trial. Trial order is counterbalanced; each comparator gets fresh state. No failures, partial results, client retries, or rejected requests occurred.

| Workload | Fixed 5, spacing 0.001 s | Adaptive | Adaptive wall-time change |
|---|---:|---:|---:|
| Original drained phases | 7.221 s | 7.529 s | +4.3% |
| stable | 3.210 s | 2.670 s | -16.8% |
| mixed | 11.298 s | 9.817 s | -13.1% |
| continuous | 13.917 s | 15.592 s | +12.0% |

A negative change is faster. Positive changes are regressions and remain reported as such. No claim is made that every workload is faster.

## Original phases

| Phase | Fixed 5 / 0.001 s | Adaptive |
|---|---:|---:|
| healthy | 1.661 s | 1.762 s |
| congested | 3.903 s | 3.987 s |
| recovery | 1.662 s | 1.778 s |

## Comparable classes and constrained tail

Client latency here starts when a harness worker issues HTTP. It excludes time waiting for a harness worker; that separate queue is documented below. Tail throughput uses the 48 requests in the second half of the constrained class, from their first HTTP start to their final completion. Concurrent windows overlap intentionally.

| Measurement | Fixed 5 / 0.001 s | Adaptive |
|---|---:|---:|
| Mixed fast p50 | 0.159 s | 0.216 s |
| Mixed fast p95 | 3.705 s | 0.549 s |
| Mixed slow p95 | 3.168 s | 3.678 s |
| Constrained second-half throughput | 6.120 requests/s | 8.733 requests/s |
| Constrained second-half p95 | 3.395 s | 1.226 s |

Continuous slowdowns were detected while requests were still running; no timeout was needed. Controller observations (50 ms sampling):

- Trial 1: first pressure 0.560 s after the first constrained HTTP request; 1 incident(s); comparable-class limit range 1–8, final sampled limit 8. All requests completed without cancellation.
- Trial 2: first pressure 0.593 s after the first constrained HTTP request; 1 incident(s); comparable-class limit range 1–8, final sampled limit 5. All requests completed without cancellation.
- Trial 3: first pressure 0.518 s after the first constrained HTTP request; 1 incident(s); comparable-class limit range 1–8, final sampled limit 8. All requests completed without cancellation.

A separate passive audit found the earlier old-tail idle-capacity defect reduced to **0.051 s / 0 s / 0 s** across the three continuous runs (about one snapshot interval). Older requests still count against the reduced capacity. This removes the confirmed empty-slot wait; it does **not** erase the **12.0% total-wall-time regression** observed during the transition and subsequent recovery. Improved tail throughput is reported alongside, not instead of, that regression.

## Queue and service separation

A job is timestamped before submission to the 12-worker executor. `frontend_wait_seconds` in the JSON means this benchmark executor queue, **not an application browser/UI queue**. `queue_seconds` measures HTTP worker start to upstream receipt, including relay admission and loopback overhead. `service_seconds` is measured by the synthetic upstream. `submitted_to_finish_seconds` includes both queues. Percentiles are medians of per-trial percentiles, not pooled percentiles.

| Original workload p95 | Fixed 5 / 0.001 s | Adaptive |
|---|---:|---:|
| frontend_wait_seconds | 2.051 s | 2.291 s |
| queue_seconds | 1.356 s | 0.890 s |
| service_seconds | 0.555 s | 0.355 s |
| latency_seconds | 1.707 s | 1.249 s |
| submitted_to_finish_seconds | 3.147 s | 3.382 s |

## Functional checks

All six new Python files passed Ruff lint and formatting checks. The earlier baseline regression suite also passed; the baseline code edit only extracts shared request preparation.

## Predeclared fixtures and limits

The [supplemental specification](dynamic-admission-scenarios-2026-10-12.json) was saved before the replacement controller was measured. No service durations, capacity curve, request order, worker counts, or comparator parameters were tuned after the results.

- Original: 48 healthy searches at 0.15 s/capacity 12, 36 constrained at 0.35 s/capacity 4, 48 recovery at 0.15 s/capacity 12. Drain between phases. Metadata every eighth search.
- Stable: 96 searches at 0.15 s/capacity 12, submitted as one queue.
- Mixed: 128 searches, repeating three scoped fast requests (0.15 s) and one broad slow request (1.2 s), capacity 12; one queue.
- Continuous: 48 healthy, 96 constrained, 48 recovery searches with the original base times/capacities, submitted as one queue with no draining barrier. Phase is selected by request identity, so cohorts overlap. This is a controlled request-cost fixture, not a simulation of an internal database scheduler.
- All fixtures: 12 client workers, one 0.03 s metadata request per eight searches; upstream delay is `base + 0.2 * max(0, active_searches-capacity)**2`. Stream headers and a nonmeaningful progress frame arrive immediately, terminal evidence arrives after the delay. Client timeout 180 s. Relay/controller defaults are unscaled. State snapshots every 50 ms use passive atomic-file reads.
- The diagnostic keeps the exact original five-slot gate and 16-client limit. Only spacing changes to 0.001 s; the original rejects zero and polls admission every 0.02 s. Candidate defaults use up to 32 search permits and separate metadata permits. Neither relay reached the client-capacity ceiling in this matrix.

An abrupt capacity drop can still affect already admitted work; this controller does not cancel healthy requests to erase an overload. The matrix includes this transient and reports total wall time alongside tail behavior. Error responses, Retry-After, malformed/incomplete streams, caller cancellation, and cross-process state behavior are covered by separate functional tests, not by this no-error speed fixture.

## Reproduce locally

Baseline: `d211d78c2cbf604dc5a27d098f90fda96d43f07d:local-deploy/vox_relay.py`, SHA-256 `7f9b4b67eb3165743924f46ef31048433366ff6507c9207d0b53818998f234a6`. The baseline snapshot is immutable. Run from the repository root with Python 3.12 and loopback-bind permission:

```sh
mkdir -p .local/vox-adaptive-reproduce
git show d211d78c2cbf604dc5a27d098f90fda96d43f07d:local-deploy/vox_relay.py > .local/vox-adaptive-reproduce/baseline.py
python local-deploy/benchmark_adaptive_relay.py --baseline .local/vox-adaptive-reproduce/baseline.py --output .local/vox-adaptive-reproduce/original --trials 3 --variants adaptive fixed_matched_spacing
python local-deploy/benchmark_adaptive_scenarios.py --baseline .local/vox-adaptive-reproduce/baseline.py --output .local/vox-adaptive-reproduce/supplemental --trials 3
# Optional: reproduce the original-default reference as a separate run
python local-deploy/benchmark_adaptive_relay.py --baseline .local/vox-adaptive-reproduce/baseline.py --output .local/vox-adaptive-reproduce/old-anchor --trials 1 --variants fixed
```

Output directories must be new. Every run archives source files in `code/`, uses fake credentials, and redirects upstream traffic exclusively to loopback. Absolute timings vary by machine. Raw final artifacts are retained in `.local/vox-adaptive-local/benchmark-v8-final/` (gitignored); the accompanying JSON carries portable summaries without home-directory paths.

## Retained intermediate failures

| Version | Original-default median | Adaptive median | Diagnostic median | Interpretation |
|---|---:|---:|---:|---|
| V1 | 37.800 s | 22.793 s | 7.289 s | Permissive startup estimates; incomplete source identity and active observer; not final evidence |
| V2 | 37.803 s | 23.695 s | 7.269 s | Persistent-latency recovery and comparable-class isolation required correction |
| V3 | 37.761 s | 20.015 s | 7.432 s | Failed congestion and p95 acceptance despite faster original-default comparison |
| V4 supplemental pilot | — | Stable 4.180 s | Stable 3.218 s | Failed stable-throughput acceptance; cross-epoch completion accounting was corrected |

V5 continuous final repetitions were 16.160 s adaptive versus 13.935 s diagnostic. Passive traces exposed permitted capacity left idle behind old pressure-marked requests. V6 removed that wait but measured an unrealized two-slot allocation against a one-slot baseline (15.729 s versus 13.928 s). V7 fixed the allocation measurement and improved mixed throughput; an additional active-other-class eligibility edge was corrected before the final matrix. These intermediate runs and their exact source snapshots are retained: [V5 failed report](../../.local/vox-adaptive-local/benchmark-v5-acceptance/report-failed.md), [V6 notes](../../.local/vox-adaptive-local/benchmark-v6-continuous-pilot/INTERMEDIATE.md), [V7 continuous evidence](../../.local/vox-adaptive-local/benchmark-v7-continuous-pilot/summary.json).

V4 source snapshots and all later sources are retained. V1–V3 retain raw measurements and available hashes, but not complete immutable source snapshots; they are not presented as reproducible final-code evidence. The final fixture remains unchanged. Intermediate numeric summaries are included in the accompanying JSON. Local-only audit links (not available in a clean clone): [V1](../../.local/vox-adaptive-local/benchmark-v1-local/INTERMEDIATE.md), [V2](../../.local/vox-adaptive-local/benchmark-v2-frozen/INTERMEDIATE.md), [V3](../../.local/vox-adaptive-local/benchmark-v3-final/report-acceptance-failed.md), [V4](../../.local/vox-adaptive-local/benchmark-v4-supplement-pilot/INTERMEDIATE.md).
