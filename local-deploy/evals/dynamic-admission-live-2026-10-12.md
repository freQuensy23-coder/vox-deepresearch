# Real Vox comparison — 12 October 2026

**The final adaptive relay reduced median elapsed time by 27.7%: 40.256 s →
29.106 s. It beat the original fixed-five scheduler in all three paired rounds.**
These are actual read-only Vox requests from the local worktree, not fixture
timings. No SSH access or deployment was performed.

| Round | Original fixed 5, 250 ms spacing | Fixed 5, 1 ms spacing | Adaptive |
|---|---:|---:|---:|
| 1 | 38.962 s | 34.683 s | 29.106 s |
| 2 | 44.188 s | 39.534 s | 29.629 s |
| 3 | 40.256 s | 25.424 s | 28.685 s |
| Median | **40.256 s** | **34.683 s** | **29.106 s** |

The diagnostic without the old start delay isolates more than simply removing
that delay: adaptive reduced its median by **16.1%**, but was **12.8% slower in
round 3**. This is an observed improvement on this workload, not a universal
speed guarantee or a measurement of Vox's maximum capacity.

## Completed work and controls

- **288/288 searches completed with `stop_reason=finished`**, without HTTP,
  transport, admission or partial-result errors. No retries were used.
- Every block returned 1,202 message records: 1,179 from a populated search using
  the CLI's default `limit=2000`, and 23 across 31 scoped searches. All 32
  per-query hashes of message IDs, text and dates matched across all nine blocks.
  Mutable membership metadata was excluded from that comparison. These are
  returned records, not a claim of 1,202 distinct messages across queries.
- The exact 32-query workload was frozen before measuring the final code;
  its SHA-256 is `c7520cfa055252700641cc2ca70add89b97383e94f3db1d7a56f3350a964fe04`.
  Each block used fresh state and 12 client workers. Order rotated:
  adaptive/diagnostic/original, diagnostic/original/adaptive,
  original/adaptive/diagnostic.
- Baseline code was the immutable `d211d78c` relay. Its five slots and spacing
  were unchanged. Its queue budget was extended to 700 s for complete comparisons;
  adaptive used its actual default of waiting until caller cancellation. The
  **largest observed queue wait was 35.51 s**, below even the original 120 s
  budget, so neither budget fired in this final matrix.
- All clients used the current relay-mode CLI policy of no separate socket-idle
  deadline. The relay's upstream deadline remained 700 s, starting after admission.
  Wire query parameters were identical across comparators. Actual adaptive peak
  concurrency was 6, 7 and 7; the configured maximum of 32 was not reached.
- Runtime, parser and CLI files were hashed and archived. Before/after hashes
  matched, and the parent independently checked the raw results and current files.

## Corrections required by the real tests

The original candidate was **38.5% slower** on an earlier workload. A first
variance fix was still **26.8% slower**. Traces showed the controller keeping
requests queued despite faster upstream service: one unusually quick response
became a latency ceiling, and isolated slow requests repeatedly serialized an
entire request class. The final controller starts within the operator's initial
five-slot budget, measures an initial cohort, and requires correlated evidence
before reducing admission. Single slow requests retain their own permits.

Two original 120 s queue runs admitted only 12/25 and 8/32 searches. All 20
requests actually sent to Vox finished; these were local admission failures,
not Vox rate-limit responses, and are excluded from speed calculations.
Cancellable admission and the matching CLI timeout policy remove that local
cutoff while retaining the separate upstream deadline.

An actual CLI call also exposed a large-response bug: 1,179 messages in a
1.75 MB final frame were returned correctly but classified as `neutral` for
learning. The new bounded-memory JSON validator fixes this. A fresh real CLI
call without `--limit` returned 1,179 messages and recorded
**one success, zero neutral outcomes**. All 96 adaptive searches in the final
matrix were likewise recorded as successes, including their large frames.

The earlier speed workloads used a smaller filled page whose results varied;
their absolute timings are not compared with this final, content-matched workload.

## Evidence

[Sanitized measurements](dynamic-admission-live-2026-10-12.json) contain all block
timings, queue/service separation, source hashes, result checks and failed earlier
attempts. Raw bodies, manifests and snapshots remain private under
`.local/vox-live-ab-20261012/`. No credentials or message bodies are published.

**287 local regression tests passed**, including overload recovery, cancellation,
the actual CLI interface, large streamed/ordinary JSON, malformed responses and
parser memory bounds. Synthetic results remain separately labelled in the
[historical report](dynamic-admission-local-2026-10-12.md).
