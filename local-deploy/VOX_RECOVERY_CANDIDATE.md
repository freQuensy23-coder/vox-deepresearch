# Staged Vox recovery candidate — generation accounting only

**Not deployed. Default off. Not an adaptive-recovery implementation or proof of a shorter safe pause.**

`vox_recovery_candidate.py` is a separate executable experiment beside the unchanged `vox_relay.py`. The live Compose command, mounts, environment, relay source, state and services are not changed by this work. Without `VOX_RELAY_RECOVERY_CANDIDATE=1`, the candidate entrypoint calls the original relay entrypoint. Only literal `0` and `1` are accepted. Constructing `CandidateRelayServer` directly is also an explicit opt-in used by offline fixtures.

## Implemented boundary

- Requests capture an admission generation under the existing stable `gate.lock`. One cohort's non-429 failures increment the fallback stage once; late siblings renew the **current stage** from their own failure time rather than multiplying it. Genuinely later admitted failing cohorts still increase protection.
- Production fallback remains 60 / 120 / 240 / 480 / 900 seconds, capped at 900. No learned delay, early admission, zero-backoff policy, retries or additional query exists.
- A separate monotonically increasing fault revision fences successes admitted before **any** subsequent fault, including a late sibling whose cohort generation does not change. Generation alone cannot provide that fence.
- Explicit finite nonnegative numeric (including fractional) or HTTP-date Retry-After is persisted at header receipt as a separate, uncapped hard floor. It survives cancellation and successful completion; expired deadlines are retained rather than erased. Raw header telemetry is limited to 256 characters; oversized/invalid headers use the existing fallback, with parsing validity/truncation exposed. 429 retains its separate gap increase and two-second missing/invalid-header fallback.
- Stable slot flocks cover the entire upstream response and both close operations. Validated recovery is committed only after upstream/response close, before slot release. A close exception faults that server and retains its slot until process exit.
- Search recovery requires transport EOF, 2xx, no cancellation/transport failure, and a structurally valid terminal search frame with a messages list and literal `finished: true`, without an error or subsequent nonblank frame. Frame memory is capped at 1 MiB. Oversized/malformed/incomplete frames cannot prove recovery. This is structural search-result validation, **not full message-schema or content validation**.
- Positively observed downstream EOF/reset or downstream broken-pipe/reset is neutral, never successful. Relay deadlines and unknown transport failures remain conservative failures. An upstream failure already recorded before cancellation is not undone.
- Metadata contains route class, random request ID, timings, header raw/parsed values, termination/exception class, EOF/semantic booleans, admission/failure generation, policy and atomic completion state before/after. `recovery_owner` is explicitly null because no isolated trial owner exists. The candidate emits no request query/body, response content, personal IDs, exception text, tool correlation or `.body` files. Existing baseline tracing is unchanged.
- Version 1/2 migration requires all five possible legacy slots to be drained and preserves the entire legacy combined deadline as a hard floor. Version 3 validates slot count, deadlines and counters; legacy readers reject it. Stable lock files are never replaced. State writes reuse atomic replace and file/directory fsync.

## Deliberately unimplemented

This is the explicitly permitted smaller **generation-accounting + telemetry** alternative, not the full half-open design:

1. No one-request half-open recovery owner or inflight-drain gate after cooldown. Existing slot concurrency remains. The test `test_incremental_scope_keeps_multiple_slots_after_deadline` makes that boundary explicit.
2. Therefore sequential failing cohorts are tested, **not isolated recovery trials**; no cross-process single-probe-exclusion claim is made. Five independent processes are tested for cohort coalescing and shared slots.
3. No durable ledger of ordinary in-flight attempts or abandoned-trial crash penalty. SIGKILL tests prove previously persisted failures/hard deadlines survive process death, not detection of a crash before failure recording. A future half-open implementation must persist and recover trial ownership conservatively.
4. Non-search endpoints are forwarded but never accepted as validated recovery. Endpoint-specific nonstream schemas remain to be implemented. Pretty-printed/multiline JSON is also not recognized as recovery by this NDJSON-oriented observer.
5. No estimator, initial-delay optimization, real upstream comparison, production activation, automatic downgrade utility or restart is supplied.
6. Disk/power-loss fault injection and candidate cross-container integration are not established by these tests. Existing atomic filesystem primitives are reused; successful synthetic tests are not a durability guarantee under arbitrary storage failure.

## Offline verification and comparison

From the checkout:

```sh
python -m pytest local-deploy/test_vox_recovery_candidate.py -q
python -m pytest local-deploy tests/skills -q
```

All request fixtures use local synthetic upstreams/dummy credentials. Unit-clock tests retain production 60/900 values; HTTP fixtures scale waits unless explicitly named production timing. Compare legacy `Gate.finish` with candidate `GenerationGate.complete(owner, failed=True)` on the same cohort admissions: five already-admitted faults produce legacy stages 1–5 versus candidate stage 1; later candidate admissions failing after each deadline retain the full ladder. This comparison establishes accounting behavior, not upstream readiness.

Evidence: `.deployment/vox-adaptive-recovery-candidate/` contains before snapshots/hashes, RED/GREEN logs, synthetic comparison, task-only diff and final hashes. The pinned baseline relay hash is `7f9b4b67eb3165743924f46ef31048433366ff6507c9207d0b53818998f234a6`.

## Activation gate — no authorization assumed

Do not mount/run the opt-in against production state or switch the live relay while research is active. The running research must not be restarted, replaced, manually supplemented with Vox calls or used as an implicit canary.

Any activation requires separate approval for a bounded real baseline/candidate comparison, an authorized paused/idle relay with all owners drained, reviewed deployment/rollback artifacts, preserved state, matched endpoint/workload/budget conditions, a finite ordinary-request budget and stop criteria. The separate candidate file and original relay must both be available to that future deployment. No Compose override was created here.

Compare semantic-finished rate, true provider 429/5xx, interruption versus identified caller cancellation, retries/amplification, queue/service/end-to-end latency and late sibling failures. Stop on worse provider errors, retry volume or finished rate. The unchanged baseline lacks the candidate's semantic/header fields; comparable baseline observation instrumentation or independently available endpoint completion receipts is still needed before claiming a fair real comparison. Merely comparing historical clean relay ends against candidate `finished` counts is invalid.

Any shorter **initial** fallback needs a separately authorized early-admission experiment; passive success after enforced waits does not establish the minimum safe pause. This candidate contains no setting that learns or promotes a shorter initial delay.

Downgrade to the baseline on a version-3 directory intentionally fails closed. Do not delete state/locks or restore an obsolete snapshot to make it start. A separately reviewed, quiescent downgrade must preserve the maximum hard/soft/combined deadline and current counters; it is not automated here.
