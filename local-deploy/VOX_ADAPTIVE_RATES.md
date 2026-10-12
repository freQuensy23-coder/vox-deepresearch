# Baseline Vox relay: fixed concurrency and adaptive spacing

This describes the earlier relay. The separate, locally tested dynamic
concurrency implementation is documented in [VOX_DYNAMIC_ADMISSION.md](VOX_DYNAMIC_ADMISSION.md).
Its addition does not deploy it or change the running baseline.

Rate-only port from TG-OSINT `tg-collect-v3.py`, SHA256 `b8569cf1d2f1f174cc64450ca743bac4f4f1f06bda44f2fd2d5fd8f775e77026`; resolved source configuration: five workers, 0.25-second base gap.

- Five global in-flight slots, shared across all sandbox clients and cooperating relay processes on the same private state directory. Stable per-slot flock files remain held through response EOF and upstream socket close. Process death releases ownership.
- Start admission is serialized by the existing stable `gate.lock`, not by a CLI-local gate. The persisted start gap is initially 0.25 seconds.
- Each upstream HTTP 429 doubles the gap, capped at five seconds, and resets the successful-response streak. A numeric Retry-After, including fractional seconds, or HTTP date sets a common persistent cooldown. Missing/invalid Retry-After uses the source's first-attempt fallback of two seconds. The next admission waits for both the current gap and the longest outstanding cooldown. Other already-admitted streams are not interrupted.
- The relay does not retry, and relay-mode CLI remains one HTTP attempt. The source collector's later-attempt exponential retry schedule is deliberately not imported: there is no relay retry attempt index and no new retry budget. A later caller's 429 without Retry-After again establishes a two-second common cooldown, plus the adaptive gap.
- Fifty consecutive completed successful HTTP responses reduce the gap by 20%, with a 0.25-second floor, and reset the streak after a reduction. Non-success responses break the streak. Success from an overlapping request cannot erase another request's still-active cooldown.
- Non-429 transient failures retain the existing 60/120/240/480/900-second backoff, Retry-After extension, classifications and success reset. HTTP 429 does not increment this non-429 failure ladder.
- Queue wait 120 seconds, request deadline 700 seconds, socket idle 630 seconds, client validation 10 seconds and maximum 16 client connections are unchanged. No query, response forwarding, privacy, authentication, fixed-upstream or network policy changed.
- Private limiter state migrates from version 1 to 2 under the gate lock, preserving last start, cooldown, failures and request count. It adds gap and success streak. Missing/corrupt initialized state remains fail-closed. Do not delete or replace live lock files. Mixed old/new relay processes are unsupported; upgrade only while globally idle.

## Verification and deployment

`.deployment/vox-adaptive-rates/verification.json` binds pre-change files, actual RED/GREEN logs, isolated HTTP tests, cross-container CLI proof and installed source/config/state readback. The fixture uses an internal Docker network and a dummy key; no real Vox requests were made. Tests cover five concurrent full streams with a queued sixth client, process sharing, crash/disconnect release, persisted 429 and Retry-After, real 50-response decay, floor/cap, and unchanged non-429 backoff.

Only `vox-relay` was recreated with all four existing Compose layers, `--no-deps --no-build`; gateway/tracing image and manually managed Jev bridge retained their container IDs. Predeployment global run/API, child tables, sandbox inventory, relay TCP and gate-lock checks were idle. State requests did not increase during deployment verification.

The CLI search interface changed concurrently in a separate pagination task. Rate tests call its stable `search-page` primitive; this rate-only task did not modify the CLI or collector. Its transport, relay selection and Admission ASTs match the pre-change snapshot.

Rollback requires global idle again. Version-1 relay code cannot read version-2 state: preserve the private version-2 backup and atomically convert only the state schema under the gate lock, retaining the latest timestamps/cooldown/failures/requests, before recreating the prior relay source. Do not restore stale runtime counters or clear an outstanding cooldown.
