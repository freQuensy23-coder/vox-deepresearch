# Breadth/depth workflow verification — 2026-10-12

Executed on `openclaw-dp` against the production Gateway and its real configured
model, AIO sandbox, file-backed task submission and SQL batch worker. No injected
follow-up instructions were sent during the successful run.

This was a **synthetic workflow test**, not a live Telegram investigation.
Vox, Jev and web requests were explicitly excluded; their research quality is
not established by this test. Previous live research reports describe the older
workflow and do not prove this new division of responsibilities.

The lead read the skill, created the shared registry, claimed two synthetic
groups, generated a JSON task manifest and called `batch_task(items_file=...)`.
While the children ran, the lead added another keyword. Each child handled its
own group, wrote sourced synthetic `facts.md`, `findings.json` and `todo.md`,
added a distinct keyword and proposed the same third synthetic group with its
own provenance. The lead read the final files and checked the registry.

Observed results:

- Both batch items succeeded. Their executions overlapped for about 53 seconds
  (15:31:10–15:32:03 UTC).
- `groups.done`: 2; `groups.todo`: 1. The shared candidate has two sources and
  was not assigned or researched.
- Four distinct keywords survived concurrent updates.
- Fact sheets were 78 and 57 words, below the 500-word one-page convention.
- Final report and all six child deliverables existed; original test records
  clearly identify synthetic content.
- Gateway readiness was healthy; its API reported 32 execution slots and an
  active durable batch worker. Execution and queue timeouts were configured 0.

An initial run stopped before any batch because the deployment provisioner
rejected shell capacity above 10. Regression reproduced this failure. Provisioner
now forwards/reports the requested capacity (33 for 32 subagents plus the lead);
25 provisioner tests passed and the clean second run above completed.

Other focused server tests: 511 backend cases (manifest admission, persistent
per-run budget, timeout cancellation and regressions) and 139 skill/CLI cases
(including concurrent registry writes). No external research load test with
32 simultaneous model workers or 1000 live research assignments was performed.
Raw logs, credentials and runtime data remain outside Git.
