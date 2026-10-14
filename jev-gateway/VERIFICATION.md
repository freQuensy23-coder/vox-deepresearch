# Verification evidence

Scope: local implementation, synthetic automated integration tests, and parent-operated deployment/live verification. The implementation tests use no live credentials; the separate live scripts perform genuine account-authorized inference.

Executed from `/home/private/jev-gateway` using the project's own `.venv/bin/python`:

- Initial TDD red suite: 41 failures because the gateway package did not yet exist.
- Additional safety TDD red suite: missing total attempt deadline, launcher, blank-key rejection, and honest unknown metadata date were observed before those changes.
- Response-preservation TDD red: integer probability was incorrectly changed to float by FastAPI response serialization; fixed by returning the already-validated original response as JSON with only model normalization.
- Final `pytest tests -q --tb=short --junitxml=test-results.xml`: **64 passed in 2.06s**.
- Immediate repeat full suite: **64 passed in 2.11s**.
- `pip check`: **No broken requirements found.**
- Editable package wheel build/install: succeeded for `jev-local-gateway==1.0.0`.
- `compileall -q jev_gateway tests`: exit 0.
- Token/AST audit of source/tests: **comments/docstrings: []**.

The tests exercise real gateway lifespan, request validation, routing, bounded streaming/total deadlines, half-open concurrency and cancellation, authentication, body caps, durable SQLite insertion/recovery/cancellation/pagination, database permissions and ownership locking. Provider I/O is a synthetic `httpx.MockTransport`; the real official `typesafe-sdk==0.7.4` is exercised using a synthetic ASGI transport adapter. The official request requiredness/nullability is compared against the supplied snapshot at `tests/official-openapi.json`.

SDK integration discovered that its `base_url` must be `http://127.0.0.1:18791`, without `/v1`, because the SDK appends `/v1` itself.

## Parent deployment and live verification

User service `jev-gateway.service` is enabled and running; socket inspection confirmed only `127.0.0.1:18791`. The systemd unit is `/home/private/.config/systemd/user/jev-gateway.service`, with private state and separate local/upstream credential EnvironmentFiles. Auto-recharge was not enabled.

`python scripts/live_smoke.py` passed against the deployed localhost service with real inference: health 200, unauthenticated models 401, invalid request 422, normal mixed noul/choice/score evaluation 200, real official SDK 0.7.4 evaluation and model enumeration, and a three-item batch with paginated results. All three batch items completed successfully. Consumer model names remained `jev-1.13.0`, with no provider/free identifiers in payloads.

A service restart preserved completed batch `batch_e458e5c058434b8b9cef951231043d2b` (3 completed, 0 pending). Database and lock permissions were 0600 in the private directory.

`python scripts/live_routing.py` passed through a disposable localhost HTTP instance using the same gateway factory. Its first free response was deliberately injected as 429; the resulting paid fallback and next cooldown request both contacted the actual paid Jev route and returned HTTP200. A controlled clock was advanced by 301 seconds, after which the actual free route returned HTTP200. This verifies recovery logic and actual paid results; it does not prove a provider quota resets in five minutes or that a naturally occurring free quota was exhausted. The disposable listener/database were removed after the run.

Remaining boundaries: batch recovery is at least once; a crash after upstream completion but before SQLite commit can repeat an item and its cost. Cancellation cannot recall a request already submitted upstream. Completed input/result retention is indefinite. Release metadata stays an empty string unless an authoritative date is configured. The bundled automated suite passed again in the parent runtime: 64 tests, with no broken requirements.
