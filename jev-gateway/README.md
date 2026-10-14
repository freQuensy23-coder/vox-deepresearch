# Jev local gateway

Python 3.11+ gateway exposing the Jev System One protocol and a durable asynchronous batch extension. The bundled launcher binds **127.0.0.1** on `jev_gateway.port` from the repository `config.yaml` with exactly one server worker. Do not expose it publicly or start additional ASGI workers.

## Install and run

```sh
python -m jev_gateway   # from jev-gateway/, with the backend virtualenv
```

Settings come from the `jev_gateway` section of the repository `config.yaml`; keys come from the repository `.env`, which must provide `JEV_GATEWAY_API_KEY` and `OPENCODE_GO_SUBSCRIPTION_KEY`. Empty or whitespace-only keys fail startup. Consumers use only the local key, never the upstream key.

With the official Python SDK 0.7.4, `base_url` is the server root, **not** a URL ending in `/v1`:

```python
import os
from typesafe_sdk import Noul, TypeSafeClient

with TypeSafeClient(api_key=os.environ["JEV_GATEWAY_API_KEY"], base_url="http://127.0.0.1:18791") as client:
    response = client.system_one(state="Please refund this purchase.", questions={"refund": Noul(instructions="Does the message request a refund?")})
    print(response.model, response.answers["refund"].noul)
```

## Consumer contract

All `/v1/*` endpoints require `Authorization: Bearer <local key>`. `/healthz` is unauthenticated and reports worker health without operational details.

- `POST /v1/systemone`: required `state` (string, object, or array), required `model`, required nonempty `questions` map. Models: `jev-latest`, `jev-preview`, `jev-1.13.0`. Responses always report `jev-1.13.0`.
- Questions use a `type` discriminator: `noul`, `choice`, `score`. `instructions` is optional and nullable and otherwise accepts string/object/array. Noul `criteria` is optional/nullable; `true` and `false` are optional/nullable structured values. Choice `criteria` is a required map whose values may be null or structured. Score `criteria` is a required array of **at least one** non-null structured value. Unknown System One request fields are retained and forwarded, including nested question and noul-criteria fields.
- `GET /v1/models`: `{models:[{name,description,release_date}]}` with all three local names. `release_date` is an empty string when the actual release date is unknown; set `JEV_GATEWAY_MODEL_RELEASE_DATE` if an authoritative date is available. No release date is invented.
- System One success: `{model,answers,usage}`. Answer IDs and types must match the request exactly. Usage requires nonnegative integer `input_tokens` and `output_tokens`, excluding booleans. Probability/confidence fields are finite and in `[0,1]`; distributions sum to approximately one (absolute tolerance 0.02). Choice keys must match criteria, with a selected maximal-probability option. Score legend and probability keys must match the supplied rubric; score must be finite and within its range. Unknown upstream response fields are rejected rather than leaked. The only mutation of an accepted upstream response is its model name.
- Invalid local input: 422 before upstream submission. Oversize body: 413. Invalid authentication: 401. Upstream 400/422 remains that status without fallback. Other upstream failure statuses are preserved on the final attempted route; malformed responses and transport/total-timeout failures return 502. Error bodies are generic, with no upstream messages, headers, model IDs, bodies, or credentials. For example, insufficient funds returns **402**, never a synthetic success.
- Non-finite JSON values and malformed Unicode are rejected throughout the request before persistence/submission. Valid Unicode and structured state are preserved.

The authoritative request-schema snapshot supplied for this implementation is `tests/official-openapi.json`. Deliberate stricter response and local-input safety checks are described above.

## Durable batches

```json
{
  "requests": [
    {
      "custom_id": "document-1",
      "body": {
        "model": "jev-latest",
        "state": "Please refund this purchase.",
        "questions": {"refund": {"type": "noul"}}
      }
    }
  ],
  "policy": {"free_wait_seconds": 600, "concurrency": 4, "allow_paid": true}
}
```

- `POST /v1/batches`: returns 202 and `{id,status,total,completed,failed,cancelled,pending,created_at,policy}`. All requests validate before a single transactional insertion. `custom_id` is unique within the batch, nonempty and at most 256 characters. Batch-envelope unknown fields are rejected. Default policy is the one above; `free_wait_seconds` supports 0–86400 seconds, concurrency 1–64.
- `GET /v1/batches/{id}`: same metadata; status is `in_progress`, `completed`, or `cancelled`. `completed` counts successful items; `failed` counts terminal failures. A finished batch with failed items still has status `completed`.
- `GET /v1/batches/{id}/results?offset=0&limit=100`: `{results:[{custom_id,status_code,response,error}],offset,limit,has_more,total}`. Limit 1–1000. **All item slots** are returned in original request order, including pending slots with null status/response/error. Total is always the complete item count, so page positions do not shift while items finish. Successful slots contain a normal System One response; failed/cancelled slots contain a generic error. Invalid offsets/limits return 422; unknown IDs return 404.
- `DELETE /v1/batches/{id}`: durable cancellation, stops further submissions and cancels local in-flight tasks. Completed/failed results are retained; unfinished slots get status 499 and a cancellation error. A request already submitted upstream cannot be recalled, and may still complete or incur a charge there.

Each item's wait starts at its **first processing attempt**, not batch admission. During the wait only the preferred route is attempted; failures requeue without occupying a worker. After the deadline an item permits ordinary fallback. `free_wait_seconds:0` permits immediate fallback. `allow_paid:false` waits/retries indefinitely without submitting to the alternate route, except terminal 400/422 validation errors. Online System One calls do not wait for batch deadlines.

Requests, ordering, first-attempt timestamps, policy, cancellation and terminal results persist in SQLite. Startup recovers `running` items to `queued`; completed items are not rerun. Recovery is **at least once**: a crash after upstream completion but before the database commit may repeat that item and its cost. Fixed worker tasks avoid one task per queued item. Global and per-batch concurrency are both enforced. Pending queue and batch admission are bounded; 429 indicates full queue. Completed data is retained indefinitely; arrange operator retention/backups according to data sensitivity and disk capacity.

SQLite operations run off the event loop. Database directory is 0700 and database file 0600. A process-level lock rejects a second owner of the same database. The database stores requests/results, not gateway credentials; it is not encrypted at rest. Use a dedicated private directory.

## Operator routing

The upstream endpoint is fixed to `https://opencode.ai/zen/v1/systemone`. Route order is `jev-1.13-free`, then `jev-1.13`; those identifiers never appear in API responses. The gateway sends an honest `User-Agent: Jev-Local-Gateway/1.0` and a stable `x-opencode-session` identifier. Set an explicit session ID to keep it stable across process restarts; otherwise it is generated once per startup.

Every preferred-route failure other than 400/422, including auth errors, malformed 200s, network failures and timeouts, enters a local cooldown. Default cooldown is 300 seconds. Online calls during cooldown go directly to the alternate route. On expiration exactly one half-open preferred-route probe is admitted, without holding the routing lock during I/O. Other online calls use the alternate route and waiting batches remain queued. A successful probe restores the preferred route; failure extends cooldown. Retry-After numeric/date values are bounded; invalid/non-finite headers are ignored. Cooldown is a local retry decision, **not a claim of upstream quota reset**, and is in-memory only across restart.

The shared HTTPX client is created and closed by lifespan; redirect following and environment proxies are disabled. Both HTTPX timeouts and a total per-attempt deadline apply. Gateway code never logs request bodies or keys; the bundled launcher disables access logs.

## Configuration

`jev_gateway` in the repository `config.yaml` (secrets in `.env`):

| Key | Meaning |
|---|---|
| `port` | Loopback port |
| `database` | Private durable queue database, relative to the repository root |
| `cooldown_seconds` | Preferred-route local cooldown |
| `retry_after_max_seconds` | Maximum upstream Retry-After extension; configured cooldown remains the floor |
| `upstream_timeout_seconds` | HTTPX and total per-attempt deadline |
| `upstream_concurrency` | Shared online/batch upstream concurrency cap |
| `max_body_bytes` | Wire request-body byte cap |
| `max_response_bytes` | Decoded upstream response byte cap |
| `batch_max_items` | Item cap per batch |
| `queue_limit` | Maximum queued/running items across batches |
| `batch_workers` | Fixed batch worker count |
| `retry_interval` | Batch poll/requeue interval in seconds |
| `session_id` | Explicit stable upstream session ID (empty = generated per process) |
| `model_release_date` | Verified metadata date, if known |

## Testing and injection

```sh
.venv/bin/python -m pytest tests -q
.venv/bin/python -m pip check
```

All bundled tests use **synthetic HTTPX transport and ASGI fixtures**, including the real official SDK parser/client with an ASGI transport adapter. They do not use live credentials or claim live provider E2E coverage. No service was deployed by this implementation.

`jev_gateway.app.create_app(settings=None, upstream_client=None, clock=None)` is the ASGI factory. Omitted settings load the environment through `Settings.from_env()`. Inject a `Settings` instance and an `httpx.AsyncClient` for isolated routing fault injection; injected clients remain caller-owned and are not closed by the gateway. `clock` optionally supplies a monotonic callable for cooldown testing. Lifespan exposes `app.state.gateway`, `app.state.store` and `app.state.batch_runner` for operator/test diagnostics; none are consumer endpoints. Batch persisted deadlines use epoch time.
