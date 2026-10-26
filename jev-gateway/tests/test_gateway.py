import asyncio
import copy
import json
from contextlib import asynccontextmanager

import httpx
import pytest


BODY = {"model": "jev-latest", "state": {"message": "hello"}, "questions": {"q": {"type": "noul"}}}
UPSTREAM_URL = "https://decision.invalid/providers/microsoft/v1/systemone"
DEPLOYMENT = "decision-1"
# What a consumer must never see: the provider, the deployment, the upstream host or key.
LEAKS = ["microsoft", DEPLOYMENT, "decision.invalid", "synthetic-upstream"]


def answer(body, model="microsoft-decision-1"):
    answers = {}
    for key, question in body["questions"].items():
        kind = question["type"]
        if kind == "noul":
            answers[key] = {"type": kind, "noul": 0.75}
        elif kind == "choice":
            keys = list(question["criteria"])
            answers[key] = {"type": kind, "choice": keys[0], "confidence": 1.0, "probabilities": {k: float(i == 0) for i, k in enumerate(keys)}}
        else:
            legend = {str(i): v for i, v in enumerate(question["criteria"])}
            answers[key] = {"type": kind, "score": 0.0, "confidence": 1.0, "legend": legend, "probabilities": {k: float(k == "0") for k in legend}}
    return {"model": model, "answers": answers, "usage": {"input_tokens": 5, "output_tokens": 2}}


@asynccontextmanager
async def service(tmp_path, handler, **kwargs):
    from jev_gateway.app import Settings, create_app
    settings = Settings(upstream_key="synthetic-upstream", api_key="synthetic-local", upstream_url=UPSTREAM_URL, deployment=DEPLOYMENT, database=str(tmp_path / "private" / "jobs.sqlite3"), retry_interval=0.015, **kwargs)
    upstream = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    app = create_app(settings=settings, upstream_client=upstream)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app, raise_app_exceptions=False), base_url="http://test", headers={"Authorization": "Bearer synthetic-local"}) as client:
            yield client, app
    await upstream.aclose()


@pytest.mark.asyncio
async def test_success_contract(tmp_path):
    calls = []
    def handler(request):
        calls.append(request)
        return httpx.Response(200, json=answer(json.loads(request.content)))
    async with service(tmp_path, handler) as (client, app):
        for alias in ["jev-latest", "jev-preview", "jev-1.13.0"]:
            body = dict(BODY, model=alias)
            result = await client.post("/v1/systemone", json=body)
            assert result.status_code == 200
            assert result.json() == answer(BODY, "jev-1.13.0")
        assert [str(r.url) for r in calls] == [UPSTREAM_URL] * 3
        assert [json.loads(r.content)["model"] for r in calls] == [DEPLOYMENT] * 3
        assert all(r.headers["User-Agent"] == "Jev-Local-Gateway/1.0" for r in calls)
        assert all(r.headers["api-key"] == "synthetic-upstream" and "Authorization" not in r.headers for r in calls)
        models = (await client.get("/v1/models")).json()
        assert {m["name"] for m in models["models"]} == {"jev-latest", "jev-preview", "jev-1.13.0"}
        assert all(set(m) == {"name", "description", "release_date"} for m in models["models"])
        assert not any(leak in json.dumps(models).lower() for leak in LEAKS)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [400, 401, 402, 403, 404, 422, 429, 500, 503, "timeout", "malformed"])
async def test_upstream_failure_keeps_its_status_and_is_sanitized(tmp_path, failure):
    calls = []
    def handler(request):
        calls.append(request)
        if failure == "timeout":
            raise httpx.ReadTimeout("synthetic secret provider", request=request)
        if failure == "malformed":
            return httpx.Response(200, json={"model": "microsoft-decision-1"})
        return httpx.Response(failure, text="Microsoft decision-1 at decision.invalid: synthetic-upstream rejected")
    async with service(tmp_path, handler) as (client, app):
        response = await client.post("/v1/systemone", json=BODY)
        assert response.status_code == (502 if failure in ("timeout", "malformed") else failure)
        assert response.json() == {"error": {"type": "service_error", "message": "Jev request could not be completed."}}
        assert len(calls) == 1
        assert not any(leak in response.text.lower() for leak in LEAKS)


@pytest.mark.asyncio
async def test_bound_upstream_concurrency(tmp_path):
    active = maximum = 0
    async def handler(request):
        nonlocal active, maximum
        active += 1
        maximum = max(maximum, active)
        await asyncio.sleep(0.02)
        active -= 1
        return httpx.Response(200, json=answer(json.loads(request.content)))
    async with service(tmp_path, handler, upstream_concurrency=2) as (client, app):
        results = await asyncio.gather(*(client.post("/v1/systemone", json=BODY) for _ in range(10)))
        assert all(r.status_code == 200 for r in results)
        assert maximum == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("change", [{"model": DEPLOYMENT}, {"model": "unknown"}, {"state": None}, {"state": 1}, {"questions": {}}, {"questions": {"q": {"type": "score", "criteria": []}}}, {"questions": {"q": {"type": "score", "criteria": [None]}}}, {"questions": {"q": {"type": "choice", "criteria": {"x": 1}}}}, {"questions": {"q": {"type": "noul", "instructions": 3}}}, {"questions": {"q": {"type": "noul", "criteria": []}}}])
async def test_invalid_request_no_upstream(tmp_path, change):
    calls = []
    async with service(tmp_path, lambda r: calls.append(r)) as (client, app):
        response = await client.post("/v1/systemone", json=dict(BODY, **change))
        assert response.status_code == 422
        assert calls == []
        assert DEPLOYMENT not in response.text


@pytest.mark.asyncio
async def test_structured_optional_nullable_and_unknown_fields_forward(tmp_path):
    calls = []
    def handler(request):
        body = json.loads(request.content)
        calls.append(body)
        return httpx.Response(200, json=answer(body))
    body = dict(BODY, future={"a": 2}, questions={"n": {"type": "noul", "criteria": {"true": None, "false": ["x"]}, "extra": 1}, "c": {"type": "choice", "instructions": None, "criteria": {"a": None, "b": {"data": 1}}}, "s": {"type": "score", "instructions": {"task": "x"}, "criteria": [["one"]]}})
    async with service(tmp_path, handler) as (client, app):
        assert (await client.post("/v1/systemone", json=body)).status_code == 200
        assert calls == [dict(body, model=DEPLOYMENT)]


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", ["missing_answer", "extra_answer", "wrong_type", "bool_usage", "missing_usage", "negative_usage", "nan", "infinity", "out_of_range", "extra_metadata"])
async def test_invalid_response_is_sanitized(tmp_path, mutation):
    def handler(request):
        result = answer(BODY)
        if mutation == "missing_answer": result["answers"] = {}
        if mutation == "extra_answer": result["answers"]["extra"] = result["answers"]["q"]
        if mutation == "wrong_type": result["answers"]["q"]["type"] = "score"
        if mutation == "bool_usage": result["usage"]["input_tokens"] = True
        if mutation == "missing_usage": del result["usage"]["input_tokens"]
        if mutation == "negative_usage": result["usage"]["input_tokens"] = -1
        if mutation == "nan": result["answers"]["q"]["noul"] = float("nan")
        if mutation == "infinity": result["answers"]["q"]["noul"] = float("inf")
        if mutation == "out_of_range": result["answers"]["q"]["noul"] = 2
        if mutation == "extra_metadata": result["provider"] = "microsoft"
        return httpx.Response(200, content=json.dumps(result))
    async with service(tmp_path, handler) as (client, app):
        response = await client.post("/v1/systemone", json=BODY)
        assert response.status_code == 502
        assert not any(leak in response.text.lower() for leak in LEAKS)


@pytest.mark.asyncio
async def test_auth_size_raw_nonfinite_and_unicode(tmp_path):
    calls = []
    async with service(tmp_path, lambda r: calls.append(r), max_body_bytes=1024) as (client, app):
        assert (await client.get("/v1/models", headers={"Authorization": "Bearer wrong"})).status_code == 401
        assert (await client.post("/v1/systemone", content=b"x" * 1025)).status_code == 413
        for value in ["NaN", "Infinity", "-Infinity", '"\\ud800"']:
            raw = '{"model":"jev-latest","state":{"x":' + value + '},"questions":{"q":{"type":"noul"}}}'
            assert (await client.post("/v1/systemone", content=raw, headers={"Content-Type": "application/json"})).status_code == 422
        assert calls == []


async def completed(client, batch_id):
    for _ in range(150):
        response = await client.get(f"/v1/batches/{batch_id}")
        if response.json()["status"] in {"completed", "cancelled"}:
            return response.json()
        await asyncio.sleep(0.01)
    pytest.fail("batch did not finish")


def batch(count=3, **policy):
    return {"requests": [{"custom_id": str(i), "body": copy.deepcopy(BODY)} for i in range(count)], "policy": {"concurrency": 2, **policy}}


@pytest.mark.asyncio
async def test_batches_pagination_duplicates_bounds_permissions(tmp_path):
    async with service(tmp_path, lambda r: httpx.Response(200, json=answer(json.loads(r.content))), batch_max_items=4, queue_limit=5) as (client, app):
        duplicate = batch()
        duplicate["requests"][1]["custom_id"] = "0"
        assert (await client.post("/v1/batches", json=duplicate)).status_code == 422
        assert (await client.post("/v1/batches", json=batch(5))).status_code == 422
        result = await client.post("/v1/batches", json=batch())
        assert result.status_code == 202
        batch_id = result.json()["id"]
        final = await completed(client, batch_id)
        assert final["completed"] == 3
        first = (await client.get(f"/v1/batches/{batch_id}/results?offset=0&limit=2")).json()
        second = (await client.get(f"/v1/batches/{batch_id}/results?offset=2&limit=2")).json()
        assert [r["custom_id"] for r in first["results"] + second["results"]] == ["0", "1", "2"]
        assert first["has_more"] is True and second["has_more"] is False
        assert first["total"] == second["total"] == 3
        assert all(r["response"]["model"] == "jev-1.13.0" for r in first["results"])
        assert app.state.store.path.stat().st_mode & 0o777 == 0o600
        assert app.state.store.path.parent.stat().st_mode & 0o777 == 0o700
        assert (await client.get("/v1/batches/nonexistent")).status_code == 404


@pytest.mark.asyncio
async def test_batch_retries_rate_limited_items_instead_of_failing_them(tmp_path):
    calls = []
    def handler(request):
        calls.append(request)
        if len(calls) <= 2:
            return httpx.Response(429)
        return httpx.Response(200, json=answer(json.loads(request.content)))
    async with service(tmp_path, handler) as (client, app):
        created = (await client.post("/v1/batches", json=batch(1, concurrency=1))).json()
        final = await completed(client, created["id"])
        assert final["completed"] == 1 and final["failed"] == 0
        assert len(calls) == 3


@pytest.mark.asyncio
async def test_batch_cancel_and_queue_bound(tmp_path):
    calls = []
    entered = asyncio.Event()
    async def handler(request):
        calls.append(json.loads(request.content)["model"])
        entered.set()
        await asyncio.Event().wait()
    async with service(tmp_path, handler, queue_limit=2) as (client, app):
        created = (await client.post("/v1/batches", json=batch(2))).json()
        await asyncio.wait_for(entered.wait(), 1)
        assert (await client.post("/v1/batches", json=batch(1))).status_code == 429
        assert (await client.delete(f'/v1/batches/{created["id"]}')).json()["status"] == "cancelled"
        count = len(calls)
        await asyncio.sleep(0.1)
        assert len(calls) == count
        results = (await client.get(f'/v1/batches/{created["id"]}/results')).json()
        assert results["total"] == 2
        assert all(r["status_code"] == 499 for r in results["results"])


@pytest.mark.asyncio
async def test_batch_restart_preserves_completed_and_recovers_running(tmp_path):
    entered = asyncio.Event()
    calls = []
    async def handler(request):
        body = json.loads(request.content)
        calls.append(body["state"])
        if body["state"] == "block":
            entered.set()
            await asyncio.Event().wait()
        return httpx.Response(200, json=answer(body))
    payload = batch(2)
    payload["policy"]["concurrency"] = 1
    payload["requests"][1]["body"]["state"] = "block"
    async with service(tmp_path, handler, batch_workers=1) as (client, app):
        created = (await client.post("/v1/batches", json=payload)).json()
        await asyncio.wait_for(entered.wait(), 1)
    def recovered(request):
        body = json.loads(request.content)
        calls.append(body["state"])
        return httpx.Response(200, json=answer(body))
    async with service(tmp_path, recovered, batch_workers=1) as (client, app):
        assert (await completed(client, created["id"]))["completed"] == 2
        assert calls.count({"message": "hello"}) == 1
        assert calls.count("block") == 2


@pytest.mark.asyncio
async def test_extra_usage_fields_are_accepted_and_not_passed_on(tmp_path):
    def handler(request):
        result = answer(BODY)
        result["usage"].update(cost=0.00001302, billing={"source": "balance", "tokens": {"input": 5, "output": 2}})
        return httpx.Response(200, content=json.dumps(result))
    async with service(tmp_path, handler) as (client, app):
        response = await client.post("/v1/systemone", json=BODY)
        assert response.status_code == 200
        assert response.json()["usage"] == {"input_tokens": 5, "output_tokens": 2}
        assert "billing" not in response.text and "cost" not in response.text
