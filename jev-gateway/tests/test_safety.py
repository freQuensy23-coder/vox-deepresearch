import asyncio
import json

import httpx
import pytest

from test_gateway import BODY, LEAKS, answer, batch, completed, service


@pytest.mark.asyncio
async def test_total_timeout_is_502(tmp_path):
    calls = []
    async def handler(request):
        calls.append(request)
        await asyncio.sleep(0.08)
        return httpx.Response(200, json=answer(json.loads(request.content)))
    async with service(tmp_path, handler, upstream_timeout_seconds=0.01) as (client, app):
        response = await client.post("/v1/systemone", json=BODY)
        assert response.status_code == 502
        assert len(calls) == 1


def test_entrypoint_enforces_localhost_single_worker(monkeypatch):
    import jev_gateway.__main__ as entry
    calls = []
    monkeypatch.setattr(entry.uvicorn, "run", lambda *a, **kw: calls.append((a, kw)))
    entry.main()
    assert calls == [(("jev_gateway.app:create_app",), {"factory": True, "host": "127.0.0.1", "port": 18791, "workers": 1, "access_log": False})]


def test_blank_keys_are_rejected():
    from jev_gateway.app import Settings
    from pydantic import ValidationError
    valid = {"api_key": "test", "upstream_key": "test", "database": "jobs.db", "upstream_url": "https://decision.invalid/systemone", "deployment": "decision-1"}
    Settings(**valid)
    for key in ["api_key", "upstream_key", "deployment"]:
        with pytest.raises(ValidationError):
            Settings(**dict(valid, **{key: " " if key != "deployment" else ""}))
    with pytest.raises(ValidationError):
        Settings(**dict(valid, upstream_url="http://decision.invalid/systemone"))


@pytest.mark.asyncio
async def test_release_date_not_invented(tmp_path):
    async with service(tmp_path, lambda r: httpx.Response(500)) as (client, app):
        assert all(m["release_date"] == "" for m in (await client.get("/v1/models")).json()["models"])


@pytest.mark.asyncio
async def test_cancel_batch_inflight_does_not_submit_followups(tmp_path):
    entered = asyncio.Event()
    calls = []
    async def handler(request):
        calls.append(request)
        entered.set()
        await asyncio.Event().wait()
    async with service(tmp_path, handler, batch_workers=2) as (client, app):
        payload = batch(3)
        payload["policy"]["concurrency"] = 1
        created = (await client.post("/v1/batches", json=payload)).json()
        await asyncio.wait_for(entered.wait(), 1)
        assert (await client.delete(f'/v1/batches/{created["id"]}')).status_code == 200
        await asyncio.sleep(0.05)
        assert len(calls) == 1
        assert (await client.get(f'/v1/batches/{created["id"]}')).json()["cancelled"] == 3


@pytest.mark.asyncio
async def test_batch_upstream_failure_is_durable_sanitized(tmp_path):
    async with service(tmp_path, lambda r: httpx.Response(402, text="Microsoft decision-1 at decision.invalid: synthetic-upstream out of funds")) as (client, app):
        created = (await client.post("/v1/batches", json=batch(1))).json()
        assert (await completed(client, created["id"]))["failed"] == 1
        response = await client.get(f'/v1/batches/{created["id"]}/results')
        result = response.json()["results"][0]
        assert result["status_code"] == 402
        assert result["response"] is None
        assert result["error"]["type"] == "service_error"
        assert not any(leak in response.text.lower() for leak in LEAKS)


@pytest.mark.asyncio
async def test_shared_client_and_store_shutdown_ownership(tmp_path):
    from jev_gateway.app import Settings, create_app
    upstream = httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(500)))
    settings = Settings(api_key="x", upstream_key="x", upstream_url="https://decision.invalid/systemone", deployment="decision-1", database=str(tmp_path / "private" / "jobs.db"))
    app = create_app(settings=settings, upstream_client=upstream)
    async with app.router.lifespan_context(app):
        with pytest.raises(RuntimeError, match="already owned"):
            second = create_app(settings=settings, upstream_client=upstream)
            async with second.router.lifespan_context(second):
                pass
    assert all(t.done() for t in app.state.batch_runner.workers)
    assert not upstream.is_closed
    await upstream.aclose()


@pytest.mark.asyncio
async def test_sdk_adapter_with_synthetic_asgi_transport(tmp_path):
    import httpx2
    from typesafe_sdk import AsyncTypeSafeClient, Choice, Noul, Score
    async with service(tmp_path, lambda r: httpx.Response(200, json=answer(json.loads(r.content)))) as (client, app):
        class Adapter(httpx2.AsyncBaseTransport):
            async def handle_async_request(self, request):
                response = await client.request(request.method, request.url.path, content=await request.aread(), headers=dict(request.headers))
                return httpx2.Response(response.status_code, headers=dict(response.headers), content=response.content)
        async with AsyncTypeSafeClient(api_key="synthetic-local", base_url="http://test", transport=Adapter()) as sdk:
            response = await sdk.system_one(state={"Русский": "😀"}, questions={"n": Noul(instructions="yes?"), "c": Choice(criteria={"yes": None, "no": "No"}), "s": Score(criteria=["single"])} )
            assert response.model == "jev-1.13.0"
            assert response.answers["n"].noul == 0.75
            models = await sdk.models.list()
            assert {m.name for m in models.models} == {"jev-latest", "jev-preview", "jev-1.13.0"}


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", ["choice_missing_probability", "choice_invalid_choice", "choice_confidence_bool", "choice_distribution", "score_invalid_legend", "score_out_of_range", "score_probability_nan"])
async def test_choice_score_response_contract(tmp_path, mutation):
    body = dict(BODY, questions={"c": {"type": "choice", "criteria": {"a": None,"b": "b"}}, "s": {"type": "score", "criteria": ["one", {"data": "two"}]}})
    def handler(request):
        result = answer(body)
        if mutation == "choice_missing_probability": del result["answers"]["c"]["probabilities"]["b"]
        if mutation == "choice_invalid_choice": result["answers"]["c"]["choice"] = "other"
        if mutation == "choice_confidence_bool": result["answers"]["c"]["confidence"] = True
        if mutation == "choice_distribution": result["answers"]["c"]["probabilities"] = {"a": 0.1, "b": 0.1}
        if mutation == "score_invalid_legend": result["answers"]["s"]["legend"]["0"] = "wrong"
        if mutation == "score_out_of_range": result["answers"]["s"]["score"] = 2.0
        if mutation == "score_probability_nan": result["answers"]["s"]["probabilities"]["0"] = float("nan")
        return httpx.Response(200, content=json.dumps(result))
    async with service(tmp_path, handler) as (client, app):
        assert (await client.post("/v1/systemone", json=body)).status_code == 502
