import asyncio
import hmac
import json
import os
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
import yaml
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, ConfigDict, Field, field_validator

from .batches import BatchRunner, Store
from .models import ALIASES, BatchRequest, ModelMetadataList, SystemOneRequest, SystemOneResponse, safe_json
from .routing import ERROR, GatewayError, Router


class Settings(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    upstream_key: str = Field(min_length=1, repr=False)
    api_key: str = Field(min_length=1, repr=False)
    database: str
    cooldown_seconds: float = Field(default=300, gt=0)
    retry_after_max_seconds: float = Field(default=3600, ge=0)
    upstream_timeout_seconds: float = Field(default=120, gt=0)
    upstream_concurrency: int = Field(default=8, ge=1, le=128)
    max_body_bytes: int = Field(default=16777216, ge=1)
    max_response_bytes: int = Field(default=16777216, ge=1)
    batch_max_items: int = Field(default=10000, ge=1)
    queue_limit: int = Field(default=20000, ge=1)
    batch_workers: int = Field(default=8, ge=1, le=64)
    retry_interval: float = Field(default=1, gt=0)
    session_id: str = ""
    model_release_date: str = ""

    @field_validator("upstream_key", "api_key")
    @classmethod
    def nonblank_key(cls, value):
        if not value.strip():
            raise ValueError("Key must not be blank")
        return value

    @classmethod
    def from_config(cls):
        """`jev_gateway` in the repository config.yaml; keys from the repository .env."""
        values = {key: value for key, value in config_section().items() if key != "port"}
        values["database"] = str(REPO_ROOT / values["database"])
        load_dotenv(REPO_ROOT / ".env", override=False)
        return cls(upstream_key=os.environ.get("OPENCODE_GO_SUBSCRIPTION_KEY", ""), api_key=os.environ.get("JEV_GATEWAY_API_KEY", ""), **values)


REPO_ROOT = Path(__file__).resolve().parents[2]


def config_section():
    return yaml.safe_load((REPO_ROOT / "config.yaml").read_text(encoding="utf-8"))["jev_gateway"]


class BoundaryMiddleware:
    def __init__(self, app, max_bytes):
        self.app = app
        self.max_bytes = max_bytes

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)
        data = bytearray()
        while True:
            message = await receive()
            if message["type"] == "http.disconnect":
                return
            data.extend(message.get("body", b""))
            if len(data) > self.max_bytes:
                return await JSONResponse({"error": {"type": "request_error", "message": "Request body too large."}}, status_code=413)(scope, receive, send)
            if not message.get("more_body", False):
                break
        if scope["method"] == "POST" and data:
            try:
                safe_json(json.loads(data))
            except (ValueError, TypeError, UnicodeError, RecursionError, OverflowError):
                return await JSONResponse({"detail": [{"loc": ["body"], "msg": "Invalid JSON body", "type": "value_error"}]}, status_code=422)(scope, receive, send)
        delivered = False
        async def replay():
            nonlocal delivered
            if delivered:
                return await receive()
            delivered = True
            return {"type": "http.request", "body": bytes(data), "more_body": False}
        await self.app(scope, replay, send)


def create_app(settings=None, upstream_client=None, clock=None):
    settings = settings or Settings.from_config()
    bearer = HTTPBearer(auto_error=False)

    async def authenticate(credentials: HTTPAuthorizationCredentials | None = Depends(bearer)):
        token = credentials.credentials if credentials is not None else ""
        if not hmac.compare_digest(token.encode("utf-8"), settings.api_key.encode("utf-8")):
            raise HTTPException(401, detail="Invalid gateway credentials", headers={"WWW-Authenticate": "Bearer"})

    @asynccontextmanager
    async def lifespan(app):
        client = upstream_client or httpx.AsyncClient(follow_redirects=False, trust_env=False, limits=httpx.Limits(max_connections=settings.upstream_concurrency, max_keepalive_connections=settings.upstream_concurrency))
        store = None
        runner = None
        try:
            store = await asyncio.to_thread(Store, settings.database, settings.queue_limit)
            router = Router(settings, client, **({"clock": clock} if clock else {}))
            runner = BatchRunner(store, router, settings)
            app.state.store = store
            app.state.gateway = router
            app.state.batch_runner = runner
            runner.start()
            yield
        finally:
            if runner is not None:
                await runner.stop()
            if store is not None:
                await store.call("close")
            if upstream_client is None:
                await client.aclose()

    app = FastAPI(title="Jev", version="1.0.0", lifespan=lifespan)
    app.add_middleware(BoundaryMiddleware, max_bytes=settings.max_body_bytes)

    @app.exception_handler(GatewayError)
    async def gateway_error(request, error):
        return JSONResponse(ERROR, status_code=error.status)

    @app.exception_handler(RequestValidationError)
    async def validation_error(request, error):
        detail = [{"loc": ["body"], "msg": "Invalid request", "type": "value_error"}]
        return JSONResponse({"detail": detail}, status_code=422)

    @app.get("/healthz", include_in_schema=False)
    async def healthz():
        alive = all(not worker.done() for worker in app.state.batch_runner.workers)
        return JSONResponse({"status": "ok" if alive else "unavailable"}, status_code=200 if alive else 503)

    @app.get("/v1/models", response_model=ModelMetadataList, dependencies=[Depends(authenticate)])
    async def models():
        return {"models": [{"name": name, "description": "Jev System One model.", "release_date": settings.model_release_date} for name in ALIASES]}

    @app.post("/v1/systemone", response_model=SystemOneResponse, dependencies=[Depends(authenticate)])
    async def systemone(body: SystemOneRequest):
        return JSONResponse(await app.state.gateway.evaluate(body))

    @app.post("/v1/batches", status_code=202, dependencies=[Depends(authenticate)])
    async def create_batch(body: BatchRequest):
        if len(body.requests) > settings.batch_max_items:
            raise HTTPException(422, detail="Batch has too many requests")
        return await app.state.store.call("insert", body)

    @app.get("/v1/batches/{batch_id}", dependencies=[Depends(authenticate)])
    async def get_batch(batch_id: str):
        return await app.state.store.call("metadata", batch_id)

    @app.get("/v1/batches/{batch_id}/results", dependencies=[Depends(authenticate)])
    async def get_results(batch_id: str, offset: int = Query(default=0, ge=0), limit: int = Query(default=100, ge=1, le=1000)):
        return await app.state.store.call("results", batch_id, offset, limit)

    @app.delete("/v1/batches/{batch_id}", dependencies=[Depends(authenticate)])
    async def cancel_batch(batch_id: str):
        return await app.state.batch_runner.cancel(batch_id)

    return app
