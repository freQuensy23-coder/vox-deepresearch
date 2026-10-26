import asyncio
import json

import httpx

from .models import validate_response


ERROR = {"error": {"type": "service_error", "message": "Jev request could not be completed."}}


class GatewayError(Exception):
    def __init__(self, status=503, retry_after=None):
        self.status = status
        self.retry_after = retry_after


class Router:
    """Sends every System One call to the configured Microsoft-Decision-1 deployment."""

    def __init__(self, settings, client):
        self.settings = settings
        self.client = client
        self.semaphore = asyncio.Semaphore(settings.upstream_concurrency)

    async def evaluate(self, request):
        async with self.semaphore:
            try:
                async with asyncio.timeout(self.settings.upstream_timeout_seconds):
                    return await self.perform(request)
            except TimeoutError:
                raise GatewayError(502) from None

    async def perform(self, request):
        payload = request.model_dump(exclude_unset=True)
        payload["model"] = self.settings.deployment
        headers = {"api-key": self.settings.upstream_key, "User-Agent": "Jev-Local-Gateway/1.0"}
        try:
            async with self.client.stream("POST", self.settings.upstream_url, json=payload, headers=headers, timeout=self.settings.upstream_timeout_seconds) as response:
                if response.status_code != 200:
                    status = response.status_code if 400 <= response.status_code < 600 else 502
                    raise GatewayError(status, response.headers.get("Retry-After"))
                data = bytearray()
                async for chunk in response.aiter_bytes():
                    data.extend(chunk)
                    if len(data) > self.settings.max_response_bytes:
                        raise GatewayError(502)
                return validate_response(json.loads(data), request)
        except GatewayError:
            raise
        except (httpx.HTTPError, ValueError, TypeError, RecursionError, UnicodeError, OverflowError):
            raise GatewayError(502) from None
