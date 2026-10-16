import asyncio
import json
import math
import time
import uuid
from email.utils import parsedate_to_datetime

import httpx

from .models import validate_response


ERROR = {"error": {"type": "service_error", "message": "Jev request could not be completed."}}


class GatewayError(Exception):
    def __init__(self, status=503, retry_after=None):
        self.status = status
        self.retry_after = retry_after


class Router:
    def __init__(self, settings, client, clock=time.monotonic):
        self.settings = settings
        self.client = client
        self.clock = clock
        self.lock = asyncio.Lock()
        self.semaphore = asyncio.Semaphore(settings.upstream_concurrency)
        self.cool_until = 0
        self.probing = False
        self.generation = 0
        self.session = settings.session_id or str(uuid.uuid4())

    def delay(self, header):
        delay = self.settings.cooldown_seconds
        if header:
            try:
                extra = float(header)
            except (ValueError, TypeError):
                try:
                    extra = parsedate_to_datetime(header).timestamp() - time.time()
                except (ValueError, TypeError, OverflowError):
                    extra = 0
            if math.isfinite(extra):
                delay = max(delay, min(max(extra, 0), self.settings.retry_after_max_seconds))
        return delay

    async def attempt(self, request, model):
        try:
            async with asyncio.timeout(self.settings.upstream_timeout_seconds):
                return await self.perform(request, model)
        except TimeoutError:
            raise GatewayError(502) from None

    async def perform(self, request, model):
        payload = request.model_dump(exclude_unset=True)
        payload["model"] = model
        headers = {"Authorization": f"Bearer {self.settings.upstream_key}", "User-Agent": "Jev-Local-Gateway/1.0", "x-opencode-session": self.session}
        try:
            async with self.client.stream("POST", "https://opencode.ai/zen/v1/systemone", json=payload, headers=headers, timeout=self.settings.upstream_timeout_seconds) as response:
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

    async def evaluate(self, request):
        """Free route while it answers. Its first failure switches every call to the paid
        route at once; after the cooldown one call probes the free route, and its success
        switches everything back."""
        async with self.semaphore:
            async with self.lock:
                now = self.clock()
                probe = self.cool_until > 0 and now >= self.cool_until and not self.probing
                free = self.cool_until == 0 or probe
                generation = self.generation
                if probe:
                    self.probing = True
            if free:
                try:
                    result = await self.attempt(request, "jev-1.13-free")
                except asyncio.CancelledError:
                    if probe:
                        async with self.lock:
                            self.probing = False
                            self.cool_until = max(self.cool_until, self.clock() + self.settings.cooldown_seconds)
                    raise
                except GatewayError as error:
                    async with self.lock:
                        if probe:
                            self.probing = False
                        if error.status not in {400, 422}:
                            self.generation += 1
                            self.cool_until = max(self.cool_until, self.clock() + self.delay(error.retry_after))
                    if error.status in {400, 422}:
                        raise
                else:
                    if probe:
                        async with self.lock:
                            self.probing = False
                            if generation == self.generation:
                                self.cool_until = 0
                    return result
            return await self.attempt(request, "jev-1.13")
