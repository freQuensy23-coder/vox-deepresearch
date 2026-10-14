import asyncio
import json
import socket
import tempfile
from pathlib import Path

import httpx
import uvicorn

from jev_gateway.app import Settings, create_app


class FaultThenReal(httpx.AsyncBaseTransport):
    def __init__(self):
        self.real = httpx.AsyncHTTPTransport()
        self.inject = True
        self.events = []

    async def handle_async_request(self, request):
        payload = json.loads(request.content)
        route = payload["model"]
        if route == "jev-1.13-free" and self.inject:
            self.inject = False
            self.events.append({"route": route, "status": 429, "injected": True})
            return httpx.Response(429, json={"error": "Controlled free-route failure"})
        response = await self.real.handle_async_request(request)
        self.events.append({"route": route, "status": response.status_code, "injected": False})
        return response

    async def aclose(self):
        await self.real.aclose()


async def main():
    configured = Settings.from_config()
    key = configured.api_key
    headers = {"Authorization": "Bearer " + key}
    clock = [1000.0]
    scratch = Path(tempfile.mkdtemp(prefix="jev-live-routing-"))
    database = scratch / "routing.sqlite3"
    settings = Settings(api_key=key, upstream_key=configured.upstream_key, database=str(database), cooldown_seconds=300, session_id="jev-live-routing-verification", retry_interval=0.05)
    transport = FaultThenReal()
    server = None
    task = None
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    port = listener.getsockname()[1]
    try:
        async with httpx.AsyncClient(transport=transport, trust_env=False, follow_redirects=False) as upstream:
            app = create_app(settings=settings, upstream_client=upstream, clock=lambda: clock[0])
            server = uvicorn.Server(uvicorn.Config(app, log_level="error", access_log=False))
            task = asyncio.create_task(server.serve(sockets=[listener]))
            async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}", trust_env=False, timeout=180) as http:
                for _ in range(100):
                    if server.started:
                        break
                    await asyncio.sleep(0.05)
                assert server.started
                payload = {"model": "jev-latest", "state": "The service is down for all customers.", "questions": {"urgent": {"type": "noul", "instructions": "Does this require immediate attention?"}}}
                results = []
                for advance in [0, 0, 301]:
                    clock[0] += advance
                    r = await http.post("/v1/systemone", headers=headers, json=payload)
                    assert r.status_code == 200, r.text
                    answer = r.json()
                    assert answer["model"] == "jev-1.13.0"
                    assert answer["answers"]["urgent"]["noul"] > 0.8
                    assert "opencode" not in r.text.lower() and "free" not in r.text.lower()
                    results.append(answer)
                assert [e["route"] for e in transport.events] == ["jev-1.13-free", "jev-1.13", "jev-1.13", "jev-1.13-free"]
                assert all(e["status"] == 200 for e in transport.events[1:])
                print(json.dumps({"verification": "localhost HTTP with one injected free 429, real paid fallback/cooldown calls, advanced test clock then real free recovery", "events": transport.events, "responses": results}))
    finally:
        if server is not None:
            server.should_exit = True
        if task is not None:
            await task
        listener.close()
        for p in scratch.iterdir():
            p.unlink()
        scratch.rmdir()


if __name__ == "__main__":
    asyncio.run(main())
