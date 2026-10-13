"""Concurrent same-file reads must not starve the pool needed by their owner."""

import os
import subprocess
import sys


def test_parallel_reads_leave_threads_available_for_the_lock_holder():
    source = r"""
import asyncio
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from unittest.mock import MagicMock
from langchain_core.messages import ToolMessage
from langgraph.prebuilt.tool_node import ToolCallRequest
import deerflow.agents.middlewares.read_before_write_middleware as mod

@asynccontextmanager
async def auth(_runtime):
    yield
mod.sandbox_authorization_scope_async = auth

async def main():
    asyncio.get_running_loop().set_default_executor(ThreadPoolExecutor(max_workers=2))
    middleware = mod.ReadBeforeWriteMiddleware(content_reader=lambda _r, _p: 'skill content')
    runtime = MagicMock()
    runtime.context = {'thread_id': 'shared-research'}
    async def read(index):
        request = ToolCallRequest(tool_call={'name':'read_file', 'args':{'path':'/mnt/skills/shared/SKILL.md'}, 'id':str(index)}, tool=None, state={'messages':[]}, runtime=runtime)
        async def handler(_request):
            value = await asyncio.to_thread(lambda: 'skill content')
            return ToolMessage(content=value, name='read_file', tool_call_id=str(index))
        result = await middleware.awrap_tool_call(request, handler)
        assert result.content == 'skill content'
        assert result.additional_kwargs['deerflow_read_mark']['path'] == '/mnt/skills/shared/SKILL.md'
    await asyncio.gather(*(read(i) for i in range(32)))
    print('completed 32')
asyncio.run(main())
"""
    # Isolate the old deadlock: timing out must not leave blocked threads in pytest.
    result = subprocess.run([sys.executable, "-c", source], capture_output=True, text=True, timeout=8, env=dict(os.environ))
    assert result.returncode == 0, result.stderr
    assert "completed 32" in result.stdout
