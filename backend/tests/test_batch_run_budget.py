"""Concurrent durable admissions share a persisted, per-run item budget."""

import asyncio

import pytest

from deerflow.config.database_config import DatabaseConfig
from deerflow.persistence.engine import close_engine, get_session_factory, init_engine_from_config
from deerflow.persistence.subagent_batches import SubagentBatchRepository


@pytest.mark.asyncio
async def test_atomic_budget_retry_terminal_and_new_run(tmp_path):
    await init_engine_from_config(DatabaseConfig(backend="sqlite", sqlite_dir=str(tmp_path)))
    try:
        repo = SubagentBatchRepository(get_session_factory())

        async def submit(key, run_id="run-1", count=600):
            return await repo.create_batch(
                batch_id=key,
                user_id="user-1",
                thread_id="thread-1",
                run_id=run_id,
                tool_call_id=key,
                submission_key=f"{run_id}:{key}",
                title="wave",
                subagent_type="general-purpose",
                items=[{"key": str(i), "prompt": "Read one group"} for i in range(count)],
                max_live_items=1000,
                max_running_items=32,
                max_attempts=1,
                execution_spec={},
                max_items_per_run=1000,
            )

        outcomes = await asyncio.gather(submit("wave-1"), submit("wave-2"), return_exceptions=True)
        accepted = [result for result in outcomes if isinstance(result, dict)]
        rejected = [result for result in outcomes if isinstance(result, ValueError)]
        assert len(accepted) == len(rejected) == 1
        assert "Per-run batch item limit" in str(rejected[0])
        first = accepted[0]
        assert (await submit(first["id"]))["id"] == first["id"]  # Replay does not spend again.
        await repo.cancel_batch(first["id"], user_id="user-1")
        with pytest.raises(ValueError, match="limit"):
            await submit("wave-3")  # Terminal items still count.
        assert (await submit("wave-4", count=400))["total_items"] == 400
        assert (await submit("wave-5", run_id="run-2", count=1000))["total_items"] == 1000
    finally:
        await close_engine()
