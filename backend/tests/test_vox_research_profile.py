"""The shipped research profile must enable its actual native execution tools."""

from pathlib import Path

import yaml

from deerflow.config.subagent_batches_config import SubagentBatchesConfig
from deerflow.config.subagent_runtime_config import SubagentRuntimeConfig
from deerflow.config.subagents_config import SubagentsAppConfig


def test_shipped_research_profile_supports_requested_capacity():
    config = yaml.safe_load((Path(__file__).resolve().parents[2] / "config.example.yaml").read_text(encoding="utf-8"))
    runtime = SubagentRuntimeConfig.model_validate(config["subagent_runtime"])
    batches = SubagentBatchesConfig.model_validate(config["subagent_batches"])
    agents = SubagentsAppConfig.model_validate(config["subagents"])
    assert config["database"]["backend"] in {"sqlite", "postgres"}
    assert batches.enabled
    assert runtime.max_running == batches.default_max_running_items == batches.max_running_items_per_batch == 32
    assert runtime.max_queued == batches.default_max_live_items == batches.max_live_items_per_batch == 1000
    assert agents.max_total_per_run == batches.max_items_per_run == batches.max_items_per_batch == 1000
    assert agents.timeout_seconds == runtime.queue_timeout_seconds == 0
