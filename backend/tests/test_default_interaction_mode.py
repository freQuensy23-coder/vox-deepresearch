from types import SimpleNamespace

import pytest

from deerflow.agents.interaction_policy import RunInteractionMode, apply_default_interaction_mode, resolve_run_interaction_policy
from deerflow.config.app_config import AppConfig

AUTONOMOUS = SimpleNamespace(default_interaction_mode="autonomous")


def test_shipped_default_stays_interactive():
    assert AppConfig.model_fields["default_interaction_mode"].default == "interactive"
    config = {"configurable": {"thread_id": "t1"}}
    apply_default_interaction_mode(config, SimpleNamespace(default_interaction_mode="interactive"))
    assert resolve_run_interaction_policy(config).allows_clarification


def test_autonomous_default_removes_clarification_from_an_unset_run():
    config = {"configurable": {"thread_id": "t1"}, "context": {"user_id": "u1"}}
    apply_default_interaction_mode(config, AUTONOMOUS)
    assert config["context"]["interaction_mode"] == "autonomous"
    policy = resolve_run_interaction_policy(config)
    assert policy.mode is RunInteractionMode.AUTONOMOUS and "ask_clarification" in policy.disabled_tool_names


def test_context_without_a_context_section_gets_one():
    config = {}
    apply_default_interaction_mode(config, AUTONOMOUS)
    assert config == {"context": {"interaction_mode": "autonomous"}}


@pytest.mark.parametrize(
    "config,mode",
    [
        ({"context": {"interaction_mode": "interactive"}}, RunInteractionMode.INTERACTIVE),
        ({"configurable": {"interaction_mode": "interactive"}}, RunInteractionMode.INTERACTIVE),
        ({"context": {"non_interactive": True}}, RunInteractionMode.SCHEDULED),
        ({"context": {"channel_name": "github"}}, RunInteractionMode.WEBHOOK),
    ],
)
def test_a_mode_chosen_by_a_trusted_caller_is_kept(config, mode):
    apply_default_interaction_mode(config, AUTONOMOUS)
    assert resolve_run_interaction_policy(config).mode is mode
    assert config["context"].get("interaction_mode") in {None, "interactive"}


def test_configs_without_the_setting_change_nothing():
    config = {"context": {}}
    apply_default_interaction_mode(config, object())
    assert config == {"context": {}}
