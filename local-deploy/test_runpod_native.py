"""The native deployment preserves enabled services from the operator config."""

import ast
from pathlib import Path

import yaml


def test_native_config_preserves_scheduler_and_channels(tmp_path):
    module = ast.parse(
        Path(__file__).with_name("runpod_native.py").read_text(encoding="utf-8")
    )
    script = next(
        node.value.value
        for node in module.body
        if isinstance(node, ast.Assign)
        and any(
            isinstance(target, ast.Name) and target.id == "config_script"
            for target in node.targets
        )
    )
    source = tmp_path / "source.yaml"
    destination = tmp_path / "native.yaml"
    original = {
        "scheduler": {"enabled": True, "tool_enabled": True},
        "channels": {"enabled": True, "telegram": {"enabled": True}},
        "models": [{"name": "operator-model"}],
    }
    source.write_text(yaml.safe_dump(original), encoding="utf-8")
    script = script.replace(
        "/workspace/vox-openclaw-dp/home/private/vox-deepresearch/config.yaml",
        str(source),
    ).replace("/app/config.yaml", str(destination))
    exec(script, {})
    actual = yaml.safe_load(destination.read_text(encoding="utf-8"))
    for name in ("scheduler", "channels", "models"):
        assert actual[name] == original[name]
    assert actual["sandbox"]["allow_host_bash"] is True
    assert actual["subagent_runtime"]["max_running"] == 32
    assert actual["subagents"]["timeout_seconds"] == 0
