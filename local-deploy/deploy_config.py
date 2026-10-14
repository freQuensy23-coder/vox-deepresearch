"""Read a service section from the repository's config.yaml."""

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def section(name):
    config = yaml.safe_load((ROOT / "config.yaml").read_text(encoding="utf-8"))
    return config[name]


def path(value):
    """Resolve a config path relative to the repository root."""
    return ROOT / Path(value).expanduser()
