"""Service settings from the repository's config.yaml and secrets from its .env."""

import os
from pathlib import Path

import yaml
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]


def section(name):
    config = yaml.safe_load((ROOT / "config.yaml").read_text(encoding="utf-8"))
    return config[name]


def path(value):
    """Resolve a config path relative to the repository root."""
    return ROOT / Path(value).expanduser()


def secret(name):
    load_dotenv(ROOT / ".env", override=False)
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"{name} is not set in {ROOT / '.env'}")
    return value
