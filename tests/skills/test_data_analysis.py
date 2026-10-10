"""Missing dependencies must not trigger installation while importing a skill."""

import builtins
import runpy
import subprocess
import types
from pathlib import Path

import pytest


@pytest.mark.parametrize("missing", ["duckdb", "openpyxl"])
def test_missing_dependency_reports_setup_without_installing(
    monkeypatch, caplog, missing
):
    original_import = builtins.__import__

    def import_dependency(name, *args, **kwargs):
        if name == missing:
            raise ImportError(f"No module named {missing!r}")
        if name in {"duckdb", "openpyxl"}:
            return types.ModuleType(name)
        return original_import(name, *args, **kwargs)

    def unexpected_process(*args, **kwargs):
        pytest.fail("Importing the analysis script must not run an installer")

    monkeypatch.setattr(builtins, "__import__", import_dependency)
    monkeypatch.setattr(subprocess, "Popen", unexpected_process)
    script = (
        Path(__file__).resolve().parents[2]
        / "skills/public/data-analysis/scripts/analyze.py"
    )
    with pytest.raises(SystemExit) as error:
        runpy.run_path(str(script))
    assert error.value.code == 1
    assert missing in caplog.text
    assert "pip install duckdb openpyxl" in caplog.text
