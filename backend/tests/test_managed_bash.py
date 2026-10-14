"""Managed command sessions: real local processes, no external services."""

import json
import shlex
import sys
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from deerflow.sandbox.local.local_sandbox import LocalSandbox


def command(source):
    return f"{shlex.quote(sys.executable)} -u -c {shlex.quote(source)}"


@pytest.fixture
def sandbox():
    box = LocalSandbox("managed-test")
    yield box
    box.release_command_scope("owner")
    box.release_command_scope("other")


def test_fast_command_returns_completed_output(sandbox):
    result = sandbox.start_command(command("print('ready')"), owner_id="owner", yield_time=1)
    assert result["status"] == "completed"
    assert result["exit_code"] == 0
    assert result["stdout"] == "ready\n"


def test_long_command_yields_and_independent_work_can_run(sandbox, tmp_path):
    result = sandbox.start_command(command("import time; print('first'); time.sleep(.4); print('last')"), owner_id="owner", yield_time=0.05)
    assert result["status"] == "running"
    sid = result["session_id"]
    # A second command must not queue behind the first one's shell session.
    other = sandbox.start_command(command("print('independent')"), owner_id="owner", yield_time=0.2)
    assert other["status"] == "completed"
    assert "independent" in other["stdout"]
    final = sandbox.poll_command(sid, owner_id="owner", wait_seconds=1)
    assert final["status"] == "completed"
    assert final["exit_code"] == 0
    assert result["stdout"] + final["stdout"] == "first\nlast\n"


def test_session_owned_by_agent_and_cancel_kills_process(sandbox):
    result = sandbox.start_command(command("import time; time.sleep(30)"), owner_id="owner", yield_time=0.02)
    with pytest.raises(PermissionError):
        sandbox.poll_command(result["session_id"], owner_id="other", cancel=True)
    final = sandbox.poll_command(result["session_id"], owner_id="owner", cancel=True)
    assert final["status"] == "cancelled"


def test_scope_release_cancels_running_commands(sandbox):
    result = sandbox.start_command(command("import time; time.sleep(30)"), owner_id="owner", yield_time=0.02)
    sandbox.release_command_scope("owner")
    with pytest.raises(KeyError):
        sandbox.poll_command(result["session_id"], owner_id="owner")


def test_command_deadline_is_separate_from_yield(sandbox):
    result = sandbox.start_command(command("import time; time.sleep(30)"), owner_id="owner", timeout=0.15, yield_time=0.02)
    assert result["status"] == "running"
    final = sandbox.poll_command(result["session_id"], owner_id="owner", wait_seconds=1)
    assert final["status"] == "timed_out"
    assert final["exit_code"] == 124


def test_nonzero_exit_is_not_reported_as_success(sandbox):
    result = sandbox.start_command(command("import sys; print('failed',file=sys.stderr); sys.exit(7)"), owner_id="owner", yield_time=1)
    assert result["status"] == "completed"
    assert result["exit_code"] == 7
    assert "failed" in result["stderr"]


def test_secret_redaction_applies_when_polling_after_start(sandbox):
    result = sandbox.start_command(command("import os,time; time.sleep(.1); print(os.environ['TEST_SECRET'])"), owner_id="owner", env={"TEST_SECRET": "private-test-value"}, yield_time=0.02)
    final = sandbox.poll_command(result["session_id"], owner_id="owner", wait_seconds=1)
    assert "private-test-value" not in json.dumps(final)
    assert final["exit_code"] == 0


def test_split_secret_and_unicode_survive_incremental_reads(sandbox):
    source = "import os,time; os.write(1,b'private-'); time.sleep(.2); os.write(1,b'test-value'); print(' Привет')"
    result = sandbox.start_command(command(source), owner_id="owner", env={"TEST_SECRET": "private-test-value"}, yield_time=0.05)
    final = sandbox.poll_command(result["session_id"], owner_id="owner", wait_seconds=1)
    output = result["stdout"] + final["stdout"]
    assert "private-test-value" not in output
    assert "[REDACTED]" in output
    assert "Привет" in output


def test_output_is_bounded_without_blocking_producer(sandbox):
    result = sandbox.start_command(command("print('a'*500000)"), owner_id="owner", yield_time=1)
    assert result["exit_code"] == 0
    assert result["output_truncated"]
    assert len(result["stdout"]) <= 20000


def test_tool_returns_managed_identity_and_can_poll(sandbox, monkeypatch):
    from deerflow.sandbox.tools import _execute_bash_command

    runtime = SimpleNamespace(context={"sandbox_lease_owner_id": "owner"})
    result = json.loads(_execute_bash_command(sandbox, command("print('tool-result')"), runtime=runtime, env=None, background=True))
    assert result["status"] == "completed"
    assert result["stdout"] == "tool-result\n"
    assert result["exit_code"] == 0
    again = json.loads(_execute_bash_command(sandbox, "", runtime=runtime, env=None, session_id=result["session_id"]))
    assert again["exit_code"] == 0
    assert again["stdout"] == ""


def test_bash_schema_exposes_native_mode_and_session_controls():
    from deerflow.sandbox.tools import bash_tool

    # Runtime is framework-injected and not a model-facing JSON value.
    assert {"command", "background", "session_id", "cancel", "timeout_seconds"} <= bash_tool.args_schema.model_fields.keys()


def aio_result(status, stdout="", offset=0, code=None):
    info = SimpleNamespace(command_id="cmd", status=status, exit_code=code)
    return SimpleNamespace(data=SimpleNamespace(command=info, command_id="cmd", status=status, exit_code=code, stdout=stdout, stderr="", offset=offset, stderr_offset=0))


def test_aio_native_async_poll_and_owned_cleanup():
    from deerflow.community.aio_sandbox.aio_sandbox import AioSandbox

    with patch("deerflow.community.aio_sandbox.aio_sandbox.AioSandboxClient") as client_class:
        box = AioSandbox("test", "http://127.0.0.1:9")
    client = client_class.return_value
    client.bash.exec.return_value = aio_result("running", "first\n", 6)
    client.bash.output.side_effect = [aio_result("running", "", 6), aio_result("completed", "last\n", 11, 0)]
    result = box.start_command("python3 collect.py", owner_id="owner", env={"TEST_SECRET": "secret"}, timeout=90000, yield_time=0)
    assert result["status"] == "running"
    args = client.bash.exec.call_args.kwargs
    assert args["async_mode"] is True
    assert args["hard_timeout"] == 90000
    assert args["env"]["TEST_SECRET"] == "secret"
    assert "nohup" not in args["command"]
    final = box.poll_command(result["session_id"], owner_id="owner")
    assert final["status"] == "completed"
    assert result["stdout"] + final["stdout"] == "first\nlast\n"
    assert client.bash.output.call_args.kwargs["offset"] == 6
    client.bash.close_session.assert_called_once()
    box.release_command_scope("owner")


def test_aio_owner_release_kills_native_background_session():
    from deerflow.community.aio_sandbox.aio_sandbox import AioSandbox

    with patch("deerflow.community.aio_sandbox.aio_sandbox.AioSandboxClient") as client_class:
        box = AioSandbox("test", "http://127.0.0.1:9")
    client = client_class.return_value
    client.bash.exec.return_value = aio_result("running")
    client.bash.output.return_value = aio_result("running")
    result = box.start_command("python3 collect.py", owner_id="owner", yield_time=0)
    box.release_command_scope("owner")
    client.bash.kill.assert_called_once()
    client.bash.close_session.assert_called_once()
    with pytest.raises(KeyError):
        box.poll_command(result["session_id"], owner_id="owner")


def test_aio_submit_failure_is_not_replayed():
    from deerflow.community.aio_sandbox.aio_sandbox import AioSandbox

    with patch("deerflow.community.aio_sandbox.aio_sandbox.AioSandboxClient") as client_class:
        box = AioSandbox("test", "http://127.0.0.1:9")
    client = client_class.return_value
    client.bash.exec.side_effect = RuntimeError("unknown submission outcome")
    with pytest.raises(RuntimeError):
        box.start_command("python3 collect.py", owner_id="owner", yield_time=0)
    client.bash.exec.assert_called_once()
    client.bash.close_session.assert_called_once()


def test_closed_registry_rejects_new_processes(sandbox):
    sandbox._command_sessions.close()
    with pytest.raises(RuntimeError, match="closed"):
        sandbox.start_command(command("print(1)"), owner_id="owner")


@pytest.mark.parametrize("background", [False, True])
def test_native_bash_runs_python_file_suffixes_and_globs(sandbox, tmp_path, monkeypatch, background):
    from pathlib import Path

    from deerflow.sandbox.tools import bash_tool

    thread_data = {f"{name}_path": str(tmp_path / name) for name in ("workspace", "uploads", "outputs")}
    for path in thread_data.values():
        Path(path).mkdir()
    runtime = SimpleNamespace(state={"sandbox": {"sandbox_id": "local"}, "thread_data": thread_data}, context={"thread_id": "native", "sandbox_lease_owner_id": "owner"}, config={})
    monkeypatch.setattr("deerflow.sandbox.tools.ensure_sandbox_initialized", lambda runtime: sandbox)
    monkeypatch.setattr("deerflow.sandbox.tools.is_host_bash_allowed", lambda: True)
    source = (
        "from pathlib import Path; p=Path.cwd(); (p/'search-a').mkdir(); "
        "Path(f'{p}/facts.md').write_text('fact'); Path(f'{p}/registry.json').write_text('{}'); "
        "(p/'search-a'/'manifest.json').write_text('{}'); "
        "print([str(x.relative_to(p)) for x in p.glob('search-*/manifest.json')])"
    )
    result = bash_tool.func(runtime=runtime, command=command(source), background=background, timeout_seconds=90000)
    if background:
        result = json.loads(result)
        assert result["exit_code"] == 0
        result = result["stdout"]
    assert "search-a/manifest.json" in result
    assert (Path(thread_data["workspace_path"]) / "facts.md").read_text() == "fact"
    assert (Path(thread_data["workspace_path"]) / "registry.json").exists()


def test_starting_more_than_64_commands_retains_prior_handles(sandbox):
    sessions = [sandbox.start_command(command("print('ok')"), owner_id="owner", yield_time=1) for _ in range(70)]
    for session in sessions:
        assert session["exit_code"] == 0
        assert sandbox.poll_command(session["session_id"], owner_id="owner")["exit_code"] == 0
