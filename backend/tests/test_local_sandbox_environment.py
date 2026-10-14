import os

import pytest

from deerflow.sandbox.local.local_sandbox import LocalSandbox


@pytest.mark.skipif(os.name != "posix", reason="POSIX shell")
def test_configured_environment_reaches_commands_under_injected_secrets():
    sandbox = LocalSandbox("local", environment={"VOX_RELAY_URL": "http://127.0.0.1:8083", "SHARED": "config"})

    output = sandbox.execute_command('printf "%s %s" "$VOX_RELAY_URL" "$SHARED"', env={"SHARED": "request"})

    assert output == "http://127.0.0.1:8083 request"
