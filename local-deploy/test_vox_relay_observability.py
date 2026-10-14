"""Completion helper shared with the native tool/CLI integration test."""

import time
import pytest


def completed(server):
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline:
        with server.gate.condition:
            if not server.gate.active and not server.gate.waiting:
                break
        time.sleep(0.005)
    else:
        pytest.fail("relay requests did not finish")
    server.trace_writer.pending.join()
