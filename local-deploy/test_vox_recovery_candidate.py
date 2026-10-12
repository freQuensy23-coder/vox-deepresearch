import concurrent.futures
import http.client
import importlib.util
import socket
import threading
import json
import os
import subprocess
import sys
import time
from email.utils import formatdate
from pathlib import Path

import pytest

from test_vox_relay import load_relay, request, upstream
from test_vox_relay_observability import completed


MODULE = Path(__file__).with_name("vox_recovery_candidate.py")


def candidate():
    assert MODULE.exists(), "staged generation-accounting candidate is not implemented"
    spec = importlib.util.spec_from_file_location("candidate", MODULE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def clock(monkeypatch):
    now = [1800000000.0]
    monkeypatch.setattr(time, "time", lambda: now[0])
    return now


def admit(gate, clock):
    clock[0] += 0.3
    return gate.acquire(0.01)


def test_default_entrypoint_is_legacy(monkeypatch):
    module = candidate()
    calls = []
    monkeypatch.delenv("VOX_RELAY_RECOVERY_CANDIDATE", raising=False)
    monkeypatch.setattr(module.legacy, "main", lambda: calls.append("legacy"))
    module.main()
    assert calls == ["legacy"]


def test_invalid_optin_is_rejected(monkeypatch):
    module = candidate()
    monkeypatch.setenv("VOX_RELAY_RECOVERY_CANDIDATE", "true")
    with pytest.raises(ValueError):
        module.main()


def test_five_sibling_failures_refresh_without_multiplication(tmp_path, clock):
    module = candidate()
    gate = module.GenerationGate(tmp_path, .25, 60, 900)
    owners = [admit(gate, clock) for _ in range(5)]
    for owner in owners:
        clock[0] += 1
        gate.complete(owner, failed=True)
        gate.release(owner)
        state = gate.read()
        assert state["failures"] == 1
        assert state["generation"] == 1
        assert state["cooldown_until"] == clock[0] + 60


def test_later_cohorts_escalate_and_old_failures_only_refresh(tmp_path, clock):
    gate = candidate().GenerationGate(tmp_path, .25, 60, 900)
    old, sibling = [admit(gate, clock) for _ in range(2)]
    gate.complete(old, failed=True)
    gate.release(old)
    for expected in (120, 240, 480, 900, 900):
        clock[0] = gate.read()["cooldown_until"] + 1
        fresh = admit(gate, clock)
        gate.complete(fresh, failed=True)
        gate.release(fresh)
        assert gate.read()["cooldown_until"] == clock[0] + expected
    before = gate.read()
    clock[0] += 1
    gate.complete(sibling, failed=True)
    gate.release(sibling)
    after = gate.read()
    assert after["generation"] == before["generation"]
    assert after["failures"] == before["failures"]
    assert after["cooldown_until"] == clock[0] + 900


@pytest.mark.parametrize("raw,seconds", [("3600.75", 3600.75), ("date", 7200), ("invalid", None), ("NaN", None), ("Infinity", None), ("-1", None), ("1e309", None), ("x" * 2000, None)])
def test_retry_after_separate_uncapped_floor(tmp_path, clock, raw, seconds):
    module = candidate()
    gate = module.GenerationGate(tmp_path, .25, 60, 900)
    owner = admit(gate, clock)
    if raw == "date":
        raw = formatdate(clock[0] + 7200, usegmt=True)
        seconds = int(clock[0] + 7200) - clock[0]
    gate.complete(owner, failed=True, retry_after=raw)
    gate.release(owner)
    state = gate.read()
    assert state["soft_until"] == clock[0] + 60
    assert state["hard_until"] == (clock[0] + seconds if seconds is not None else 0)
    assert state["cooldown_until"] == max(state["soft_until"], state["hard_until"])
    assert module.GenerationGate(tmp_path, .25, 60, 900).read() == state
    metadata = module.retry_after_metadata(raw, clock[0])
    assert len(metadata["retry_after_raw"]) <= 256
    assert metadata["retry_after_valid"] is (seconds is not None)


def test_stale_success_cannot_reset_new_failure_or_deadline(tmp_path, clock):
    gate = candidate().GenerationGate(tmp_path, .25, 60, 900)
    old, failed = [admit(gate, clock) for _ in range(2)]
    gate.complete(failed, failed=True, retry_after="3600")
    gate.release(failed)
    before = gate.read()
    gate.complete(old, validated=True)
    gate.release(old)
    assert gate.read() == before


def test_only_validated_current_generation_can_reset(tmp_path, clock):
    gate = candidate().GenerationGate(tmp_path, .25, 60, 900)
    first = admit(gate, clock)
    gate.complete(first, failed=True)
    gate.release(first)
    clock[0] = gate.read()["cooldown_until"] + 1
    incomplete = admit(gate, clock)
    gate.complete(incomplete)
    gate.release(incomplete)
    assert gate.read()["failures"] == 1
    valid = admit(gate, clock)
    gate.complete(valid, validated=True)
    gate.release(valid)
    assert gate.read()["failures"] == 0


def test_cancellation_is_neutral_not_success(tmp_path, clock):
    gate = candidate().GenerationGate(tmp_path, .25, 60, 900)
    owner = admit(gate, clock)
    before = gate.read()
    gate.complete(owner, cancelled=True, validated=True)
    gate.release(owner)
    assert gate.read() == before
    owner = admit(gate, clock)
    gate.complete(owner, failed=True)
    gate.release(owner)
    assert gate.read()["failures"] == 1


def test_429_floor_and_gap_do_not_enter_non429_ladder(tmp_path, clock):
    gate = candidate().GenerationGate(tmp_path, .25, 60, 900)
    owner = admit(gate, clock)
    gate.complete(owner, failed=True, rate_limited=True, retry_after="4000.25")
    gate.release(owner)
    state = gate.read()
    assert state["failures"] == 0
    assert state["gap"] == .5
    assert state["hard_until"] == clock[0] + 4000.25


def test_migration_preserves_combined_deadline_as_hard_floor(tmp_path, clock):
    baseline = load_relay().Gate(tmp_path, .25, 60, 900)
    baseline.finish(True, "5000")
    before = baseline.read()
    inode = (tmp_path / "gate.lock").stat().st_ino
    gate = candidate().GenerationGate(tmp_path, .25, 60, 900)
    assert gate.read()["hard_until"] == before["cooldown_until"]
    assert gate.read()["failures"] == before["failures"]
    assert (tmp_path / "gate.lock").stat().st_ino == inode
    with pytest.raises(ValueError):
        baseline.read()
    with pytest.raises(ValueError):
        load_relay().Gate(tmp_path, .25, 60, 900)
    with pytest.raises(candidate().legacy.Busy):
        gate.acquire(.001)


def test_migration_requires_drained_slots(tmp_path, clock):
    baseline = load_relay().Gate(tmp_path, .25, 60, 900)
    owner = baseline.acquire(.01)
    before = (tmp_path / "state.json").read_bytes()
    try:
        with pytest.raises((ValueError, BlockingIOError)):
            candidate().GenerationGate(tmp_path, .25, 60, 900)
        assert (tmp_path / "state.json").read_bytes() == before
    finally:
        baseline.release(owner)


@pytest.mark.parametrize("change", [{"hard_until": -1}, {"generation": True}, {"soft_until": float("inf")}, {"version": 99}, {"cooldown_until": 0}, {"slot_count": 1}])
def test_corrupt_or_mismatched_state_fails_closed(tmp_path, clock, change):
    gate = candidate().GenerationGate(tmp_path, .25, 60, 900)
    owner = admit(gate, clock)
    gate.complete(owner, failed=True)
    gate.release(owner)
    state = gate.read() | change
    (tmp_path / "state.json").write_text(json.dumps(state))
    with pytest.raises(ValueError):
        gate.acquire(.001)


@pytest.mark.parametrize("body,finished,error", [
    (b'{"point":0}\n', False, False),
    (b'{"messages":[],"finished":false}\n', False, False),
    (b'{"messages":[],"finished":true}\n', True, False),
    (b'{"messages":[],"finished":"true"}\n', False, False),
    (b'{"messages":[],"finished":true,"error":"private"}\n', False, True),
    (b'{"messages":[],"finished":true,"count":NaN}\n', False, False),
    (b'{"messages":[],"finished":true,"partial":true}\n', False, False),
    (b'{"messages":[],"finished":true,"truncated":true}\n', False, False),
    (b'{"messages":[],"finished":true,"status":"incomplete"}\n', False, False),
    (b'{"messages":[],"finished":true}\nnot-json\n', False, False),
    (b'{"messages":[],"finished":true}\n{"point":1}\n', False, False),
])
def test_search_semantics_are_not_transport_eof(body, finished, error):
    probe = candidate().CompletionProbe("search_text")
    for offset in range(0, len(body), 7):
        probe.feed(body[offset:offset+7])
    metadata = probe.finish()
    assert metadata["semantic_complete"] is finished
    assert metadata["semantic_error"] is error
    assert all(type(value) is bool for value in metadata.values())
    assert "private" not in json.dumps(metadata)


def test_oversized_terminal_is_unvalidated_not_success():
    probe = candidate().CompletionProbe("search_text")
    probe.feed(b'{"messages":[],"finished":true,"padding":"' + b'x' * (2**20) + b'"}\n')
    assert probe.finish()["semantic_complete"] is False


def test_nonsearch_is_not_claimed_schema_validated():
    probe = candidate().CompletionProbe("group")
    probe.feed(b'{"ok":true}\n')
    assert probe.finish()["semantic_complete"] is False


def test_process_slots_stay_shared_and_crash_preserves_fault(tmp_path):
    module = candidate()
    gate = module.GenerationGate(tmp_path, .01, .05, .1, max_inflight=1)
    code = (
        "import importlib.util,time,sys;"
        f"s=importlib.util.spec_from_file_location('candidate',{str(MODULE)!r});"
        "m=importlib.util.module_from_spec(s);s.loader.exec_module(m);"
        f"g=m.GenerationGate({str(tmp_path)!r},.01,.05,.1,max_inflight=1);"
        "fd=g.acquire(.1);g.complete(fd,failed=True,retry_after='30');"
        "print('held',flush=True);sys.stdin.readline()"
    )
    process = subprocess.Popen([sys.executable, "-c", code], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        assert process.stdout.readline().strip() == "held"
        state = gate.read()
        with pytest.raises(module.legacy.Busy):
            gate.acquire(.03)
        process.kill()
        process.communicate(timeout=3)
        assert gate.read() == state
        with pytest.raises(module.legacy.Busy):
            gate.acquire(.03)
    finally:
        if process.poll() is None:
            process.kill()
        process.communicate(timeout=3)


@pytest.fixture
def candidate_server(tmp_path, upstream):
    module = candidate()
    assert hasattr(module, "CandidateRelayServer"), "candidate relay integration is missing"
    key = tmp_path / "key"
    key.write_text("synthetic-only-key")
    servers = []

    class Transport(module.legacy.HTTPSUpstream):
        def __init__(self, timeout):
            self.connection = http.client.HTTPConnection(*upstream.address, timeout=timeout)

    def start(**options):
        settings = dict(spacing=.01, cooldown=.05, backoff_cap=.2, queue_wait=.1,
                        request_deadline=2, socket_idle=2, client_timeout=.3, max_inflight=5)
        settings.update(options)
        server = module.CandidateRelayServer(("127.0.0.1", 0), key_file=key, state_dir=tmp_path / "candidate-state",
                                             transport_factory=Transport, **settings)
        thread = threading.Thread(target=server.serve_forever)
        thread.start()
        servers.append((server, thread))
        return server

    yield start
    upstream.release.set()
    for server, thread in servers:
        server.shutdown()
        server.server_close()
        thread.join(3)


def metadata(server):
    return [json.loads(path.read_text()) for path in sorted((server.state_dir / "traces").glob("*.json"))]


def test_candidate_metadata_has_timing_policy_and_no_payload(candidate_server, upstream, capsys):
    upstream.status, upstream.retry_after = 503, "3600.5"
    server = candidate_server()
    assert request(server, "/search/text?query=private-topic")[0] == 503
    completed(server)
    rows = metadata(server)
    assert len(rows) == 4
    end = next(row for row in rows if row["event"] == "end")
    header = next(row for row in rows if row["event"] == "upstream_headers")
    assert header["recovery"]["retry_after_raw"] == "3600.5"
    assert header["recovery"]["retry_after_until"] == header["recovery"]["retry_after_observed_at"] + 3600.5
    recovery = end["recovery"]
    assert recovery["current_policy"] == "staged-generation-only-v1"
    assert recovery["failure_generation"] == 1
    assert recovery["admission_generation"] == 0
    assert recovery["recovery_owner"] is None
    assert recovery["deadline_reason"] == "retry_after"
    assert recovery["termination_cause"] == "upstream_http_error"
    assert recovery["transport_eof"] is True
    assert recovery["upstream_closed"] is True
    assert recovery["state_before"]["failures"] == 0
    assert recovery["state_after"]["failures"] == 1
    text = json.dumps(rows) + capsys.readouterr().out
    for value in ("private-topic", "synthetic-only-key", "Authorization", "synthetic", "target", "request_payload"):
        assert value not in text
    assert not list((server.state_dir / "traces").glob("*.body"))


def test_downstream_disconnect_is_neutral_at_http_boundary(candidate_server, upstream):
    server = candidate_server()
    connection = http.client.HTTPConnection(*server.server_address, timeout=2)
    connection.request("GET", "/search/text?query=silent")
    response = connection.getresponse()
    assert response.readline() == b'{"point":0}\n'
    sock = connection.sock or response.fp.raw._sock
    sock.shutdown(socket.SHUT_RDWR)
    response.close()
    connection.close()
    completed(server)
    end = next(row for row in metadata(server) if row["event"] == "end")
    assert end["recovery"]["termination_cause"] == "downstream_disconnect"
    assert end["recovery"]["downstream_cancelled"] is True
    assert end["recovery"]["recovery_validated"] is False
    assert server.gate.read()["failures"] == 0
    assert server.gate.read()["success_streak"] == 0
    assert upstream.disconnected.wait(1)


def test_unknown_upstream_error_remains_failure(candidate_server):
    server = candidate_server()

    def broken(timeout):
        raise OSError("private-error-text")

    server.transport_factory = broken
    assert request(server)[0] == 502
    completed(server)
    end = next(row for row in metadata(server) if row["event"] == "end")
    assert end["recovery"]["termination_cause"] == "upstream_or_unknown_transport"
    assert end["recovery"]["exception_class"] == "OSError"
    assert server.gate.read()["failures"] == 1
    assert "private-error-text" not in json.dumps(end)


def test_local_deadline_is_not_classified_as_cancellation(candidate_server):
    server = candidate_server(request_deadline=.08)
    assert request(server, "/search/text?query=silent")[0] == 200
    completed(server)
    end = next(row for row in metadata(server) if row["event"] == "end")
    assert end["recovery"]["termination_cause"] == "relay_deadline"
    assert end["recovery"]["downstream_cancelled"] is False
    assert server.gate.read()["failures"] == 1


def test_stream_recovery_commits_only_after_close_and_before_release(candidate_server):
    server = candidate_server(max_inflight=1)
    closed = threading.Event()
    permit_close = threading.Event()

    class Response:
        status, length = 200, 0

        def __init__(self):
            self.sent = False

        def getheader(self, name, default=None):
            return default

        def read1(self, size):
            if self.sent:
                return b""
            self.sent = True
            return b'{"messages":[],"finished":true}\n'

        def close(self):
            closed.set()
            permit_close.wait(3)

    class Transport:
        def __init__(self, timeout):
            pass

        def request(self, *args):
            return Response()

        def abort(self):
            pass

        def close(self):
            pass

    server.transport_factory = Transport
    with concurrent.futures.ThreadPoolExecutor() as pool:
        result = pool.submit(request, server)
        try:
            assert closed.wait(2)
            assert server.gate.read()["success_streak"] == 0
            with pytest.raises(candidate().legacy.Busy):
                server.gate.acquire(.02)
        finally:
            permit_close.set()
        assert result.result(timeout=3)[0] == 200
    completed(server)
    assert server.gate.read()["success_streak"] == 1
    end = next(row for row in metadata(server) if row["event"] == "end")
    assert end["recovery"]["recovery_validated"] is True


def test_optin_entrypoint_constructs_candidate_only(monkeypatch):
    module = candidate()
    assert hasattr(module, "CandidateRelayServer")
    calls = []

    class Server:
        def __init__(self, *args, **kwargs):
            calls.append("candidate")

        def serve_forever(self):
            calls.append("serve")

        def server_close(self):
            calls.append("close")

    monkeypatch.setenv("VOX_RELAY_RECOVERY_CANDIDATE", "1")
    monkeypatch.setattr(module, "CandidateRelayServer", Server)
    module.main()
    assert calls == ["candidate", "serve", "close"]


def test_late_sibling_fault_fences_already_admitted_new_generation_success(tmp_path, clock):
    gate = candidate().GenerationGate(tmp_path, .25, 60, 900)
    first, sibling = [admit(gate, clock) for _ in range(2)]
    gate.complete(first, failed=True)
    gate.release(first)
    clock[0] = gate.read()["cooldown_until"] + 1
    fresh = admit(gate, clock)
    gate.complete(sibling, failed=True)
    gate.release(sibling)
    before = gate.read()
    gate.complete(fresh, validated=True)
    gate.release(fresh)
    assert gate.read() == before


def test_retry_after_on_success_headers_survives_later_cancellation(tmp_path, clock):
    module = candidate()
    gate = module.GenerationGate(tmp_path, .25, 60, 900)
    owner = admit(gate, clock)
    assert hasattr(gate, "observe_retry_after"), "hard deadline needs a header-time write independent of outcome"
    timing = module.retry_after_metadata("7200.5", clock[0])
    gate.observe_retry_after(owner, timing)
    gate.complete(owner, cancelled=True)
    gate.release(owner)
    assert gate.read()["hard_until"] == timing["retry_after_until"]
    assert gate.read()["failures"] == 0


def test_five_processes_coalesce_same_admission_cohort(tmp_path):
    module = candidate()
    gate = module.GenerationGate(tmp_path, .01, 60, 900)
    code = (
        "import importlib.util,sys,json;"
        f"s=importlib.util.spec_from_file_location('candidate',{str(MODULE)!r});"
        "m=importlib.util.module_from_spec(s);s.loader.exec_module(m);"
        f"g=m.GenerationGate({str(tmp_path)!r},.01,60,900);"
        "fd=g.acquire(1);print('held',flush=True);sys.stdin.readline();"
        "g.complete(fd,failed=True);g.release(fd);print(json.dumps(g.read()),flush=True)"
    )
    processes = []
    try:
        for _ in range(5):
            process = subprocess.Popen([sys.executable, "-c", code], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            processes.append(process)
            assert process.stdout.readline().strip() == "held"
        for process in processes:
            process.stdin.write("finish\n")
            process.stdin.flush()
        for process in processes:
            out, err = process.communicate(timeout=3)
            assert process.returncode == 0, err
            assert json.loads(out)["failures"] == 1
        assert gate.read()["generation"] == 1
        assert gate.read()["requests"] == 5
    finally:
        for process in processes:
            if process.poll() is None:
                process.kill()
            process.communicate(timeout=3)


def test_incremental_scope_keeps_multiple_slots_after_deadline(tmp_path, clock):
    gate = candidate().GenerationGate(tmp_path, .25, 60, 900)
    first, old = [admit(gate, clock) for _ in range(2)]
    gate.complete(first, failed=True)
    gate.release(first)
    clock[0] = gate.read()["cooldown_until"] + 1
    new = [admit(gate, clock) for _ in range(2)]
    for owner in [old] + new:
        gate.complete(owner, cancelled=True)
        gate.release(owner)
    assert gate.read()["failures"] == 1
