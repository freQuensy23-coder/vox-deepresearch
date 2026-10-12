"""Controller-only tests; all processes/fixtures are local and make no HTTP calls."""

import json
import os
from pathlib import Path
import subprocess
import sys
import time

import pytest

from adaptive_gate import AdaptiveGate, Busy, Cancelled


class Clock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now

    def advance(self, amount):
        self.now += amount


@pytest.fixture
def setup(tmp_path):
    clock = Clock()
    gate = AdaptiveGate(tmp_path, clock=clock, monotonic=clock, sleep=clock.advance)
    return gate, clock


def pending(gate, lane="search"):
    fd = gate._try_lock("wait-1000.lock", create=True)
    with gate._transaction() as state:
        state["tickets"] += 1
        state["pending"]["1000"] = {
            "token": "a" * 32,
            "lane": lane,
            "ticket": state["tickets"],
            "class": "search_text" if lane == "search" else "group",
        }
    return fd


def complete(gate, clock, leases, duration=1):
    clock.advance(duration)
    for lease in leases:
        gate.headers(lease, 200)
        gate.progress(lease)
        gate.finish(lease)


def established(gate, request_class="search_text", capacity=5):
    """Seed a previously measured class for ownership/failure contract tests.

    Growth and cold-burst behavior have separate tests that use actual feedback.
    """
    with gate._transaction() as state:
        control = state["baselines"][request_class]["control"]
        control.update(limit=capacity, safe=capacity, probe=False)


def test_established_five_and_separate_metadata_lane(setup):
    gate, _ = setup
    established(gate)
    leases = [gate.acquire(0) for _ in range(5)]
    with pytest.raises(Busy):
        gate.acquire(0)
    metadata = gate.acquire(0, "group")
    assert gate.snapshot()["lanes"]["search"]["active"] == 5
    gate.finish(metadata, "cancelled")
    for lease in leases:
        gate.finish(lease, "cancelled")


def test_healthy_backlog_grows_to_32_without_spacing(setup):
    gate, clock = setup
    capacities = []
    for _ in range(70):
        state = gate.snapshot()
        cap = min(
            state["lanes"]["search"]["capacity"],
            state["baselines"]["search_text"]["control"]["limit"],
        )
        capacities.append(cap)
        leases = [gate.acquire(0) for _ in range(cap)]
        fd = pending(gate)
        try:
            complete(gate, clock, leases, 0.2)
        finally:
            os.close(fd)
    assert capacities[0] == 1
    assert max(capacities) == 32
    assert gate.snapshot()["baselines"]["search_text"]["duration"] == pytest.approx(0.2)


def test_no_backlog_does_not_expand(setup):
    gate, clock = setup
    for _ in range(20):
        complete(gate, clock, [gate.acquire(0)], 0.1)
    assert gate.snapshot()["lanes"]["search"]["capacity"] == 5


def test_current_stall_stops_new_work_before_completion(setup):
    gate, clock = setup
    complete(gate, clock, [gate.acquire(0)], 1)
    established(gate)
    lease = gate.acquire(0)
    gate.headers(lease, 200)  # Early HTTP headers are not query progress.
    clock.advance(3)
    with pytest.raises(Busy):
        gate.acquire(0)
    state = gate.snapshot()
    assert state["lanes"]["search"]["capacity"] == 2
    assert state["lanes"]["search"]["incidents"] == 1
    assert state["lanes"]["search"]["active"] == 1
    for _ in range(10):
        gate.snapshot()
    assert gate.snapshot()["lanes"]["search"]["incidents"] == 1
    metadata = gate.acquire(0, "group")
    gate.finish(metadata, "neutral")
    gate.finish(lease)  # Late success must not erase the incident.
    assert gate.snapshot()["lanes"]["search"]["capacity"] == 2
    assert gate.snapshot()["baselines"]["search_text"]["duration"] == pytest.approx(1)


def test_meaningful_progress_keeps_long_healthy_request_admissible(setup):
    gate, clock = setup
    complete(gate, clock, [gate.acquire(0)], 1)
    lease = gate.acquire(0)
    for _ in range(20):
        clock.advance(1)
        gate.progress(lease)
        assert gate.snapshot()["lanes"]["search"]["incidents"] == 0
    # Final long completion may signal total-latency pressure; it was not killed
    # merely for duration while continuing to produce meaningful data.
    assert gate.snapshot()["lanes"]["search"]["active"] == 1
    gate.finish(lease, "cancelled")


def test_same_failure_cohort_reduces_once_then_new_cohort_can_reduce(setup):
    gate, clock = setup
    established(gate)
    leases = [gate.acquire(0) for _ in range(5)]
    for lease in leases:
        gate.headers(lease, 503)
        gate.finish(lease, "failure")
    state = gate.snapshot()["lanes"]["search"]
    assert (state["capacity"], state["incidents"]) == (2, 1)
    clock.advance(0.3)
    lease = gate.acquire(0)
    gate.finish(lease, "failure")
    assert gate.snapshot()["lanes"]["search"]["incidents"] == 2


def test_old_success_cannot_recover_failure_cohort(setup):
    gate, clock = setup
    established(gate)
    leases = [gate.acquire(0) for _ in range(5)]
    fd = pending(gate)
    try:
        gate.headers(leases[0], 503)
        gate.finish(leases[0], "failure")
        complete(gate, clock, leases[1:], 1)
        assert gate.snapshot()["lanes"]["search"]["capacity"] == 2
        assert gate.snapshot()["lanes"]["search"]["successes"] == 0
    finally:
        os.close(fd)


def test_retry_after_is_immediate_global_durable_and_survives_cancel(setup):
    gate, clock = setup
    lease = gate.acquire(0)
    gate.headers(lease, 429, "30")
    with pytest.raises(Busy):
        gate.acquire(0, "group")
    gate.finish(lease, "cancelled")
    other = AdaptiveGate(
        gate.directory, clock=clock, monotonic=clock, sleep=clock.advance
    )
    assert other.snapshot()["retry_until"] == 1030
    clock.advance(29)
    with pytest.raises(Busy):
        other.acquire(0)
    clock.advance(1)
    current = other.acquire(0)
    other.finish(current, "neutral")


@pytest.mark.parametrize("status", [200, 429, 503])
def test_retry_after_http_date(status, setup):
    from email.utils import formatdate

    gate, clock = setup
    lease = gate.acquire(0)
    gate.headers(lease, status, formatdate(clock() + 45, usegmt=True))
    assert gate.snapshot()["retry_until"] == clock() + 45
    gate.finish(lease, "cancelled")


@pytest.mark.parametrize("value", [None, "garbage", "nan", "inf", "-2"])
def test_429_fallback_is_bounded(value, setup):
    gate, clock = setup
    lease = gate.acquire(0)
    gate.headers(lease, 429, value)
    assert 2 <= gate.snapshot()["retry_until"] - clock() <= 30
    gate.finish(lease, "cancelled")


def test_cancellation_does_not_manufacture_failure(setup):
    gate, _ = setup
    with pytest.raises(Cancelled):
        gate.acquire(10, cancelled=lambda: True)
    assert gate.snapshot()["lanes"]["search"]["queued"] == 0
    lease = gate.acquire(0)
    gate.finish(lease, "cancelled")
    assert gate.snapshot()["lanes"]["search"]["incidents"] == 0


def test_faulted_transport_retains_permit_and_fails_closed(setup):
    gate, clock = setup
    lease = gate.acquire(0)
    gate.finish(lease, "failure", transport_closed=False)
    clock.advance(10000)
    assert gate.snapshot()["faulted"] is True
    assert gate.snapshot()["lanes"]["search"]["active"] == 1
    with pytest.raises(Busy):
        gate.acquire(0, "group")
    with pytest.raises(ValueError):
        gate.finish(lease)
    # Simulate OS release at process exit; real callers must never do this.
    os.close(lease.fd)
    assert gate.snapshot()["lanes"]["search"]["active"] == 0
    assert gate.snapshot()["faulted"] is False


def test_lease_is_owned_and_can_only_finish_once(setup):
    gate, _ = setup
    lease = gate.acquire(0)
    other = AdaptiveGate(gate.directory)
    with pytest.raises(ValueError):
        other.finish(lease)
    gate.finish(lease)
    with pytest.raises(ValueError):
        gate.finish(lease)


def test_class_baselines_are_separate_and_bounded(setup):
    gate, clock = setup
    fast = "search_text/single/short/small"
    slow = "search_text/all/long/large"
    complete(gate, clock, [gate.acquire(0, fast)], 0.1)
    assert gate.snapshot()["baselines"][fast]["duration"] == pytest.approx(0.1)
    assert slow not in gate.snapshot()["baselines"]
    complete(gate, clock, [gate.acquire(0, slow)], 5)
    assert gate.snapshot()["baselines"][slow]["duration"] == pytest.approx(5)
    with pytest.raises(ValueError):
        gate.acquire(0, "search_text/my-secret-query")


@pytest.mark.parametrize(
    "mutate",
    [
        lambda s: s.update(version=5),
        lambda s: s.update(retry_until=float("nan")),
        lambda s: s["lanes"]["search"].update(capacity=999),
        lambda s: s["baselines"]["search_text"].update(duration=-1),
        lambda s: s.update(unrecognized=True),
    ],
)
def test_corruption_fails_closed(tmp_path, mutate):
    gate = AdaptiveGate(tmp_path)
    path = tmp_path / "state.json"
    state = json.loads(path.read_text(encoding="utf-8"))
    mutate(state)
    path.write_text(json.dumps(state), encoding="utf-8")
    with pytest.raises(ValueError):
        gate.acquire(0)


def test_missing_state_and_symlink_are_not_reset(tmp_path):
    directory = tmp_path / "state"
    AdaptiveGate(directory)
    (directory / "state.json").unlink()
    with pytest.raises(ValueError):
        AdaptiveGate(directory)
    link = tmp_path / "link"
    link.symlink_to(directory, target_is_directory=True)
    with pytest.raises(ValueError):
        AdaptiveGate(link)


MODULE_DIR = str(Path(__file__).resolve().parent)


def test_os_lock_survives_age_but_reaps_crashed_process(tmp_path):
    code = """
import sys, time
sys.path.insert(0, sys.argv[1])
from adaptive_gate import AdaptiveGate
g = AdaptiveGate(sys.argv[2], search_cap=1, initial_search=1)
l = g.acquire(0)
print('held', flush=True)
time.sleep(30)
"""
    process = subprocess.Popen(
        [sys.executable, "-c", code, MODULE_DIR, str(tmp_path)],
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        assert process.stdout.readline().strip() == "held"
        gate = AdaptiveGate(tmp_path, search_cap=1, initial_search=1)
        with pytest.raises(Busy):
            gate.acquire(0.05)
        assert gate.snapshot()["lanes"]["search"]["active"] == 1
        process.kill()
        process.wait(timeout=5)
        lease = gate.acquire(0.1)
        gate.finish(lease, "neutral")
    finally:
        if process.poll() is None:
            process.kill()
        process.wait(timeout=5)
        process.stdout.close()


def test_cross_process_admission_never_exceeds_capacity(tmp_path):
    # Independent processes share previously learned OS admission permits.
    established(AdaptiveGate(tmp_path, search_cap=2, initial_search=2), capacity=2)
    code = """
import json, sys, time
sys.path.insert(0, sys.argv[1])
from adaptive_gate import AdaptiveGate
g = AdaptiveGate(sys.argv[2], search_cap=2, initial_search=2)
l = g.acquire(5)
start = time.monotonic()
time.sleep(.04)
end = time.monotonic()
g.finish(l, 'neutral')
print(json.dumps([start, end]), flush=True)
"""
    processes = [
        subprocess.Popen(
            [sys.executable, "-c", code, MODULE_DIR, str(tmp_path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for _ in range(8)
    ]
    intervals = []
    try:
        for process in processes:
            out, err = process.communicate(timeout=10)
            assert process.returncode == 0, err
            intervals.append(json.loads(out))
    finally:
        for process in processes:
            if process.poll() is None:
                process.kill()
                process.wait()
    active = maximum = 0
    for _, delta in sorted(
        [(start, 1) for start, _ in intervals] + [(end, -1) for _, end in intervals]
    ):
        active += delta
        maximum = max(maximum, active)
    assert maximum == 2
    assert (
        AdaptiveGate(tmp_path, search_cap=2, initial_search=2).snapshot()["lanes"][
            "search"
        ]["active"]
        == 0
    )


def test_unchanged_telemetry_and_full_lane_poll_do_not_fsync(setup, monkeypatch):
    gate, _ = setup
    established(gate)
    leases = [gate.acquire(0) for _ in range(5)]
    writes = []
    original = gate._write

    def track(state):
        writes.append(True)
        original(state)

    monkeypatch.setattr(gate, "_write", track)
    for _ in range(10):
        gate.snapshot()
    assert writes == []
    with pytest.raises(Busy):
        gate.acquire(0.1)
    assert len(writes) == 2  # Queue registration and removal, not every poll.
    for lease in leases:
        gate.finish(lease, "neutral")


def test_sibling_429_fallback_does_not_escalate_per_response(setup):
    gate, clock = setup
    established(gate)
    leases = [gate.acquire(0) for _ in range(5)]
    deadlines = []
    for lease in leases:
        gate.headers(lease, 429)
        deadlines.append(gate.snapshot()["retry_until"])
    assert deadlines == [clock() + 2] * 5
    assert gate.snapshot()["lanes"]["search"]["incidents"] == 1
    for lease in leases:
        gate.finish(lease, "cancelled")


def test_material_slow_cohort_reduces_even_with_regular_progress(setup):
    gate, clock = setup
    complete(gate, clock, [gate.acquire(0)], 1)
    established(gate)
    leases = [gate.acquire(0) for _ in range(3)]
    for _ in range(4):
        clock.advance(1)
        for lease in leases:
            gate.progress(lease)
    state = gate.snapshot()
    assert state["lanes"]["search"]["capacity"] == 2
    assert state["lanes"]["search"]["incidents"] == 1
    assert state["lanes"]["search"]["active"] == 3
    for lease in leases:
        gate.finish(lease, "cancelled")


def test_post_retry_recovery_admits_one_until_verified_success(setup):
    gate, clock = setup
    lease = gate.acquire(0)
    gate.headers(lease, 429, "2")
    gate.finish(lease, "cancelled")
    clock.advance(2)
    probe = gate.acquire(0)
    with pytest.raises(Busy):
        gate.acquire(0, "group")
    gate.finish(probe, "cancelled")
    assert gate.snapshot()["recovering"] is True
    probe = gate.acquire(0)
    complete(gate, clock, [probe], 0.1)
    assert gate.snapshot()["recovering"] is False
    assert gate.snapshot()["lanes"]["search"]["recovering"] is False
    leases = [gate.acquire(0), gate.acquire(0, "search_group")]
    for lease in leases:
        gate.finish(lease, "neutral")


def test_newer_request_does_not_jump_older_lane_waiter(setup):
    gate, _ = setup
    fd = pending(gate)
    try:
        with pytest.raises(Busy):
            gate.acquire(0)
        # Per-lane fairness should not prevent unrelated metadata traffic.
        lease = gate.acquire(0, "group")
        gate.finish(lease, "neutral")
    finally:
        os.close(fd)
    lease = gate.acquire(0)
    gate.finish(lease, "neutral")


def test_outcome_counters_distinguish_cancel_and_failure(setup):
    gate, _ = setup
    for outcome in ("success", "neutral", "cancelled", "failure"):
        lease = gate.acquire(0)
        gate.finish(lease, outcome)
    assert gate.snapshot()["outcomes"] == dict(
        success=1, neutral=1, cancelled=1, failure=1, faulted=0, crashed=0
    )


def test_global_half_open_probe_is_shared_between_processes(tmp_path):
    gate = AdaptiveGate(tmp_path)
    lease = gate.acquire(0)
    gate.headers(lease, 429, "0")
    gate.finish(lease, "failure")
    time.sleep(0.26)
    code = """
import sys
sys.path.insert(0, sys.argv[1])
from adaptive_gate import AdaptiveGate
g = AdaptiveGate(sys.argv[2])
l = g.acquire(2)
print('probe', flush=True)
sys.stdin.readline()
g.finish(l, 'success')
"""
    process = subprocess.Popen(
        [sys.executable, "-c", code, MODULE_DIR, str(tmp_path)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        assert process.stdout.readline().strip() == "probe"
        with pytest.raises(Busy):
            gate.acquire(0.05, "group")
        out, err = process.communicate("finish\n", timeout=5)
        assert process.returncode == 0, err
        assert gate.snapshot()["recovering"] is False
        current = gate.acquire(0, "group")
        gate.finish(current, "neutral")
    finally:
        if process.poll() is None:
            process.kill()
            process.communicate()


def test_new_burst_learns_changed_cost_with_one_valid_probe(setup):
    gate, clock = setup
    complete(gate, clock, [gate.acquire(0)], 0.15)
    lease = gate.acquire(0)
    clock.advance(0.6)
    # The isolated cost probe must not throttle unrelated established traffic.
    assert gate.snapshot()["lanes"]["search"]["incidents"] == 0
    gate.headers(lease, 200)
    gate.progress(lease)
    gate.finish(lease, "success")
    baseline = gate.snapshot()["baselines"]["search_text"]
    assert baseline["duration"] == pytest.approx(0.6)
    assert baseline["reference"]["duration"] == pytest.approx(0.15)


@pytest.mark.parametrize("interruption", ["failure", "neutral", "cancelled"])
def test_invalid_or_cancelled_cost_probe_never_trains(setup, interruption):
    gate, clock = setup
    complete(gate, clock, [gate.acquire(0)], 0.15)
    lease = gate.acquire(0)
    clock.advance(0.6)
    gate.finish(lease, interruption)
    assert gate.snapshot()["baselines"]["search_text"]["duration"] == pytest.approx(
        0.15
    )
    clock.advance(0.3)
    complete(gate, clock, [gate.acquire(0)], 0.6)
    assert gate.snapshot()["baselines"]["search_text"]["duration"] == pytest.approx(0.6)


def test_relearning_does_not_normalize_overloaded_concurrent_cohort(setup):
    gate, clock = setup
    complete(gate, clock, [gate.acquire(0)], 0.15)
    established(gate)
    leases = [gate.acquire(0) for _ in range(5)]
    complete(gate, clock, leases, 0.35)
    assert gate.snapshot()["baselines"]["search_text"]["duration"] == pytest.approx(
        0.15
    )


def test_fast_class_skips_blocked_slow_class_head_without_exceeding_total_cap(tmp_path):
    import threading

    clock = Clock()
    gate = AdaptiveGate(tmp_path, clock=clock)
    fast, slow = "search_text/single/short/small", "search_text/all/long/large"
    complete(gate, clock, [gate.acquire(0, fast)], 0.1)
    complete(gate, clock, [gate.acquire(0, slow)], 0.1)
    with gate._transaction() as state:
        state["lanes"]["search"]["capacity"] = 8
    established(gate, slow, capacity=8)
    slow_lease = gate.acquire(0, slow)
    clock.advance(0.3)
    gate.snapshot()
    recovery_probe = gate.acquire(0, slow)
    stopped = threading.Event()
    result = []

    def wait_slow():
        try:
            result.append(gate.acquire(2, slow, cancelled=stopped.is_set))
        except Cancelled:
            pass

    worker = threading.Thread(target=wait_slow)
    worker.start()
    try:
        deadline = time.monotonic() + 1
        while gate.snapshot()["lanes"]["search"]["queued"] < 1:
            assert time.monotonic() < deadline
            time.sleep(0.005)
        quick = gate.acquire(0.1, fast)
        snapshot = gate.snapshot()["lanes"]["search"]
        assert snapshot["active"] == 3
        assert snapshot["capacity"] == 4
        assert result == []
        gate.finish(quick, "neutral")
    finally:
        stopped.set()
        worker.join(timeout=3)
        gate.finish(recovery_probe, "cancelled")
        gate.finish(slow_lease, "cancelled")
        for lease in result:
            gate.finish(lease, "cancelled")


def test_failed_recovery_probe_never_clears_new_retry_after(setup):
    gate, clock = setup
    lease = gate.acquire(0)
    gate.headers(lease, 429, "2")
    gate.finish(lease, "failure")
    clock.advance(2)
    probe = gate.acquire(0)
    gate.headers(probe, 429, "20")
    gate.finish(probe, "failure")
    assert gate.snapshot()["retry_until"] == clock() + 20
    assert gate.snapshot()["recovering"] is True
    with pytest.raises(Busy):
        gate.acquire(0, "group")


def test_continuous_queue_reprobes_changed_cost_without_six_serial_requests(setup):
    gate, clock = setup
    lease = gate.acquire(0)
    fd = pending(gate)
    try:
        complete(gate, clock, [lease], 0.15)
    finally:
        os.close(fd)
    # Demand never drains: the second cohort's changed cost must trigger one
    # bounded reprobe, rather than permanently comparing with the old baseline.
    for _ in range(3):
        state = gate.snapshot()
        baseline = state["baselines"]["search_text"]
        count = (
            1
            if baseline["recovering"]
            else min(baseline["control"]["limit"], state["lanes"]["search"]["capacity"])
        )
        leases = [gate.acquire(0) for _ in range(count)]
        fd = pending(gate)
        try:
            clock.advance(0.45)
            gate.snapshot()
            for lease in leases:
                gate.headers(lease, 200)
                gate.progress(lease)
                gate.finish(lease)
        finally:
            os.close(fd)
    assert gate.snapshot()["baselines"]["search_text"]["duration"] == pytest.approx(
        0.45
    )
    assert gate.snapshot()["lanes"]["search"]["capacity"] >= 2


def test_rolling_goodput_counts_successful_work_carried_across_capacity_change(setup):
    gate, clock = setup
    complete(gate, clock, [gate.acquire(0)], 0.15)
    established(gate, capacity=2)
    with gate._transaction() as state:
        state["baselines"]["search_text"]["control"].update(safe=1, goodput=1 / 0.15)
    a, b = gate.acquire(0), gate.acquire(0)
    clock.advance(0.15)
    gate.headers(a, 200)
    gate.progress(a)
    gate.finish(a)
    carried = gate.acquire(0)  # Admitted at limit 2, but completes under limit 4.
    clock.advance(0.01)
    gate.headers(b, 200)
    gate.progress(b)
    gate.finish(b)
    assert gate.snapshot()["baselines"]["search_text"]["control"]["limit"] == 4
    current = [gate.acquire(0) for _ in range(3)]
    fd = pending(gate)
    try:
        clock.advance(0.14)
        gate.headers(carried, 200)
        gate.progress(carried)
        gate.finish(carried)
        clock.advance(0.01)
        for lease in current:
            gate.headers(lease, 200)
            gate.progress(lease)
            gate.finish(lease)
        control = gate.snapshot()["baselines"]["search_text"]["control"]
        assert control["safe"] == 4
        assert control["goodput"] == pytest.approx(4 / 0.15)
    finally:
        os.close(fd)


def test_latency_recovery_uses_spare_capacity_without_waiting_old_streams(setup):
    gate, clock = setup
    complete(gate, clock, [gate.acquire(0)], 0.15)
    with gate._transaction() as state:
        state["lanes"]["search"]["capacity"] = 8
    established(gate, capacity=8)
    old = [gate.acquire(0) for _ in range(2)]
    clock.advance(0.5)
    state = gate.snapshot()
    assert state["lanes"]["search"]["capacity"] == 4
    assert state["lanes"]["search"]["active"] == 2
    probe = gate.acquire(0)
    with pytest.raises(Busy):
        gate.acquire(0)
    complete(gate, clock, [probe], 0.35)
    assert gate.snapshot()["baselines"]["search_text"]["duration"] == pytest.approx(
        0.35
    )
    fresh = [gate.acquire(0) for _ in range(2)]
    assert gate.snapshot()["lanes"]["search"]["active"] == 4
    with pytest.raises(Busy):
        gate.acquire(0)
    before = gate.snapshot()["baselines"]["search_text"]
    # Old acknowledged slow work finishes later; it must not reset the new
    # operating baseline, recovery flag, or in-progress fresh measurement.
    for lease in old:
        gate.headers(lease, 200)
        gate.progress(lease)
        gate.finish(lease)
    after = gate.snapshot()["baselines"]["search_text"]
    assert after == before
    for lease in fresh:
        gate.finish(lease, "neutral")


def test_http_failure_recovery_still_drains_before_probe(setup):
    gate, clock = setup
    established(gate)
    old = [gate.acquire(0) for _ in range(2)]
    gate.headers(old[0], 503)
    gate.finish(old[0], "failure")
    clock.advance(0.3)
    with pytest.raises(Busy):
        gate.acquire(0)
    gate.finish(old[1], "neutral")
    probe = gate.acquire(0)
    gate.finish(probe, "neutral")


def test_unrealized_two_request_trial_is_not_rejected_as_serial_slowdown(setup):
    gate, clock = setup
    complete(gate, clock, [gate.acquire(0)], 0.15)
    with gate._transaction() as state:
        state["lanes"]["search"]["capacity"] = 8
    established(gate, capacity=8)
    old = [gate.acquire(0) for _ in range(3)]
    clock.advance(0.5)
    gate.snapshot()
    probe = gate.acquire(0)
    complete(gate, clock, [probe], 0.35)
    # Three old permits leave just one slot: two new requests must run serially.
    for _ in range(6):
        fresh = gate.acquire(0)
        complete(gate, clock, [fresh], 0.35)
    assert gate.snapshot()["baselines"]["search_text"]["control"]["limit"] == 2
    assert gate.snapshot()["lanes"]["search"]["capacity"] == 4
    assert gate.snapshot()["lanes"]["search"]["budget_trial"] is False
    for lease in old:
        gate.finish(lease, "cancelled")


def test_healthy_aggregate_class_demand_can_probe_shared_budget(setup):
    gate, clock = setup
    classes = [
        "search_text/single/short/small",
        "search_text/multi/short/small",
        "search_text/all/short/small",
    ]
    maximum = 0
    for wave in range(20):
        leases = []
        for n in range(18):
            try:
                leases.append(gate.acquire(0, classes[(wave + n) % len(classes)]))
            except Busy:
                pass
        fds = []
        with gate._transaction() as state:
            for index, request_class in enumerate(classes, 1000):
                fd = gate._try_lock(f"wait-{index}.lock", create=True)
                fds.append(fd)
                state["tickets"] += 1
                state["pending"][str(index)] = {
                    "token": f"{index:032x}",
                    "lane": "search",
                    "class": request_class,
                    "ticket": state["tickets"],
                }
        try:
            complete(gate, clock, leases, 0.1)
            maximum = max(maximum, gate.snapshot()["lanes"]["search"]["capacity"])
        finally:
            for fd in fds:
                os.close(fd)
    assert maximum > 5


def test_shared_budget_trial_rolls_back_on_latency_inflation(setup):
    gate, clock = setup
    complete(gate, clock, [gate.acquire(0)], 0.1)
    established(gate)
    with gate._transaction() as state:
        state["lanes"]["search"].update(
            capacity=6, budget_trial=True, budget_base=5, budget_peak=6
        )
    complete(gate, clock, [gate.acquire(0)], 0.14)
    lane = gate.snapshot()["lanes"]["search"]
    assert lane["capacity"] == 5
    assert lane["budget_trial"] is False
    assert lane["incidents"] == 1


def test_shared_budget_confirms_only_one_fully_realized_healthy_window(setup):
    gate, clock = setup
    complete(gate, clock, [gate.acquire(0)], 0.1)
    established(gate, capacity=6)
    with gate._transaction() as state:
        state["lanes"]["search"].update(
            capacity=6, budget_trial=True, budget_base=5, budget_peak=0
        )
    leases = [gate.acquire(0) for _ in range(6)]
    fd = pending(gate)
    try:
        complete(gate, clock, leases, 0.1)
        lane = gate.snapshot()["lanes"]["search"]
        assert lane["capacity"] == 6
        assert lane["budget_base"] == 6
        assert lane["budget_trial"] is False
    finally:
        os.close(fd)


def test_one_queued_class_can_expand_shared_budget_occupied_by_other_class(setup):
    gate, clock = setup
    fast = "search_text/single/short/small"
    slow = "search_text/all/long/large"
    complete(gate, clock, [gate.acquire(0, fast)], 0.1)
    complete(gate, clock, [gate.acquire(0, slow)], 10)
    established(gate, fast, capacity=4)
    established(gate, slow, capacity=3)
    slow_leases = [gate.acquire(0, slow) for _ in range(3)]
    metadata = gate.acquire(0, "group")
    try:
        for _ in range(3):
            fast_leases = [gate.acquire(0, fast) for _ in range(2)]
            fd = gate._try_lock("wait-1000.lock", create=True)
            try:
                with gate._transaction() as state:
                    state["tickets"] += 1
                    state["pending"]["1000"] = {
                        "token": "a" * 32,
                        "lane": "search",
                        "class": fast,
                        "ticket": state["tickets"],
                    }
                complete(gate, clock, fast_leases, 0.1)
            finally:
                os.close(fd)
        lanes = gate.snapshot()["lanes"]
        assert lanes["search"]["capacity"] == 6
        assert lanes["search"]["budget_trial"] is True
        assert lanes["metadata"]["capacity"] == 2
    finally:
        for lease in slow_leases:
            gate.finish(lease, "cancelled")
        gate.finish(metadata, "cancelled")
    # A previous full window must not make metadata occupancy count as search
    # demand: 3 active searches + 1 eligible search slot still fit budget 5,
    # even while two metadata permits are held.
    established(gate, fast, capacity=2)
    established(gate, slow, capacity=2)
    slow_leases = [gate.acquire(0, slow) for _ in range(2)]
    candidate = gate.acquire(0, fast)
    metadata_leases = [gate.acquire(0, "group"), gate.acquire(0, "user_groups")]
    fd = gate._try_lock("wait-1000.lock", create=True)
    try:
        with gate._transaction() as state:
            state["lanes"]["search"].update(successes=4, budget_peak=5)
            state["baselines"][fast]["control"].update(
                durations=[], peak=1, started=clock()
            )
            state["tickets"] += 1
            state["pending"]["1000"] = {
                "token": "b" * 32,
                "lane": "search",
                "class": fast,
                "ticket": state["tickets"],
            }
        complete(gate, clock, [candidate], 0.1)
        assert gate.snapshot()["lanes"]["search"]["capacity"] == 5
    finally:
        os.close(fd)
        for lease in slow_leases + metadata_leases:
            gate.finish(lease, "cancelled")
