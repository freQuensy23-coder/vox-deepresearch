"""Local, cross-process adaptive admission; never cancels an upstream request.

The owner must call ``headers`` before forwarding response headers, ``progress``
only for meaningful application data, and ``finish`` only after transport closure.
All capacity/incident state lives behind a stable flock; slot locks establish actual
ownership even after a crashed process. Nothing here makes or retries HTTP calls.
"""

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import timezone
from email.utils import parsedate_to_datetime
import fcntl
import json
import math
import os
from pathlib import Path
import tempfile
import time
import uuid


CLASSES = {
    "search_text": "search",
    "search_group": "search",
    "group_messages": "search",
    "group_analytics": "search",
    "group_custom_analytics": "search",
    "reporter_preview": "search",
    "user_messages": "search",
    "search": "search",
    "group": "metadata",
    "user_groups": "metadata",
    "user_top_groups": "metadata",
    "metadata": "metadata",
}
MAX_CLASSES = 64
MAX_PENDING = 1024


def _lane(name):
    if not isinstance(name, str) or len(name) > 96:
        raise ValueError("invalid request class")
    pieces = name.split("/")
    if pieces[0] not in CLASSES:
        raise ValueError("unknown request class")
    if len(pieces) != 1 and (
        len(pieces) != 4
        or pieces[1] not in {"single", "multi", "all"}
        or pieces[2] not in {"short", "medium", "long", "open"}
        or pieces[3] not in {"small", "medium", "large"}
    ):
        raise ValueError("invalid comparable request class")
    return CLASSES[pieces[0]]


MAX_STATE_BYTES = 512 * 1024


class Busy(Exception):
    """No admission before the caller's wait deadline."""


class Cancelled(Exception):
    """The downstream caller cancelled while waiting."""


@dataclass
class Lease:
    token: str
    slot: str
    fd: int
    owner: object
    finished: bool = False


def _unique(pairs):
    result = dict(pairs)
    if len(result) != len(pairs):
        raise ValueError("duplicate controller state field")
    return result


def _number(value, low=0, high=253402300799):
    return type(value) in (int, float) and math.isfinite(value) and low <= value <= high


def _integer(value, low=0, high=2**63 - 1):
    return type(value) is int and low <= value <= high


def _token(value):
    return (
        isinstance(value, str)
        and len(value) == 32
        and all(c in "0123456789abcdef" for c in value)
    )


class AdaptiveGate:
    """Two adaptive lanes sharing upstream Retry-After, with no static spacing.

    Cold-start search estimates (10 s completion, 9 s first progress) are startup
    estimates, not claimed Vox quotas. Per-route healthy low-load completions
    refine them; overloaded cohorts cannot redefine a slow service as healthy.
    """

    def __init__(
        self,
        directory,
        *,
        search_cap=32,
        metadata_cap=4,
        initial_search=5,
        poll_interval=0.02,
        clock=time.time,
        monotonic=time.monotonic,
        sleep=time.sleep,
    ):
        if not _integer(search_cap, 1, 32) or not _integer(metadata_cap, 1, 8):
            raise ValueError("invalid capacity")
        if not _integer(initial_search, 1, search_cap) or not _number(
            poll_interval, 0.001, 1
        ):
            raise ValueError("invalid initial capacity or polling interval")
        self.directory = Path(directory)
        self.directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        if self.directory.is_symlink():
            raise ValueError("invalid state directory")
        self.limits = {"search": search_cap, "metadata": metadata_cap}
        self.initial = {"search": initial_search, "metadata": min(2, metadata_cap)}
        self.clock, self.monotonic, self.sleep = clock, monotonic, sleep
        self.poll_interval = poll_interval
        self._retained = {}  # Faulted transports keep locks until process exit.
        fd = self._open("gate.lock", create=True)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            state_path = self.directory / "state.json"
            if not state_path.exists():
                # Existing lock alone may be left by a crash before first state write.
                # Existing slots mean state was lost: never silently reinitialize.
                if any(self.directory.glob("slot-*.lock")):
                    raise ValueError("missing controller state")
                state = self._new_state()
                self._write(state)
            self._load()
            for lane, count in self.limits.items():
                for index in range(count):
                    os.close(self._open(f"slot-{lane}-{index}.lock", create=True))
        finally:
            os.close(fd)

    def _open(self, name, *, create=False):
        return os.open(
            self.directory / name,
            os.O_RDWR | os.O_NOFOLLOW | (os.O_CREAT if create else 0),
            0o600,
        )

    @staticmethod
    def _control():
        return {
            "limit": 1,
            "safe": 1,
            "probe": True,
            "epoch": 0,
            "started": 0,
            "durations": [],
            "goodput": 0,
            "hold": 0,
            "hold_rounds": 1,
            "peak": 0,
        }

    def _new_state(self):
        return {
            "version": 4,
            "limits": self.limits,
            "initial": self.initial,
            "retry_until": 0,
            "requests": 0,
            "tickets": 0,
            "recovering": False,
            "outcomes": {
                name: 0
                for name in (
                    "success",
                    "failure",
                    "cancelled",
                    "neutral",
                    "faulted",
                    "crashed",
                )
            },
            "lanes": {
                lane: {
                    "capacity": self.initial[lane],
                    "generation": 0,
                    "successes": 0,
                    "incidents": 0,
                    "last_pressure": 0,
                    "hold_until": 0,
                    "recovering": False,
                    "budget_trial": False,
                    "budget_base": self.initial[lane],
                    "budget_peak": 0,
                }
                for lane in self.limits
            },
            "baselines": {
                name: {
                    "duration": 10.0 if lane == "search" else 1.0,
                    "header": 9.0 if lane == "search" else 1.0,
                    "gap": 9.0 if lane == "search" else 1.0,
                    "samples": 0,
                    "reference": None,
                    "recovering": False,
                    "calibration": [],
                    "control": self._control(),
                }
                for name, lane in CLASSES.items()
            },
            "active": {},
            "pending": {},
        }

    def _load(self):
        fd = os.open(self.directory / "state.json", os.O_RDONLY | os.O_NOFOLLOW)
        with os.fdopen(fd, "rb") as stream:
            raw = stream.read(MAX_STATE_BYTES + 1)
        if len(raw) > MAX_STATE_BYTES:
            raise ValueError("oversized controller state")
        try:
            state = json.loads(raw, object_pairs_hook=_unique)
            self._validate(state)
        except (TypeError, KeyError, AttributeError, OverflowError) as exc:
            raise ValueError("invalid controller state") from exc
        return state

    def _validate(self, state):
        if not isinstance(state, dict) or set(state) != {
            "version",
            "limits",
            "initial",
            "retry_until",
            "requests",
            "tickets",
            "recovering",
            "outcomes",
            "lanes",
            "baselines",
            "active",
            "pending",
        }:
            raise ValueError("invalid controller state")
        if type(state["version"]) is not int or state["version"] != 4:
            raise ValueError("unsupported controller state version")
        if state["limits"] != self.limits or state["initial"] != self.initial:
            raise ValueError("controller configuration mismatch")
        if type(state["recovering"]) is not bool or not _integer(state["tickets"]):
            raise ValueError("invalid recovery state")
        if set(state["outcomes"]) != {
            "success",
            "failure",
            "cancelled",
            "neutral",
            "faulted",
            "crashed",
        } or any(not _integer(v) for v in state["outcomes"].values()):
            raise ValueError("invalid outcome counters")
        if not _number(state["retry_until"]) or not _integer(state["requests"]):
            raise ValueError("invalid controller counters")
        if set(state["lanes"]) != set(self.limits):
            raise ValueError("invalid controller classes")
        for lane, value in state["lanes"].items():
            if set(value) != {
                "capacity",
                "generation",
                "successes",
                "incidents",
                "last_pressure",
                "hold_until",
                "recovering",
                "budget_trial",
                "budget_base",
                "budget_peak",
            }:
                raise ValueError("invalid lane state")
            if (
                type(value["recovering"]) is not bool
                or type(value["budget_trial"]) is not bool
            ):
                raise ValueError("invalid lane recovery")
            if not _integer(value["budget_base"], 1, self.limits[lane]) or not _integer(
                value["budget_peak"], 0, self.limits[lane]
            ):
                raise ValueError("invalid shared budget")
            if not _integer(value["capacity"], 1, self.limits[lane]):
                raise ValueError("invalid effective capacity")
            if any(
                not _integer(value[key])
                for key in ("generation", "successes", "incidents")
            ):
                raise ValueError("invalid lane counters")
            if any(not _number(value[key]) for key in ("last_pressure", "hold_until")):
                raise ValueError("invalid lane timestamps")
        if (
            not isinstance(state["baselines"], dict)
            or not set(CLASSES) <= set(state["baselines"])
            or len(state["baselines"]) > MAX_CLASSES
        ):
            raise ValueError("invalid class baselines")
        for name, value in state["baselines"].items():
            _lane(name)
            if set(value) != {
                "duration",
                "header",
                "gap",
                "samples",
                "reference",
                "recovering",
                "calibration",
                "control",
            } or not _integer(value["samples"]):
                raise ValueError("invalid baseline")
            if any(
                not _number(value[key], 0.001, 86400)
                for key in ("duration", "header", "gap")
            ):
                raise ValueError("invalid baseline timing")
            if (
                type(value["recovering"]) is not bool
                or not isinstance(value["calibration"], list)
                or len(value["calibration"]) > 6
            ):
                raise ValueError("invalid calibration state")
            control = value["control"]
            if not isinstance(control, dict) or set(control) != set(self._control()):
                raise ValueError("invalid cohort control")
            if (
                not all(
                    _integer(control[k], 1, self.limits[_lane(name)])
                    for k in ("limit", "safe")
                )
                or type(control["probe"]) is not bool
            ):
                raise ValueError("invalid class capacity")
            if (
                not _integer(control["epoch"])
                or not _integer(control["hold"], 0, 8)
                or not _integer(control["hold_rounds"], 1, 8)
            ):
                raise ValueError("invalid cohort counters")
            if not _integer(control["peak"], 0, self.limits[_lane(name)]):
                raise ValueError("invalid realized concurrency")
            if (
                not _number(control["started"])
                or not _number(control["goodput"], 0, 1e9)
                or not isinstance(control["durations"], list)
                or len(control["durations"]) > self.limits[_lane(name)]
                or any(not _number(d, 0.001, 86400) for d in control["durations"])
            ):
                raise ValueError("invalid cohort measurements")
            observations = value["calibration"] + (
                [value["reference"]] if value["reference"] is not None else []
            )
            for observation in observations:
                if (
                    not isinstance(observation, dict)
                    or set(observation) != {"duration", "header", "gap"}
                    or any(not _number(v, 0.001, 86400) for v in observation.values())
                ):
                    raise ValueError("invalid calibration observation")
        if not isinstance(state["active"], dict) or len(state["active"]) > sum(
            self.limits.values()
        ):
            raise ValueError("invalid active reservations")
        valid_slots = {
            f"{lane}-{i}" for lane, count in self.limits.items() for i in range(count)
        }
        for slot, value in state["active"].items():
            if slot not in valid_slots or set(value) != {
                "token",
                "class",
                "generation",
                "capacity",
                "started",
                "headers",
                "progress",
                "first_progress",
                "max_gap",
                "faulted",
                "pending_at_start",
                "pressure_seen",
                "probe",
                "class_probe",
                "exclusive",
                "failure_seen",
                "class_epoch",
                "cost_probe",
            }:
                raise ValueError("invalid reservation")
            if not _token(value["token"]) or value["class"] not in state["baselines"]:
                raise ValueError("invalid reservation identity")
            lane = _lane(value["class"])
            if not slot.startswith(lane + "-") or not _integer(
                value["capacity"], 1, self.limits[lane]
            ):
                raise ValueError("invalid reservation lane")
            if (
                not _integer(value["generation"])
                or value["generation"] > state["lanes"][lane]["generation"]
            ):
                raise ValueError("invalid reservation generation")
            if any(
                not _number(value[key])
                for key in (
                    "started",
                    "headers",
                    "progress",
                    "first_progress",
                    "max_gap",
                )
            ):
                raise ValueError("invalid reservation timestamps")
            if any(
                type(value[key]) is not bool
                for key in (
                    "faulted",
                    "pending_at_start",
                    "pressure_seen",
                    "probe",
                    "class_probe",
                    "exclusive",
                    "failure_seen",
                    "cost_probe",
                )
            ):
                raise ValueError("invalid reservation flags")
            if not _integer(value["class_epoch"]):
                raise ValueError("invalid class epoch")
        if (
            not isinstance(state["pending"], dict)
            or len(state["pending"]) > MAX_PENDING
        ):
            raise ValueError("invalid pending reservations")
        for index, value in state["pending"].items():
            if (
                not index.isdigit()
                or str(int(index)) != index
                or not 0 <= int(index) < MAX_PENDING
            ):
                raise ValueError("invalid pending slot")
            if (
                set(value) != {"token", "lane", "ticket", "class"}
                or value["lane"] not in self.limits
                or not _token(value["token"])
                or value["class"] not in state["baselines"]
                or _lane(value["class"]) != value["lane"]
            ):
                raise ValueError("invalid pending record")
            if not _integer(value["ticket"]) or value["ticket"] > state["tickets"]:
                raise ValueError("invalid pending ticket")

    def _write(self, state):
        self._validate(state)
        temporary = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                dir=self.directory,
                prefix=".state-",
                delete=False,
            ) as stream:
                temporary = Path(stream.name)
                json.dump(state, stream, allow_nan=False, separators=(",", ":"))
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, self.directory / "state.json")
            temporary = None
            fd = os.open(self.directory, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)

    @contextmanager
    def _transaction(self):
        fd = self._open("gate.lock")
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            state = self._load()
            before = json.dumps(state, separators=(",", ":"))
            self._reap(state)
            yield state
            # Polling a full lane and reading telemetry are normally read-only.
            # Rewriting/fsyncing on every waiter poll would throttle the relay
            # itself and contaminate the upstream-latency measurements.
            if json.dumps(state, separators=(",", ":")) != before:
                self._write(state)
        finally:
            os.close(fd)

    def _try_lock(self, name, *, create=False):
        fd = self._open(name, create=create)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return fd
        except BlockingIOError:
            os.close(fd)
            return None

    def _reap(self, state):
        # Time/PID alone never reclaim a live upstream permit.
        for kind, prefix in (("active", "slot-"), ("pending", "wait-")):
            for slot in list(state[kind]):
                fd = self._try_lock(f"{prefix}{slot}.lock")
                if fd is not None:
                    os.close(fd)
                    if kind == "active":
                        state["baselines"][state[kind][slot]["class"]][
                            "calibration"
                        ] = []
                    del state[kind][slot]
                    if kind == "active":
                        state["outcomes"]["crashed"] += 1

    def _pressure(
        self, state, lane, generation, now, *, hold=0, recover=False, target=None
    ):
        value = state["lanes"][lane]
        if recover:
            for record in state["active"].values():
                if _lane(record["class"]) == lane:
                    record["failure_seen"] = True
                    state["baselines"][record["class"]]["calibration"] = []
        if generation != value["generation"]:
            return
        value["capacity"] = max(1, value["capacity"] // 2) if target is None else target
        value["budget_trial"] = False
        value["budget_base"] = value["capacity"]
        value["budget_peak"] = 0
        value["generation"] += 1
        value["successes"] = 0
        value["incidents"] += 1
        value["last_pressure"] = now
        value["hold_until"] = max(value["hold_until"], now + hold)
        value["recovering"] = value["recovering"] or recover
        # Every currently active lease predates this pressure boundary, even if
        # its own timing has not crossed the threshold yet.
        for record in state["active"].values():
            if _lane(record["class"]) == lane:
                record["pressure_seen"] = True

    def _latency_pressure(self, state, record, now):
        lane = _lane(record["class"])
        baseline = state["baselines"][record["class"]]
        baseline["recovering"] = True
        self._pressure(state, lane, record["generation"], now)
        control = baseline["control"]
        if not control["probe"]:
            control.update(limit=1, safe=1, probe=True, started=0, durations=[], peak=0)
            control["epoch"] += 1

    @staticmethod
    def _acknowledged_latency(state, record):
        return (
            record["pressure_seen"]
            and not record["failure_seen"]
            and record["generation"]
            < state["lanes"][_lane(record["class"])]["generation"]
        )

    def _current_pressure(self, state, lane, now):
        stalled = set()
        cohorts = {}
        for record in state["active"].values():
            if _lane(record["class"]) != lane:
                continue
            baseline = state["baselines"][record["class"]]
            # A new-burst cost probe is already isolated to one request. Its
            # unfamiliar cost must not hold unrelated classes behind it.
            if record["cost_probe"] or self._acknowledged_latency(state, record):
                continue
            cohorts.setdefault(record["class"], []).append(record)
            # Headers can arrive early while the query is still stalled. First
            # meaningful data, or later meaningful data gaps, are the signal.
            last = record["progress"] or record["started"]
            threshold = max(
                0.05,
                2.5 * (baseline["gap"] if record["progress"] else baseline["header"]),
            )
            if now - last > threshold:
                stalled.add(record["class"])
                self._latency_pressure(state, record, now)
        # Regular progress must not conceal a material increase in work cost.
        # One progressing outlier is insufficient: require at least two slow
        # comparable active requests and a majority of that class's cohort.
        for request_class, records in cohorts.items():
            slow = [
                r
                for r in records
                if now - r["started"]
                > 3 * state["baselines"][request_class]["duration"]
            ]
            if len(slow) >= 2 and len(slow) * 2 >= len(records):
                for record in slow:
                    self._latency_pressure(state, record, now)
        return stalled

    def acquire(self, wait, request_class="search_text", cancelled=None):
        if not _number(wait, 0, 86400):
            raise ValueError("invalid admission request")
        lane = _lane(request_class)
        deadline = self.monotonic() + wait
        token = uuid.uuid4().hex
        pending_fd = pending_index = None
        try:
            with self._transaction() as state:
                if request_class not in state["baselines"]:
                    if len(state["baselines"]) >= MAX_CLASSES:
                        # Bounded safe fallback shares the base route's timing.
                        request_class = request_class.split("/")[0]
                    else:
                        state["baselines"][request_class] = self._new_state()[
                            "baselines"
                        ][request_class.split("/")[0]]
                for index in range(MAX_PENDING):
                    if str(index) not in state["pending"]:
                        pending_fd = self._try_lock(f"wait-{index}.lock", create=True)
                        if pending_fd is not None:
                            pending_index = str(index)
                            state["tickets"] += 1
                            state["pending"][pending_index] = {
                                "token": token,
                                "lane": lane,
                                "ticket": state["tickets"],
                                "class": request_class,
                            }
                            break
                if pending_fd is None:
                    raise Busy("admission queue full")
            while True:
                if cancelled is not None and cancelled():
                    raise Cancelled()
                lease = None
                try:
                    with self._transaction() as state:
                        now = self.clock()
                        pressured = self._current_pressure(state, lane, now)
                        value = state["lanes"][lane]
                        active = sum(
                            _lane(r["class"]) == lane for r in state["active"].values()
                        )
                        faulted = any(r["faulted"] for r in state["active"].values())
                        # A stalled class cannot monopolize the FIFO head.
                        # It gets one probe after its previous requests drain;
                        # other classes remain eligible within the common cap.
                        active_classes = {}
                        for peer in state["active"].values():
                            # Acknowledged old slow streams retain their global
                            # OS permits. Only the fresh class probe/cohort is
                            # counted against that class's new experiment limit.
                            if not self._acknowledged_latency(state, peer):
                                active_classes[peer["class"]] = (
                                    active_classes.get(peer["class"], 0) + 1
                                )

                        def class_ready(name):
                            count = active_classes.get(name, 0)
                            return (
                                name not in pressured
                                and count < state["baselines"][name]["control"]["limit"]
                                and (
                                    not state["baselines"][name]["recovering"]
                                    or count == 0
                                )
                            )

                        eligible = [
                            v
                            for v in state["pending"].values()
                            if v["lane"] == lane and class_ready(v["class"])
                        ]
                        first = (
                            bool(eligible)
                            and min(eligible, key=lambda v: v["ticket"])["token"]
                            == token
                        )
                        recovery_ready = (
                            not state["recovering"] or not state["active"]
                        ) and (not value["recovering"] or active == 0)
                        if (
                            first
                            and recovery_ready
                            and not faulted
                            and now >= max(state["retry_until"], value["hold_until"])
                            and active < value["capacity"]
                        ):
                            for index in range(self.limits[lane]):
                                slot = f"{lane}-{index}"
                                if slot in state["active"]:
                                    continue
                                fd = self._try_lock(f"slot-{slot}.lock")
                                if fd is None:
                                    continue
                                lease = Lease(token, slot, fd, self)
                                control = state["baselines"][request_class]["control"]
                                realized = active_classes.get(request_class, 0) + 1
                                old_peak = control["peak"]
                                control["peak"] = max(old_peak, realized)
                                if old_peak < control["limit"] <= realized:
                                    # Measure a concurrency experiment only once
                                    # its requested allocation actually exists.
                                    control["started"] = now
                                    control["durations"] = []
                                elif not control["started"]:
                                    control["started"] = now
                                value["budget_peak"] = max(
                                    value["budget_peak"], active + 1
                                )
                                for peer in state["active"].values():
                                    if _lane(peer["class"]) == lane:
                                        peer["exclusive"] = False
                                state["active"][slot] = {
                                    "token": token,
                                    "class": request_class,
                                    "generation": value["generation"],
                                    "capacity": value["capacity"],
                                    "started": now,
                                    "headers": 0,
                                    "progress": 0,
                                    "first_progress": 0,
                                    "max_gap": 0,
                                    "faulted": False,
                                    "pending_at_start": any(
                                        v["lane"] == lane and v["token"] != token
                                        for v in state["pending"].values()
                                    ),
                                    "pressure_seen": False,
                                    "probe": state["recovering"] or value["recovering"],
                                    "class_probe": state["baselines"][request_class][
                                        "recovering"
                                    ],
                                    "exclusive": active == 0,
                                    "failure_seen": False,
                                    "class_epoch": control["epoch"],
                                    "cost_probe": control["probe"],
                                }
                                state["requests"] += 1
                                del state["pending"][pending_index]
                                break
                    if lease is not None:
                        return lease
                except BaseException:
                    if lease is not None:
                        os.close(lease.fd)
                    raise
                remaining = deadline - self.monotonic()
                if remaining <= 0:
                    raise Busy("admission wait expired")
                self.sleep(min(remaining, self.poll_interval))
        finally:
            if pending_fd is not None:
                try:
                    with self._transaction() as state:
                        value = state["pending"].get(pending_index)
                        if value is not None and value["token"] == token:
                            del state["pending"][pending_index]
                finally:
                    os.close(pending_fd)

    def _record(self, state, lease):
        if not isinstance(lease, Lease) or lease.owner is not self or lease.finished:
            raise ValueError("invalid or closed lease")
        record = state["active"].get(lease.slot)
        if record is None or record["token"] != lease.token:
            raise ValueError("lost admission lease")
        return record

    @staticmethod
    def _retry_deadline(value, now):
        if value is None:
            return None
        try:
            value = str(value).strip()
            delay = float(value)
            if not _number(delay):
                return None
            return min(253402300799, now + delay)
        except (ValueError, TypeError, OverflowError):
            try:
                parsed = parsedate_to_datetime(value)
                timestamp = parsed.replace(
                    tzinfo=parsed.tzinfo or timezone.utc
                ).timestamp()
                return max(now, timestamp) if _number(timestamp) else None
            except (ValueError, TypeError, OverflowError):
                return None

    def headers(self, lease, status, retry_after=None):
        if not _integer(status, 100, 599):
            raise ValueError("invalid status")
        with self._transaction() as state:
            record = self._record(state, lease)
            now = self.clock()
            record["headers"] = record["headers"] or now
            deadline = self._retry_deadline(retry_after, now)
            if deadline is not None:
                # Scope is unknown: this applies across both request lanes.
                state["retry_until"] = max(state["retry_until"], deadline)
                state["recovering"] = True
            lane = _lane(record["class"])
            if status == 429 or status >= 500:
                if status == 429 and deadline is None:
                    # A fallback, not a claim about upstream window semantics.
                    # Siblings from the same failed admission generation use
                    # the same fallback, rather than escalating per response.
                    delay = min(30.0, 2.0 * 2 ** min(record["generation"], 4))
                    state["retry_until"] = max(state["retry_until"], now + delay)
                if status == 429:
                    state["recovering"] = True
                    for peer in state["active"].values():
                        peer["failure_seen"] = True
                        state["baselines"][peer["class"]]["calibration"] = []
                state["baselines"][record["class"]]["recovering"] = True
                self._pressure(
                    state, lane, record["generation"], now, hold=0.25, recover=True
                )

    def progress(self, lease):
        with self._transaction() as state:
            record = self._record(state, lease)
            now = self.clock()
            record["max_gap"] = max(
                record["max_gap"], now - (record["progress"] or record["started"])
            )
            record["progress"] = now
            record["first_progress"] = record["first_progress"] or now

    def _cohort_complete(self, state, record, outcome, timings, now):
        """Measure only admitted upstream work, never caller queue wait."""
        baseline = state["baselines"][record["class"]]
        control = baseline["control"]
        lane = _lane(record["class"])
        if outcome != "success" or record["failure_seen"] or now < state["retry_until"]:
            control["durations"] = []
            control["peak"] = 0
            control["started"] = 0
            control["peak"] = 0
            control["epoch"] += 1
            control["limit"] = max(
                1, min(control["safe"], state["lanes"][lane]["capacity"])
            )
            if outcome == "failure":
                control["probe"] = True
                control["limit"] = 1
            return False
        if (
            control["probe"]
            and not record["cost_probe"]
            and record["class_epoch"] != control["epoch"]
        ):
            return False
        if record["pressure_seen"] and not record["cost_probe"]:
            if record["class_epoch"] != control["epoch"]:
                return False
            control["limit"] = 1
            control["safe"] = 1
            control["probe"] = True
            control["durations"] = []
            control["peak"] = 0
            control["started"] = 0
            control["peak"] = 0
            control["epoch"] += 1
            return False
        if record["cost_probe"]:
            # A drained class probes its current cost once. Validated completed
            # work supplies a new operating baseline; history remains available.
            if baseline["reference"] is None:
                baseline["reference"] = timings.copy()
            baseline.update(timings)
            baseline["samples"] += 1
            baseline["recovering"] = False
            control["safe"] = 1
            control["goodput"] = 1 / timings["duration"]
            control["probe"] = False
            control["limit"] = min(2, self.limits[lane])
            if not state["lanes"][lane]["budget_trial"]:
                state["lanes"][lane]["capacity"] = max(
                    state["lanes"][lane]["capacity"], control["limit"]
                )
            control["epoch"] += 1
            control["started"] = now
            control["durations"] = []
            control["peak"] = 0
            return True
        # Rolling windows count every valid completion, including work admitted
        # before a capacity change. Dropping those completions while charging
        # their occupied time would falsely report a throughput regression.
        control["durations"].append(timings["duration"])
        inflation = max(control["durations"]) > baseline["duration"] * 1.25
        if not inflation and control["peak"] < control["limit"]:
            # The global budget (including old leases) can prevent the trial
            # allocation. Serial completions are not evidence concurrency hurts.
            if len(control["durations"]) >= control["limit"]:
                control["durations"] = []
            return False
        if len(control["durations"]) < control["limit"] and not inflation:
            return False
        goodput = len(control["durations"]) / max(0.001, now - control["started"])
        trial = control["limit"] > control["safe"]
        improved = goodput > control["goodput"]
        if inflation or (trial and not improved):
            control["limit"] = control["safe"]
            control["hold_rounds"] = min(8, control["hold_rounds"] * 2)
            control["hold"] = control["hold_rounds"]
        else:
            control["safe"] = control["limit"]
            control["goodput"] = goodput
            if trial and control["limit"] > self.initial[lane]:
                control["hold"] = max(control["hold"], 1)
            if control["hold"]:
                control["hold"] -= 1
            else:
                next_limit = (
                    min(control["limit"] * 2, self.initial[lane])
                    if control["limit"] < self.initial[lane]
                    else control["limit"] + 1
                )
                control["limit"] = min(self.limits[lane], next_limit)
        control["epoch"] += 1
        control["started"] = now
        control["durations"] = []
        control["peak"] = 0
        if not state["lanes"][lane]["budget_trial"]:
            state["lanes"][lane]["capacity"] = max(
                state["lanes"][lane]["capacity"], control["limit"]
            )
        return False

    def _budget_complete(self, state, record, outcome, timings, now):
        lane = _lane(record["class"])
        value = state["lanes"][lane]
        baseline = state["baselines"][record["class"]]
        if (
            record["pressure_seen"]
            or record["failure_seen"]
            or record["generation"] != value["generation"]
        ):
            return
        healthy = outcome == "success" and all(
            timings[k] <= baseline[k] * 1.25 for k in timings
        )
        if not healthy:
            value["successes"] = 0
            value["budget_peak"] = 0
            if value["budget_trial"] and outcome not in {"cancelled", "neutral"}:
                self._pressure(
                    state, lane, record["generation"], now, target=value["budget_base"]
                )
            return
        value["successes"] += 1
        if (
            value["successes"] < value["capacity"]
            or value["budget_peak"] < value["capacity"]
        ):
            return
        # A full, healthy window confirms at most one shared-budget experiment.
        value["successes"] = 0
        value["budget_peak"] = 0
        if value["budget_trial"]:
            value["budget_trial"] = False
            value["budget_base"] = value["capacity"]
            return
        if (
            state["recovering"]
            or value["recovering"]
            or now < max(state["retry_until"], value["hold_until"])
            or any(
                r["pressure_seen"]
                for r in state["active"].values()
                if _lane(r["class"]) == lane
            )
        ):
            return
        counts = {}
        for peer in state["active"].values():
            if _lane(peer["class"]) == lane:
                counts[peer["class"]] = counts.get(peer["class"], 0) + 1
        demanded = {
            r["class"]
            for r in state["pending"].values()
            if r["lane"] == lane
            and not state["baselines"][r["class"]]["recovering"]
            and counts.get(r["class"], 0)
            < state["baselines"][r["class"]]["control"]["limit"]
        }
        if (
            sum(counts.values())
            + sum(
                state["baselines"][name]["control"]["limit"] - counts.get(name, 0)
                for name in demanded
            )
            > value["capacity"]
            and value["capacity"] < self.limits[lane]
        ):
            value["budget_base"] = value["capacity"]
            value["capacity"] += 1
            value["budget_trial"] = True

    def _reset_drained(self, state, lane, request_class):
        if not any(
            r["class"] == request_class for r in state["active"].values()
        ) and not any(r["class"] == request_class for r in state["pending"].values()):
            baseline = state["baselines"][request_class]
            epoch = baseline["control"]["epoch"] + 1
            baseline["control"] = self._control()
            baseline["control"]["epoch"] = epoch
        if not any(
            _lane(r["class"]) == lane for r in state["active"].values()
        ) and not any(r["lane"] == lane for r in state["pending"].values()):
            value = state["lanes"][lane]
            value["capacity"] = min(value["capacity"], self.initial[lane])
            value.update(
                budget_trial=False,
                budget_base=value["capacity"],
                budget_peak=0,
                successes=0,
            )

    def finish(self, lease, outcome="success", *, transport_closed=True):
        if (
            outcome not in {"success", "failure", "cancelled", "neutral"}
            or type(transport_closed) is not bool
        ):
            raise ValueError("invalid completion")
        with self._transaction() as state:
            record = self._record(state, lease)
            if not transport_closed:
                if not record["faulted"]:
                    state["outcomes"]["faulted"] += 1
                record["faulted"] = True
                self._retained[lease.token] = lease
                return
            if record["faulted"]:
                raise ValueError(
                    "faulted transport permit is retained until process exit"
                )
            now = self.clock()
            lane = _lane(record["class"])
            value = state["lanes"][lane]
            baseline = state["baselines"][record["class"]]
            duration = max(0.001, now - record["started"])
            first = max(0.001, (record["first_progress"] or now) - record["started"])
            gap = max(
                0.001,
                record["max_gap"],
                now - (record["progress"] or record["started"]),
            )
            state["outcomes"][outcome] += 1
            timings = {"duration": duration, "header": first, "gap": gap}
            healthy = all(timings[key] <= baseline[key] * 1.75 for key in timings)
            if (
                outcome == "success"
                and not healthy
                and not self._acknowledged_latency(state, record)
            ):
                baseline["recovering"] = True
            recalibrated = self._cohort_complete(state, record, outcome, timings, now)
            if recalibrated:
                healthy = True
            if outcome == "failure":
                baseline["recovering"] = True
                self._pressure(
                    state, lane, record["generation"], now, hold=0.25, recover=True
                )
            elif outcome == "success" and not record["failure_seen"]:
                eligible = recalibrated or (
                    record["generation"] == value["generation"]
                    and not record["pressure_seen"]
                )
                if healthy and eligible:
                    if record["probe"] and now >= state["retry_until"]:
                        state["recovering"] = False
                        value["recovering"] = False
                    if record["class_probe"] or recalibrated:
                        baseline["recovering"] = False
                    # Cost baselines are learned by bounded completed probes,
                    # never by an inflated/rejected concurrency experiment.
                elif not healthy:
                    self._pressure(state, lane, record["generation"], now)
            self._budget_complete(state, record, outcome, timings, now)
            del state["active"][lease.slot]
            self._reset_drained(state, lane, record["class"])
        os.close(lease.fd)
        lease.finished = True

    def snapshot(self):
        with self._transaction() as state:
            now = self.clock()
            for lane in self.limits:
                self._current_pressure(state, lane, now)
            lanes = {}
            for lane, value in state["lanes"].items():
                lanes[lane] = dict(
                    value,
                    active=sum(
                        _lane(r["class"]) == lane for r in state["active"].values()
                    ),
                    queued=sum(r["lane"] == lane for r in state["pending"].values()),
                )
            return {
                "version": state["version"],
                "requests": state["requests"],
                "retry_until": state["retry_until"],
                "faulted": any(r["faulted"] for r in state["active"].values()),
                "recovering": state["recovering"],
                "outcomes": state["outcomes"],
                "lanes": lanes,
                "baselines": state["baselines"],
            }

    def read(self):
        return self.snapshot()

    def retry_after(self):
        state = self.snapshot()
        return max(
            1,
            math.ceil(
                max(
                    state["retry_until"],
                    *(v["hold_until"] for v in state["lanes"].values()),
                )
                - self.clock()
            ),
        )
