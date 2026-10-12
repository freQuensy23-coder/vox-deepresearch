"""Loopback-only HTTP comparison. Synthetic evidence, never a Vox benchmark.

The original relay is supplied as an immutable source snapshot. All upstreams
bind 127.0.0.1; credentials are a fake fixture string. No production calls.
"""

import argparse
import concurrent.futures
import contextlib
import hashlib
import http.client
import importlib.util
import json
import math
from pathlib import Path
import statistics
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlencode


PHASES = [
    dict(name="healthy", searches=48, capacity=12, base_seconds=0.15),
    dict(name="congested", searches=36, capacity=4, base_seconds=0.35),
    dict(name="recovery", searches=48, capacity=12, base_seconds=0.15),
]
SPEC = dict(
    phases=PHASES,
    workers=12,
    metadata_every=8,
    metadata_seconds=0.03,
    excess_latency_seconds=0.2,
    formula="base + 0.2 * max(0, active_searches-capacity)**2",
    errors="none injected",
    stream="progress headers immediately, terminal only after service delay",
    timing="synthetic subsecond service times; relay default timing unless explicitly reported",
    trials=3,
    orders=[
        ["fixed", "adaptive", "fixed_matched_spacing"],
        ["adaptive", "fixed_matched_spacing", "fixed"],
        ["fixed_matched_spacing", "fixed", "adaptive"],
    ],
)


def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def percentile(values, p):
    if not values:
        return None
    values = sorted(values)
    return values[max(0, math.ceil(p * len(values)) - 1)]


def distribution(values):
    return dict(
        count=len(values),
        p50=percentile(values, 0.5),
        p95=percentile(values, 0.95),
        maximum=max(values) if values else None,
    )


class Fixture(ThreadingHTTPServer):
    daemon_threads = False
    request_queue_size = 64

    def __init__(self):
        self.lock = threading.Lock()
        self.active = self.peak = 0
        self.rows = []
        self.targets = {}
        super().__init__(("127.0.0.1", 0), FixtureHandler)


class FixtureHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def do_GET(self):
        job = self.server.targets[self.path]
        phase = PHASES[job["phase_index"]]
        search = job["kind"] == "search"
        started = time.monotonic()
        with self.server.lock:
            self.server.active += int(search)
            active = self.server.active
            self.server.peak = max(self.server.peak, active)
        delay = (
            (
                phase["base_seconds"]
                + SPEC["excess_latency_seconds"]
                * max(0, active - phase["capacity"]) ** 2
            )
            if search
            else SPEC["metadata_seconds"]
        )
        try:
            self.send_response(200)
            self.send_header(
                "Content-Type", "application/x-ndjson" if search else "application/json"
            )
            self.send_header("Connection", "close")
            self.end_headers()
            if search:
                self.wfile.write(b'{"status":"progress"}\n')
                self.wfile.flush()
            time.sleep(delay)
            payload = (
                dict(
                    messages=[dict(id=job["id"], text="fixture evidence")],
                    count=1,
                    stop_reason="finished",
                )
                if search
                else dict(id=-job["id"], is_private=False, type="CHAT", alias="fixture")
            )
            self.wfile.write(json.dumps(payload).encode() + b"\n")
            self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            pass
        finally:
            with self.server.lock:
                self.server.active -= int(search)
                self.server.rows.append(
                    dict(
                        id=job["id"],
                        kind=job["kind"],
                        phase=phase["name"],
                        started=started,
                        finished=time.monotonic(),
                        delay=delay,
                        active_at_start=active,
                    )
                )
            self.close_connection = True


def transport_factory(address):
    class LoopbackUpstream:
        def __init__(self, timeout):
            self.connection = http.client.HTTPConnection(*address, timeout=timeout)

        def request(self, method, target, body, headers):
            self.connection.request(method, target, body=body, headers=headers)
            self.socket = self.connection.sock
            return self.connection.getresponse()

        def abort(self):
            sock = getattr(self, "socket", None)
            if sock:
                try:
                    sock.shutdown(2)
                except OSError:
                    pass

        def close(self):
            self.abort()
            self.connection.close()

    return LoopbackUpstream


def client(address, job):
    started = time.monotonic()
    conn = http.client.HTTPConnection(*address, timeout=180)
    row = dict(
        id=job["id"],
        kind=job["kind"],
        phase=PHASES[job["phase_index"]]["name"],
        started=started,
        submitted=job.get("submitted", started),
    )
    try:
        conn.request("GET", job["target"])
        response = conn.getresponse()
        raw = response.read()
        frames = [json.loads(line) for line in raw.splitlines() if line]
        terminal = frames[-1] if frames else {}
        success = response.status == 200 and (
            (
                terminal.get("stop_reason") == "finished"
                and len(terminal.get("messages", [])) == 1
                and terminal["messages"][0].get("id") == job["id"]
            )
            if job["kind"] == "search"
            else terminal.get("id") == -job["id"]
        )
        row.update(status=response.status, semantic_complete=success, bytes=len(raw))
    except Exception as exc:
        row.update(
            status=None,
            semantic_complete=False,
            error=type(exc).__name__ + ": " + str(exc),
        )
    finally:
        conn.close()
    row["finished"] = time.monotonic()
    row["elapsed"] = row["finished"] - started
    row["frontend_wait_seconds"] = started - row["submitted"]
    row["submitted_to_finish_seconds"] = row["finished"] - row["submitted"]
    return row


def workload():
    phases = []
    job_id = 1
    for phase_index, phase in enumerate(PHASES):
        jobs = []
        for index in range(phase["searches"]):
            kinds = (
                ["search", "metadata"]
                if index % SPEC["metadata_every"] == 0
                else ["search"]
            )
            for kind in kinds:
                target = (
                    "/search/text?"
                    + urlencode(
                        dict(query=f"fixture{job_id}", limit=200, stream="true")
                    )
                    if kind == "search"
                    else f"/groups/-{job_id}"
                )
                jobs.append(
                    dict(id=job_id, kind=kind, phase_index=phase_index, target=target)
                )
                job_id += 1
        phases.append(jobs)
    return phases


def run_one(module, variant, output):
    output.mkdir(parents=True)
    key = output / "fake-key"
    key.write_text("local-benchmark-not-a-real-key", encoding="utf-8")
    upstream = Fixture()
    jobs_by_phase = workload()
    upstream.targets = {job["target"]: job for jobs in jobs_by_phase for job in jobs}
    options = dict(spacing=0.001) if variant == "fixed_matched_spacing" else {}
    server_class = (
        module.AdaptiveRelayServer if variant == "adaptive" else module.RelayServer
    )
    relay = server_class(
        ("127.0.0.1", 0),
        key_file=key,
        state_dir=output / "state",
        transport_factory=transport_factory(upstream.server_address),
        **options,
    )
    threads = [
        threading.Thread(target=server.serve_forever) for server in (upstream, relay)
    ]
    for thread in threads:
        thread.start()
    done = threading.Event()
    snapshots = []

    def sample():
        while not done.wait(0.05):
            try:
                snapshots.append(
                    dict(
                        time=time.monotonic(),
                        state=json.loads(
                            (output / "state" / "state.json").read_text(
                                encoding="utf-8"
                            )
                        ),
                    )
                )
            except Exception as exc:
                snapshots.append(dict(time=time.monotonic(), error=str(exc)))

    sampler = threading.Thread(target=sample)
    sampler.start()
    rows, phases = [], []
    started = time.monotonic()
    try:
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=SPEC["workers"]
        ) as executor:
            for jobs in jobs_by_phase:
                phase_start = time.monotonic()
                futures = [
                    executor.submit(
                        client,
                        relay.server_address,
                        {**job, "submitted": time.monotonic()},
                    )
                    for job in jobs
                ]
                phase_rows = [future.result() for future in futures]
                rows.extend(phase_rows)
                phases.append(
                    dict(
                        name=phase_rows[0]["phase"],
                        started=phase_start,
                        finished=time.monotonic(),
                        count=len(phase_rows),
                    )
                )
    finally:
        finished = time.monotonic()
        done.set()
        sampler.join()
        for server in (relay, upstream):
            server.shutdown()
            server.server_close()
        for thread in threads:
            thread.join()
    upstream_by_id = {row["id"]: row for row in upstream.rows}
    for row in rows:
        if row["id"] in upstream_by_id:
            observed = upstream_by_id[row["id"]]
            row["queue_seconds"] = observed["started"] - row["started"]
            row["service_seconds"] = observed["finished"] - observed["started"]
    metrics = {}
    for scope in ["all", *[phase["name"] for phase in PHASES], "metadata"]:
        selected = [
            row
            for row in rows
            if scope == "all"
            or row["phase"] == scope
            or scope == "metadata"
            and row["kind"] == "metadata"
        ]
        metrics[scope] = dict(
            requests=len(selected),
            successful=sum(row["semantic_complete"] for row in selected),
            latency_seconds=distribution([row["elapsed"] for row in selected]),
            frontend_wait_seconds=distribution(
                [row["frontend_wait_seconds"] for row in selected]
            ),
            submitted_to_finish_seconds=distribution(
                [row["submitted_to_finish_seconds"] for row in selected]
            ),
            queue_seconds=distribution(
                [row["queue_seconds"] for row in selected if "queue_seconds" in row]
            ),
            service_seconds=distribution(
                [row["service_seconds"] for row in selected if "service_seconds" in row]
            ),
        )
    result = dict(
        variant=variant,
        options=options,
        relay_defaults=module.DEFAULTS,
        started=started,
        finished=finished,
        wall_seconds=finished - started,
        throughput=len(rows) / (finished - started),
        successful_throughput=sum(row["semantic_complete"] for row in rows)
        / (finished - started),
        peak_upstream_searches=upstream.peak,
        metrics=metrics,
        phases=phases,
        clients=rows,
        upstream=upstream.rows,
        controller_snapshots=snapshots,
    )
    (output / "result.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument(
        "--candidate",
        type=Path,
        default=Path(__file__).with_name("vox_adaptive_relay.py"),
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--trials", type=int, choices=[1, 3], default=3)
    parser.add_argument(
        "--variants",
        nargs="+",
        choices=["fixed", "adaptive", "fixed_matched_spacing"],
        default=["fixed", "adaptive", "fixed_matched_spacing"],
    )
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    sys.path.insert(0, str(args.candidate.resolve().parent))
    source_paths = [
        args.baseline,
        args.candidate,
        args.candidate.with_name("adaptive_gate.py"),
        args.candidate.with_name("vox_relay.py"),
        Path(__file__),
    ]
    probe_source = args.candidate.with_name("vox_json_probe.py")
    if probe_source.exists():
        source_paths.append(probe_source)

    def hashes():
        return {
            str(path): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in source_paths
        }

    start_hashes = hashes()
    snapshot = args.output / "code"
    snapshot.mkdir()
    for path in source_paths:
        data = path.read_bytes()
        if hashlib.sha256(data).hexdigest() != start_hashes[str(path)]:
            raise RuntimeError("Source changed before import")
        (
            snapshot / ("baseline.py" if path == args.baseline else path.name)
        ).write_bytes(data)
    modules = {
        "fixed": load_module("benchmark_fixed_relay", args.baseline),
        "adaptive": load_module("benchmark_adaptive_relay", args.candidate),
    }
    modules["fixed_matched_spacing"] = modules["fixed"]
    modules = {name: modules[name] for name in args.variants}
    orders = [
        [name for name in order if name in modules]
        for order in SPEC["orders"][: args.trials]
    ]
    manifest = dict(
        spec={**SPEC, "trials": args.trials, "orders": orders},
        baseline_sha256=hashlib.sha256(args.baseline.read_bytes()).hexdigest(),
        candidate_sha256=hashlib.sha256(args.candidate.read_bytes()).hexdigest(),
        source_hashes_start=start_hashes,
    )
    (args.output / "manifest.json").write_text(
        json.dumps(manifest, indent=2), encoding="utf-8"
    )
    results = []
    for trial, order in enumerate(orders, 1):
        for variant in order:
            destination = args.output / f"trial-{trial}-{variant}"
            print(f"Starting {destination.name}", flush=True)
            with (
                (args.output / (destination.name + ".log")).open(
                    "w", encoding="utf-8"
                ) as log,
                contextlib.redirect_stdout(log),
            ):
                result = run_one(modules[variant], variant, destination)
            results.append(result)
            print(
                f"Finished {destination.name}: {result['wall_seconds']:.3f}s; "
                f"{result['metrics']['all']['successful']}/{result['metrics']['all']['requests']} complete",
                flush=True,
            )
    summary = {
        variant: dict(
            wall_seconds=[
                r["wall_seconds"] for r in results if r["variant"] == variant
            ],
            median_wall_seconds=statistics.median(
                r["wall_seconds"] for r in results if r["variant"] == variant
            ),
            successful=sum(
                r["metrics"]["all"]["successful"]
                for r in results
                if r["variant"] == variant
            ),
        )
        for variant in modules
    }
    (args.output / "summary.json").write_text(
        json.dumps(summary, indent=2), encoding="utf-8"
    )
    manifest["source_hashes_end"] = hashes()
    manifest["source_unchanged"] = (
        manifest["source_hashes_start"] == manifest["source_hashes_end"]
    )
    (args.output / "manifest.json").write_text(
        json.dumps(manifest, indent=2), encoding="utf-8"
    )
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
