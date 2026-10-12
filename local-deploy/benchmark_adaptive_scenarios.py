"""Predeclared supplemental loopback workloads; never connects to Vox or SSH."""

import argparse
import contextlib
import hashlib
import json
from pathlib import Path
import sys
from urllib.parse import urlencode

import benchmark_adaptive_relay as bench


ORDERS = [
    ["fixed_matched_spacing", "adaptive"],
    ["adaptive", "fixed_matched_spacing"],
    ["fixed_matched_spacing", "adaptive"],
]


def configure(scenario):
    if scenario == "stable":
        phases = [dict(name="stable", searches=96, capacity=12, base_seconds=0.15)]
        sequence = [0] * 96
    elif scenario == "mixed":
        phases = [
            dict(name="scoped_fast", searches=96, capacity=12, base_seconds=0.15),
            dict(name="broad_slow", searches=32, capacity=12, base_seconds=1.2),
        ]
        sequence = [0, 0, 0, 1] * 32
    else:
        phases = [
            dict(name="healthy", searches=48, capacity=12, base_seconds=0.15),
            dict(name="constrained", searches=96, capacity=4, base_seconds=0.35),
            dict(name="recovery", searches=48, capacity=12, base_seconds=0.15),
        ]
        sequence = [0] * 48 + [1] * 96 + [2] * 48
    jobs, job_id = [], 1
    phase_counts = {index: 0 for index in range(len(phases))}
    for search_index, phase_index in enumerate(sequence):
        phase_counts[phase_index] += 1
        kinds = ["search", "metadata"] if search_index % 8 == 0 else ["search"]
        for kind in kinds:
            params = dict(query=f"fixture{job_id}", limit=200, stream="true")
            if scenario == "mixed" and phase_index == 0:
                params["include_groups_ids"] = "-100"
            target = (
                "/search/text?" + urlencode(params)
                if kind == "search"
                else f"/groups/-{job_id}"
            )
            jobs.append(
                dict(
                    id=job_id,
                    kind=kind,
                    phase_index=phase_index,
                    target=target,
                    search_index=search_index,
                    phase_search_index=phase_counts[phase_index],
                )
            )
            job_id += 1
    bench.PHASES = phases
    bench.SPEC = dict(
        phases=phases,
        workers=12,
        metadata_every=8,
        metadata_seconds=0.03,
        excess_latency_seconds=0.2,
        scenario=scenario,
        submission="entire ordered queue once, no phase barriers",
    )
    bench.workload = lambda: [jobs]
    return jobs


def summarize(result, jobs):
    by_id = {job["id"]: job for job in jobs}
    rows = result["clients"]
    for row in rows:
        row["search_index"] = by_id[row["id"]]["search_index"]
        row["phase_search_index"] = by_id[row["id"]]["phase_search_index"]
    windows = {}
    scopes = [phase["name"] for phase in bench.PHASES]
    if "constrained" in scopes:
        scopes += ["constrained_first_half", "constrained_second_half"]
    for scope in scopes:
        selected = [
            row
            for row in rows
            if row["kind"] == "search"
            and (
                row["phase"] == scope
                or scope == "constrained_first_half"
                and row["phase"] == "constrained"
                and row["phase_search_index"] <= 48
                or scope == "constrained_second_half"
                and row["phase"] == "constrained"
                and row["phase_search_index"] > 48
            )
        ]
        begin = min(row["started"] for row in selected)
        end = max(row["finished"] for row in selected)
        success = sum(row["semantic_complete"] for row in selected)
        windows[scope] = dict(
            requests=len(selected),
            successful=success,
            start_to_last_completion_seconds=end - begin,
            successful_throughput=success / (end - begin),
            latency_seconds=bench.distribution([r["elapsed"] for r in selected]),
            queue_seconds=bench.distribution(
                [r["queue_seconds"] for r in selected if "queue_seconds" in r]
            ),
            service_seconds=bench.distribution(
                [r["service_seconds"] for r in selected if "service_seconds" in r]
            ),
        )
    # Windows can overlap: concurrent classes and queued phases are intentional.
    return dict(
        variant=result["variant"],
        wall_seconds=result["wall_seconds"],
        successful=result["metrics"]["all"]["successful"],
        requests=result["metrics"]["all"]["requests"],
        peak_upstream_searches=result["peak_upstream_searches"],
        metrics=result["metrics"],
        windows=windows,
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--scenario", choices=["stable", "mixed", "continuous", "all"], default="all"
    )
    parser.add_argument("--trials", type=int, choices=[1, 3], default=3)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    directory = Path(__file__).resolve().parent
    sys.path.insert(0, str(directory))
    paths = [
        args.baseline,
        directory / "vox_adaptive_relay.py",
        directory / "adaptive_gate.py",
        directory / "vox_relay.py",
        directory / "benchmark_adaptive_relay.py",
        Path(__file__),
        directory / "evals" / "dynamic-admission-scenarios-2026-10-12.json",
    ]

    def hashes():
        return {
            str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in paths
        }

    manifest = dict(
        source_hashes_start=hashes(),
        trials=args.trials,
        spec=json.loads(paths[-1].read_text(encoding="utf-8")),
    )
    snapshot = args.output / "code"
    snapshot.mkdir()
    for path in paths:
        data = path.read_bytes()
        if (
            hashlib.sha256(data).hexdigest()
            != manifest["source_hashes_start"][str(path)]
        ):
            raise RuntimeError("Source changed before import")
        destination = snapshot / ("baseline.py" if path == args.baseline else path.name)
        if path.suffix == ".json":
            destination = snapshot / "evals" / path.name
            destination.parent.mkdir(exist_ok=True)
        destination.write_bytes(data)
    (args.output / "manifest.json").write_text(
        json.dumps(manifest, indent=2), encoding="utf-8"
    )
    modules = {
        "fixed_matched_spacing": bench.load_module("supplement_fixed", args.baseline),
        "adaptive": bench.load_module(
            "supplement_adaptive", directory / "vox_adaptive_relay.py"
        ),
    }
    summaries = []
    scenarios = (
        ["stable", "mixed", "continuous"] if args.scenario == "all" else [args.scenario]
    )
    for scenario in scenarios:
        jobs = configure(scenario)
        for trial, order in enumerate(ORDERS[: args.trials], 1):
            for variant in order:
                name = f"{scenario}-trial-{trial}-{variant}"
                print(f"Starting {name}", flush=True)
                with (
                    (args.output / (name + ".log")).open("w", encoding="utf-8") as log,
                    contextlib.redirect_stdout(log),
                ):
                    result = bench.run_one(
                        modules[variant], variant, args.output / name
                    )
                summary = summarize(result, jobs)
                summary.update(scenario=scenario, trial=trial)
                summaries.append(summary)
                (args.output / "summary.json").write_text(
                    json.dumps(summaries, indent=2), encoding="utf-8"
                )
                print(
                    f"Finished {name}: {result['wall_seconds']:.3f}s; "
                    f"{summary['successful']}/{summary['requests']} complete",
                    flush=True,
                )
    manifest["source_hashes_end"] = hashes()
    manifest["source_unchanged"] = (
        manifest["source_hashes_start"] == manifest["source_hashes_end"]
    )
    (args.output / "manifest.json").write_text(
        json.dumps(manifest, indent=2), encoding="utf-8"
    )
    print(f"Source unchanged: {manifest['source_unchanged']}")


if __name__ == "__main__":
    main()
