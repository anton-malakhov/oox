#!/usr/bin/env python3
"""Run the complete native OOX scheduler evaluation without network access."""

import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import socket
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent / "tools"))

import hardware
import datasets
import provenance


def capture(command, cwd):
    return subprocess.check_output(command, cwd=cwd, text=True,
                                   stderr=subprocess.DEVNULL).strip()


def revision(root, path="."):
    try:
        return capture(["git", "-C", path, "rev-parse", "HEAD"], root)
    except subprocess.CalledProcessError:
        return "unavailable"


def cmake_cache(build):
    values = {}
    path = build / "CMakeCache.txt"
    if not path.exists():
        return values
    for line in path.read_text(errors="replace").splitlines():
        if not line or line.startswith(("#", "//")) or "=" not in line:
            continue
        key_and_type, value = line.split("=", 1)
        key = key_and_type.split(":", 1)[0]
        values[key] = value
    return values


def parse_args(root):
    parser = argparse.ArgumentParser()
    parser.add_argument("--build", type=Path, default=root / "cmake-build-release")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--mode", action="append")
    parser.add_argument("--threads", type=int, default=os.cpu_count() or 1)
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--benchmark-min-time", default="0.5s")
    parser.add_argument("--filter", dest="benchmark_filter", default="")
    parser.add_argument("--smoke", action="store_true")
    graph_input = parser.add_mutually_exclusive_group()
    graph_input.add_argument("--graph", type=Path,
                        help="PBBS AdjacencyGraph file for BfsFile cases")
    graph_input.add_argument("--dataset", choices=[name for name, entry in datasets.CATALOG["datasets"].items() if entry["format"] == "pasl-binary"])
    parser.add_argument("--dataset-directory", type=Path, default=root / "results/original-datasets")
    parser.add_argument("--source-vertex", type=int)
    parser.add_argument("--paper-scale", action="store_true",
                        help="also register 100-million-element primary cases")
    parser.add_argument("--no-plot", action="store_true")
    parser.add_argument("--timeout", type=int, default=600,
                        help="maximum seconds for each subprocess")
    parser.add_argument("--cpu-node", type=int)
    parser.add_argument("--memory-node", type=int)
    parser.add_argument("--interleave-memory", action="store_true")
    parser.add_argument("--perf", action="store_true")
    parser.add_argument("--papi-events", help="comma-separated PAPI integer events; requires a PAPI-enabled build")
    parser.add_argument("--likwid-group", help="LIKWID event group, e.g. MEM")
    parser.add_argument("--likwid-cpus", help="explicit CPU IDs, e.g. 0-3")
    parser.add_argument("--perf-events",
                        default="cycles,instructions,cache-misses,cpu-migrations")
    return parser.parse_args()


def placement_prefix(args):
    requested = (args.cpu_node is not None or args.memory_node is not None or
                 args.interleave_memory)
    if not requested:
        return []
    if args.interleave_memory and args.memory_node is not None:
        raise ValueError("--interleave-memory and --memory-node are exclusive")
    if platform.system() != "Linux" or not shutil.which("numactl"):
        raise RuntimeError("NUMA placement requires numactl on Linux")
    command = ["numactl"]
    if args.cpu_node is not None:
        command.append(f"--cpunodebind={args.cpu_node}")
    if args.memory_node is not None:
        command.append(f"--membind={args.memory_node}")
    if args.interleave_memory:
        command.append("--interleave=all")
    return command


def run_json(command, output, env, timeout):
    print("+", " ".join(map(str, command)), flush=True)
    with output.open("w") as stream:
        subprocess.run(command, env=env, stdout=stream, stderr=subprocess.STDOUT,
                       check=True, timeout=timeout)
    json.loads(output.read_text())


def write_heartbeat_comparison(raw, output, modes):
    units = {"ns": 1e-9, "us": 1e-6, "ms": 1e-3, "s": 1.0}
    records = []
    for mode in modes:
        data = json.loads((raw / f"bench_scheduler_eval_{mode}.json").read_text())
        if not data["benchmarks"]:
            raise RuntimeError(f"no benchmarks matched for {mode}")
        for case in data["benchmarks"]:
            if case.get("run_type", "iteration") != "iteration":
                continue
            if case.get("error_occurred"):
                raise RuntimeError(f"benchmark failed: {mode}/{case['name']}")
            iterations = case.get("iterations", 1)
            def per_iteration(name):
                return case[name] / iterations if name in case else None
            records.append({
                "mode": mode,
                "benchmark": case["name"],
                "exectime": case["real_time"] * units[case["time_unit"]],
                "nb_steals": per_iteration("successful_steals"),
                "nb_promotions": per_iteration("nested_launches"),
                "tasks": per_iteration("tasks_scheduled"),
                "observed_sleep_ns": per_iteration("idle_time_ns"),
                "utilization": None,
                "papi_counts_per_iteration": {
                    key.removeprefix("papi_"): value / iterations for key, value in case.items()
                    if key.startswith("papi_") and key != "papi_callback_regions"},
            })
    (output / "heartbeat_compat.json").write_text(json.dumps({
        "schema": 1,
        "note": "exectime is seconds per iteration; null means unmeasured. "
                "nb_promotions denotes BFS inner-loop launches where available. "
                "Sleeping time alone is insufficient to infer utilization.",
        "records": records,
    }, indent=2) + "\n")


def main():
    root = Path(__file__).resolve().parents[2]
    args = parse_args(root)
    dataset_record = None
    if args.dataset:
        entry = datasets.CATALOG["datasets"][args.dataset]
        args.graph = args.dataset_directory / entry["file"]
        record_path = args.dataset_directory / (entry["file"] + ".metadata.json")
        dataset_record = json.loads(record_path.read_text())
        if not dataset_record.get("complete") or dataset_record.get("identity") != entry:
            raise ValueError("original dataset metadata does not match the catalogue")
        if args.source_vertex is None:
            args.source_vertex = entry["source_vertex"]
    if args.source_vertex is None:
        args.source_vertex = 0
    if args.papi_events and cmake_cache(args.build.resolve()).get("OOX_SCHEDULER_EVAL_PAPI") not in ("ON", "TRUE", "1"):
        raise RuntimeError("--papi-events requires -DOOX_SCHEDULER_EVAL_PAPI=ON")
    if args.source_vertex < 0 or args.source_vertex > 0xffffffff:
        raise ValueError("source-vertex must be a 32-bit unsigned vertex ID")
    if args.source_vertex and not args.graph:
        raise ValueError("source-vertex applies to --graph file inputs")
    if args.graph and not args.graph.is_file():
        raise ValueError(f"graph file does not exist: {args.graph}")
    placement = placement_prefix(args)
    hardware.validate_options(args)
    executable_dir = args.build.resolve() / "benchmarks/scheduler_eval"
    executables = sorted(executable_dir.glob("bench_scheduler_eval_*"))
    modes = args.mode or [path.name.removeprefix("bench_scheduler_eval_")
                          for path in executables if path.is_file()]
    if not modes:
        raise RuntimeError("No scheduler evaluation executables found; configure with "
                           "-DOOX_BUILD_SCHEDULER_EVALS=ON")
    if "OOX_TASKS" in modes:
        fixed_threads = int(cmake_cache(args.build.resolve()).get("OOX_EIGEN_THREADS", "0"))
        actual_threads = fixed_threads or (os.cpu_count() or 1)
        if actual_threads != args.threads:
            raise ValueError(
                f"OOX_TASKS has {actual_threads} workers; configure "
                f"-DOOX_EIGEN_THREADS={args.threads} for this comparison")
    timestamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output = (args.output or root / "results/scheduler_eval" /
              f"{timestamp}_{socket.gethostname()}").resolve()
    raw = output / "raw"
    traces = output / "traces"
    raw.mkdir(parents=True, exist_ok=True)
    traces.mkdir(parents=True, exist_ok=True)
    cache = cmake_cache(args.build.resolve())
    measured_paths = [executable_dir / f"{prefix}_{mode}"
                      for mode in modes
                      for prefix in ("bench_scheduler_eval", "scheduling_dist", "trace_spin")]
    tuner_path = executable_dir / "timespan_tuner_EIGEN_STEALING"
    if "EIGEN_STEALING" in modes and tuner_path.exists():
        measured_paths.append(tuner_path)
    binary_records = provenance.artifacts(measured_paths)
    env = provenance.execution_environment(os.environ)
    compiler = cache.get("CMAKE_CXX_COMPILER", "unavailable")
    try:
        compiler_version = capture([compiler, "--version"], root).splitlines()[0]
    except (OSError, subprocess.CalledProcessError):
        compiler_version = "unavailable"
    benchmark_filter = ((args.benchmark_filter or "Launch/64") if args.smoke
                        else args.benchmark_filter)
    benchmark_min_time = "0.01s" if args.smoke else args.benchmark_min_time
    metadata = {
        "schema": 1,
        "complete": False,
        "timestamp_utc": timestamp,
        "host": socket.gethostname(),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "python": platform.python_version(),
        "threads": args.threads,
        "repetitions": args.repetitions,
        "benchmark_filter": benchmark_filter,
        "benchmark_min_time": benchmark_min_time,
        "subprocess_timeout_seconds": args.timeout,
        "smoke": args.smoke,
        "graph_file": str(args.graph.resolve()) if args.graph else None,
        "source_vertex": args.source_vertex,
        "original_dataset": args.dataset,
        "paper_scale": args.paper_scale,
        "modes": modes,
        "execution_threads_by_mode": {
            mode: 1 if mode == "SERIAL_ELISION" else args.threads for mode in modes},
        "oox_commit": revision(root),
        "checkout": provenance.checkout(root),
        "binary_source_revision": None,
        "executables": binary_records,
        "placement_policy": "shared inherited CPU set; no per-worker pinning; optional --cpu-node restriction",
        "pbbs_snapshot_revision": json.loads((root / "benchmarks/pbbs/vendor_manifest.json").read_text())["revision"],
        "pbbs_snapshot_manifest_sha256": provenance.sha256(root / "benchmarks/pbbs/vendor_manifest.json"),
        "build_directory": str(args.build.resolve()),
        "build_type": cache.get("CMAKE_BUILD_TYPE", "unspecified"),
        "compiler": compiler,
        "compiler_version": compiler_version,
        "cxx_flags": cache.get("CMAKE_CXX_FLAGS", ""),
        "allocator": cache.get("OOX_ALLOCATOR", "unspecified"),
        "environment": {
            "KMP_AFFINITY": env["KMP_AFFINITY"],
            "OMP_PROC_BIND": env["OMP_PROC_BIND"],
            "OMP_PLACES": env.get("OMP_PLACES", "unspecified"),
        },
        "numa_command": placement,
        "perf_events": args.perf_events.split(",") if args.perf else [],
        "hardware_collection": hardware.metadata(args),
    }
    if args.graph:
        digest = hashlib.sha256()
        with args.graph.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
        metadata["graph_sha256"] = digest.hexdigest()
        if dataset_record and metadata["graph_sha256"] != dataset_record["sha256"]:
            raise ValueError("original dataset payload checksum mismatch")
    (output / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
    if args.papi_events:
        env["OOX_EVAL_PAPI_EVENTS"] = args.papi_events
    else:
        env.pop("OOX_EVAL_PAPI_EVENTS", None)
    if args.graph:
        env["OOX_BENCH_GRAPH"] = str(args.graph.resolve())
        env["OOX_BENCH_SOURCE"] = str(args.source_vertex)
    if args.paper_scale:
        env["OOX_BENCH_PAPER_SCALE"] = "1"
    env["BENCH_NUM_THREADS"] = str(args.threads)
    env["OMP_NUM_THREADS"] = str(args.threads)
    env["PARLAY_NUM_THREADS"] = str(args.threads)
    for mode in modes:
        benchmark = executable_dir / f"bench_scheduler_eval_{mode}"
        if not benchmark.exists():
            raise RuntimeError(f"Missing executable for mode {mode}: {benchmark}")
        command = [str(benchmark), "--benchmark_out_format=json",
                   f"--benchmark_out={raw / f'bench_scheduler_eval_{mode}.json'}",
                   f"--benchmark_repetitions={args.repetitions}"]
        command.append(f"--benchmark_min_time={benchmark_min_time}")
        if benchmark_filter:
            command.append(f"--benchmark_filter={benchmark_filter}")
        command = placement + command
        counter_tool = "perf" if args.perf else "likwid"
        counter_output = raw / f"{counter_tool}_{mode}.csv"
        command = hardware.counter_prefix(args, counter_output) + command
        subprocess.run(command, env=env, check=True, timeout=args.timeout)
        if (args.perf or args.likwid_group) and (
                not counter_output.is_file() or not counter_output.stat().st_size):
            raise RuntimeError(f"counter collection produced no output: {counter_output}")
        json.loads((raw / f"bench_scheduler_eval_{mode}.json").read_text())
        scenarios = ["spin"] if args.smoke else ["spin", "barrier", "multitask"]
        if not args.smoke and mode == "EIGEN_STEALING_GRAINSIZE":
            scenarios.remove("barrier")
        for scenario in scenarios:
            run_json(placement + [str(executable_dir / f"scheduling_dist_{mode}"),
                      "--scenario", scenario, "--iterations",
                      "1" if args.smoke else "10"],
                     raw / f"scheduling_dist_{scenario}_{mode}.json", env,
                     args.timeout)
        run_json(placement + [str(executable_dir / f"trace_spin_{mode}"), "--tasks",
                  "8" if args.smoke else str(args.threads * 4), "--work",
                  "8" if args.smoke else "10000", "--iterations",
                  "2" if args.smoke else "1024"],
                 traces / f"trace_spin_{mode}.json", env, args.timeout)
    tuner = executable_dir / "timespan_tuner_EIGEN_STEALING"
    if tuner.exists() and "EIGEN_STEALING" in modes:
        run_json(placement + [str(tuner), "--iterations",
                  "2" if args.smoke else "10000"],
                 raw / "timespan_tuner_EIGEN_STEALING.json", env, args.timeout)
    write_heartbeat_comparison(raw, output, modes)
    if provenance.artifacts(measured_paths) != binary_records:
        raise RuntimeError("benchmark executables changed during the run")
    if not args.no_plot:
        subprocess.run([sys.executable, str(Path(__file__).parent / "tools" / "plot.py"),
                        str(output)], check=True, timeout=args.timeout)
    metadata["complete"] = True
    (output / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
    print(output)


if __name__ == "__main__":
    main()
