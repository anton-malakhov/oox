#!/usr/bin/env python3
"""Run the complete native OOX scheduler evaluation without network access."""

import argparse
import datetime
import json
import os
from pathlib import Path
import platform
import socket
import subprocess
import sys


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
    parser.add_argument("--no-plot", action="store_true")
    parser.add_argument("--timeout", type=int, default=600,
                        help="maximum seconds for each subprocess")
    return parser.parse_args()


def run_json(command, output, env, timeout):
    print("+", " ".join(map(str, command)), flush=True)
    with output.open("w") as stream:
        subprocess.run(command, env=env, stdout=stream, stderr=subprocess.STDOUT,
                       check=True, timeout=timeout)
    json.loads(output.read_text())


def main():
    root = Path(__file__).resolve().parents[2]
    args = parse_args(root)
    executable_dir = args.build.resolve() / "benchmarks/scheduler_eval"
    executables = sorted(executable_dir.glob("bench_scheduler_eval_*"))
    modes = args.mode or [path.name.removeprefix("bench_scheduler_eval_")
                          for path in executables if path.is_file()]
    if not modes:
        raise RuntimeError("No scheduler evaluation executables found; configure with "
                           "-DOOX_BUILD_SCHEDULER_EVALS=ON")
    timestamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output = (args.output or root / "results/scheduler_eval" /
              f"{timestamp}_{socket.gethostname()}").resolve()
    raw = output / "raw"
    traces = output / "traces"
    raw.mkdir(parents=True, exist_ok=True)
    traces.mkdir(parents=True, exist_ok=True)
    cache = cmake_cache(args.build.resolve())
    compiler = cache.get("CMAKE_CXX_COMPILER", "unavailable")
    try:
        compiler_version = capture([compiler, "--version"], root).splitlines()[0]
    except (OSError, subprocess.CalledProcessError):
        compiler_version = "unavailable"
    benchmark_filter = "Launch/64" if args.smoke else args.benchmark_filter
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
        "modes": modes,
        "oox_commit": revision(root),
        "thesis_commit": revision(root, "thirdparty/composable-parallel-scheduler-thesis"),
        "pbbs_commit": revision(root, "thirdparty/pbbsbench"),
        "build_directory": str(args.build.resolve()),
        "build_type": cache.get("CMAKE_BUILD_TYPE", "unspecified"),
        "compiler": compiler,
        "compiler_version": compiler_version,
        "cxx_flags": cache.get("CMAKE_CXX_FLAGS", ""),
        "allocator": cache.get("OOX_ALLOCATOR", "unspecified"),
        "environment": {
            "KMP_AFFINITY": os.environ.get("KMP_AFFINITY", "unspecified"),
            "OMP_PROC_BIND": os.environ.get("OMP_PROC_BIND", "unspecified"),
            "OMP_PLACES": os.environ.get("OMP_PLACES", "unspecified"),
        },
    }
    (output / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
    env = os.environ.copy()
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
        subprocess.run(command, env=env, check=True, timeout=args.timeout)
        json.loads((raw / f"bench_scheduler_eval_{mode}.json").read_text())
        scenarios = ["spin"] if args.smoke else ["spin", "barrier", "multitask"]
        if not args.smoke and mode == "EIGEN_STEALING_GRAINSIZE":
            scenarios.remove("barrier")
        for scenario in scenarios:
            run_json([str(executable_dir / f"scheduling_dist_{mode}"),
                      "--scenario", scenario, "--iterations",
                      "1" if args.smoke else "10"],
                     raw / f"scheduling_dist_{scenario}_{mode}.json", env,
                     args.timeout)
        run_json([str(executable_dir / f"trace_spin_{mode}"), "--tasks",
                  "8" if args.smoke else str(args.threads * 4), "--work",
                  "8" if args.smoke else "10000", "--iterations",
                  "2" if args.smoke else "1024"],
                 traces / f"trace_spin_{mode}.json", env, args.timeout)
    tuner = executable_dir / "timespan_tuner_EIGEN_STEALING"
    if tuner.exists() and "EIGEN_STEALING" in modes:
        run_json([str(tuner), "--iterations", "2" if args.smoke else "10000"],
                 raw / "timespan_tuner_EIGEN_STEALING.json", env, args.timeout)
    if not args.no_plot:
        subprocess.run([sys.executable, str(Path(__file__).with_name("plot.py")),
                        str(output)], check=True, timeout=args.timeout)
    metadata["complete"] = True
    (output / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
    print(output)


if __name__ == "__main__":
    main()
