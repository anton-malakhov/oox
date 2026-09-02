#!/usr/bin/env python3
"""Run the complete native OOX scheduler evaluation without network access.

Measurement-protocol options (see PERFORMANCE_MODEL.md section 11):
  --shuffle-modes       randomize the order in which modes are executed;
  --fresh-process-repetitions N
                        instead of N in-process Google Benchmark repetitions,
                        run every mode binary N times with --benchmark_repetitions=1,
                        interleaving modes in a fresh random order per round.
                        Raw files are stored as bench_scheduler_eval_<mode>.round<k>.json
                        and the report generator sees the merged file;
  --seed                seed for the above shuffles (recorded in metadata).
Host topology (core classes, frequency policy where readable) is captured into
metadata.json so runs on heterogeneous hosts can be attributed.
"""

import argparse
import datetime
import json
import os
from pathlib import Path
import platform
import random
import shutil
import socket
import subprocess
import sys


def capture(command, cwd=None, timeout=20):
    return subprocess.check_output(command, cwd=cwd, text=True,
                                   stderr=subprocess.DEVNULL, timeout=timeout).strip()


def revision(root, path="."):
    try:
        return capture(["git", "-C", path, "rev-parse", "HEAD"], root)
    except (subprocess.CalledProcessError, OSError, subprocess.TimeoutExpired):
        return "unavailable"


def is_dirty(root, path="."):
    try:
        return bool(capture(["git", "-C", path, "status", "--porcelain"], root))
    except (subprocess.CalledProcessError, OSError, subprocess.TimeoutExpired):
        return None


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


def host_topology():
    """Best-effort core-class and frequency-policy capture, stdlib only."""
    info = {"os": platform.system(), "cpu_count": os.cpu_count()}
    try:
        if platform.system() == "Darwin":
            keys = ["hw.perflevel0.physicalcpu", "hw.perflevel0.logicalcpu",
                    "hw.perflevel0.name", "hw.perflevel1.physicalcpu",
                    "hw.perflevel1.logicalcpu", "hw.perflevel1.name",
                    "hw.nperflevels", "machdep.cpu.brand_string", "hw.memsize"]
            for key in keys:
                try:
                    info[key] = capture(["sysctl", "-n", key])
                except (subprocess.CalledProcessError, OSError):
                    pass
            info["note"] = ("macOS exposes no thread affinity API; use "
                            "`powermetrics --samplers cpu_power` externally to "
                            "attribute time to P/E clusters.")
        elif platform.system() == "Linux":
            try:
                info["lscpu"] = capture(["lscpu"]).splitlines()
            except (subprocess.CalledProcessError, OSError):
                pass
            governors = set()
            for path in Path("/sys/devices/system/cpu").glob("cpu[0-9]*/cpufreq/scaling_governor"):
                try:
                    governors.add(path.read_text().strip())
                except OSError:
                    pass
            info["cpufreq_governors"] = sorted(governors)
            try:
                info["sched_affinity"] = sorted(os.sched_getaffinity(0))
            except (AttributeError, OSError):
                pass
            try:
                info["taskset_available"] = shutil.which("taskset") is not None
                info["numactl_available"] = shutil.which("numactl") is not None
            except OSError:
                pass
    except Exception as error:  # noqa: BLE001 - diagnostics must not abort a run
        info["error"] = repr(error)
    return info


def parse_args(root):
    parser = argparse.ArgumentParser()
    parser.add_argument("--build", type=Path, default=root / "cmake-build-release")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--mode", action="append")
    parser.add_argument("--threads", type=int, default=os.cpu_count() or 1)
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--fresh-process-repetitions", type=int, default=0,
                        help="run each mode binary N times in fresh processes, "
                             "interleaved and shuffled per round (overrides --repetitions)")
    parser.add_argument("--shuffle-modes", action="store_true")
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--benchmark-min-time", default="0.5s")
    parser.add_argument("--filter", dest="benchmark_filter", default="")
    parser.add_argument("--smoke", action="store_true")
    parser.add_argument("--no-plot", action="store_true")
    parser.add_argument("--benchmarks-only", action="store_true",
                        help="skip distribution probes, traces, tuner, and plots")
    parser.add_argument("--timeout", type=int, default=600,
                        help="maximum seconds for each subprocess")
    return parser.parse_args()


def run_json(command, output, env, timeout):
    print("+", " ".join(map(str, command)), flush=True)
    with output.open("w") as stream:
        subprocess.run(command, env=env, stdout=stream, stderr=subprocess.STDOUT,
                       check=True, timeout=timeout)
    json.loads(output.read_text())


def merge_benchmark_json(paths, destination):
    """Concatenate Google Benchmark outputs from several fresh processes into
    one file with the same schema (context from the first, all benchmarks)."""
    merged = None
    for index, path in enumerate(paths):
        data = json.loads(path.read_text())
        if merged is None:
            merged = data
            merged["context"]["fresh_process_rounds"] = len(paths)
            for entry in merged["benchmarks"]:
                entry["fresh_process_round"] = 0
            continue
        for entry in data["benchmarks"]:
            entry["fresh_process_round"] = index
            entry["repetition_index"] = index
        merged["benchmarks"].extend(data["benchmarks"])
    destination.write_text(json.dumps(merged, indent=1) + "\n")


def main():
    root = Path(__file__).resolve().parents[2]
    args = parse_args(root)
    executable_dir = args.build.resolve() / "benchmarks/scheduler_eval"
    executables = sorted(executable_dir.glob("bench_scheduler_eval_*"))
    modes = args.mode or [path.name.removeprefix("bench_scheduler_eval_")
                          for path in executables if path.is_file()
                          and not path.name.endswith(".json")]
    if not modes:
        raise RuntimeError("No scheduler evaluation executables found; configure with "
                           "-DOOX_BUILD_SCHEDULER_EVALS=ON")
    seed = args.seed if args.seed is not None else random.SystemRandom().randrange(1 << 31)
    rng = random.Random(seed)
    if args.shuffle_modes:
        rng.shuffle(modes)
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
    except (OSError, subprocess.CalledProcessError, subprocess.TimeoutExpired):
        compiler_version = "unavailable"
    benchmark_filter = "Launch/64" if args.smoke else args.benchmark_filter
    benchmark_min_time = "0.01s" if args.smoke else args.benchmark_min_time
    fresh_rounds = args.fresh_process_repetitions
    metadata = {
        "schema": 2,
        "complete": False,
        "timestamp_utc": timestamp,
        "host": socket.gethostname(),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "python": platform.python_version(),
        "threads": args.threads,
        "repetitions": args.repetitions if not fresh_rounds else 1,
        "fresh_process_repetitions": fresh_rounds,
        "shuffle_modes": args.shuffle_modes,
        "seed": seed,
        "benchmark_filter": benchmark_filter,
        "benchmark_min_time": benchmark_min_time,
        "subprocess_timeout_seconds": args.timeout,
        "smoke": args.smoke,
        "benchmarks_only": args.benchmarks_only,
        "modes": modes,
        "mode_order": [],
        "oox_commit": revision(root),
        "oox_dirty": is_dirty(root),
        "thesis_commit": revision(root, "thirdparty/composable-parallel-scheduler-thesis"),
        "pbbs_commit": revision(root, "thirdparty/pbbsbench"),
        "build_directory": str(args.build.resolve()),
        "build_type": cache.get("CMAKE_BUILD_TYPE", "unspecified"),
        "compiler": compiler,
        "compiler_version": compiler_version,
        "cxx_flags": cache.get("CMAKE_CXX_FLAGS", ""),
        "allocator": cache.get("OOX_ALLOCATOR", "unspecified"),
        "topology": host_topology(),
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

    def bench_command(mode, out_path, repetitions):
        command = [str(executable_dir / f"bench_scheduler_eval_{mode}"),
                   "--benchmark_out_format=json",
                   f"--benchmark_out={out_path}",
                   f"--benchmark_repetitions={repetitions}",
                   f"--benchmark_min_time={benchmark_min_time}"]
        if benchmark_filter:
            command.append(f"--benchmark_filter={benchmark_filter}")
        return command

    if fresh_rounds:
        round_files = {mode: [] for mode in modes}
        for round_index in range(fresh_rounds):
            order = list(modes)
            rng.shuffle(order)
            metadata["mode_order"].append(order)
            (output / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
            for mode in order:
                out_path = raw / f"bench_scheduler_eval_{mode}.round{round_index}.json"
                print("+", " ".join(bench_command(mode, out_path, 1)), flush=True)
                subprocess.run(bench_command(mode, out_path, 1), env=env,
                               check=True, timeout=args.timeout)
                json.loads(out_path.read_text())
                round_files[mode].append(out_path)
        for mode in modes:
            merge_benchmark_json(round_files[mode],
                                 raw / f"bench_scheduler_eval_{mode}.json")
    else:
        metadata["mode_order"].append(list(modes))
        for mode in modes:
            out_path = raw / f"bench_scheduler_eval_{mode}.json"
            print("+", " ".join(bench_command(mode, out_path, args.repetitions)), flush=True)
            subprocess.run(bench_command(mode, out_path, args.repetitions), env=env,
                           check=True, timeout=args.timeout)
            json.loads(out_path.read_text())

    if args.benchmarks_only:
        metadata["complete"] = True
        (output / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
        print(output)
        return

    for mode in modes:
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
