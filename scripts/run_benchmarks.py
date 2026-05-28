#!/usr/bin/env python3

import argparse
import csv
import datetime as dt
import itertools
import json
import os
import platform
import shutil
import subprocess
import sys
from pathlib import Path


def run(cmd, cwd):
    printable = " ".join(str(part) for part in cmd)
    print(f"+ {printable}", flush=True)
    subprocess.run([str(part) for part in cmd], cwd=cwd, check=True)


def capture(cmd, cwd):
    return subprocess.check_output([str(part) for part in cmd], cwd=cwd, text=True)


def repo_root():
    return Path(capture(["git", "rev-parse", "--show-toplevel"], Path.cwd()).strip())


def git(root, *args):
    return capture(["git", *args], root).strip()


def current_branch(root):
    return git(root, "branch", "--show-current")


def profile_name(requested):
    if requested != "auto":
        return requested
    machine = platform.machine().lower()
    if machine in ("arm64", "aarch64"):
        return "apple-m4"
    if machine in ("x86_64", "amd64"):
        return "x86"
    return machine or "local"


def branch_mode(branch, requested):
    if requested != "auto":
        return requested
    if branch.startswith("add-twist-testing"):
        return "comparison"
    return "oox-only"


def sysctl_value(name):
    try:
        return subprocess.check_output(["sysctl", "-n", name], text=True, stderr=subprocess.DEVNULL).strip()
    except (FileNotFoundError, subprocess.CalledProcessError):
        return ""


def machine_info(root, profile, mode, allocator):
    keys = [
        "machdep.cpu.brand_string",
        "hw.model",
        "hw.machine",
        "hw.ncpu",
        "hw.physicalcpu",
        "hw.logicalcpu",
        "hw.perflevel0.physicalcpu",
        "hw.perflevel0.logicalcpu",
        "hw.perflevel1.physicalcpu",
        "hw.perflevel1.logicalcpu",
        "hw.memsize",
    ]
    return {
        "profile": profile,
        "mode": mode,
        "allocator": allocator,
        "branch": current_branch(root),
        "commit": git(root, "rev-parse", "HEAD"),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "processor": platform.processor(),
        "python": platform.python_version(),
        "sysctl": {key: sysctl_value(key) for key in keys},
    }


def existing_path(path):
    candidate = Path(path)
    return candidate if candidate.exists() else None


def openmp_cmake_hints(out_dir):
    prefixes = [
        Path("/opt/homebrew/opt/libomp"),
        Path("/usr/local/opt/libomp"),
        Path("/opt/homebrew/opt/llvm"),
        Path("/usr/local/opt/llvm"),
    ]
    for prefix in prefixes:
        include_dir = existing_path(prefix / "include")
        if include_dir and not existing_path(include_dir / "omp.h"):
            include_dir = None
        if include_dir is None:
            clang_dirs = sorted((prefix / "lib" / "clang").glob("*/include"), reverse=True)
            if clang_dirs and existing_path(clang_dirs[0] / "omp.h"):
                include_dir = out_dir / "openmp-include"
                include_dir.mkdir(parents=True, exist_ok=True)
                target = include_dir / "omp.h"
                if not target.exists():
                    shutil.copy2(clang_dirs[0] / "omp.h", target)
        libomp = existing_path(prefix / "lib" / "libomp.dylib")
        if include_dir and libomp:
            return [
                f"-DOpenMP_CXX_FLAGS=-Xpreprocessor -fopenmp -I{include_dir.resolve()}",
                "-DOpenMP_CXX_LIB_NAMES=omp",
                f"-DOpenMP_omp_LIBRARY={libomp.resolve()}",
            ]
    return []


def configure(root, build_dir, out_dir, mode, jobs, allocator):
    enable_comparison = mode == "comparison"
    args = [
        "cmake",
        "-S",
        root,
        "-B",
        build_dir,
        "-DCMAKE_BUILD_TYPE=Release",
        "-DOOX_BUILD_TESTS=OFF",
        "-DOOX_BUILD_BENCHMARKS=ON",
        "-DOOX_BUILD_TASKBENCH=ON",
        "-DOOX_ENABLE_EXCEPTIONS=ON",
        "-DOOX_ENABLE_TBB=ON",
        f"-DOOX_ENABLE_TF={'ON' if enable_comparison else 'OFF'}",
        f"-DOOX_ENABLE_OMP={'ON' if enable_comparison else 'OFF'}",
        "-DOOX_ENABLE_FOLLY=OFF",
        f"-DOOX_ALLOCATOR={allocator}",
    ]
    if enable_comparison:
        args.extend(openmp_cmake_hints(out_dir))
    run(args, root)
    run(["cmake", "--build", build_dir, "--config", "Release", "-j", str(jobs)], root)


def ctest_smoke(root, build_dir):
    run(["ctest", "--test-dir", build_dir, "--output-on-failure", "-R", "bench"], root)


def split_csv(value):
    return [item.strip() for item in str(value).split(",") if item.strip()]


def time_to_seconds(value, unit):
    scale = {"ns": 1.0e-9, "us": 1.0e-6, "ms": 1.0e-3, "s": 1.0}
    return float(value) * scale.get(unit, 1.0e-9)


def rows_from_benchmark_json(stdout, params):
    data = json.loads(stdout)
    rows = []
    for item in data.get("benchmarks", []):
        if item.get("run_type") != "iteration":
            continue
        name_parts = str(item.get("name", "")).split("/")
        backend = name_parts[1] if len(name_parts) > 1 else ""
        wall_s = time_to_seconds(item.get("real_time", 0), item.get("time_unit", "ns"))
        width = int(round(float(item.get("width", params["width"]))))
        tasks = int(round(float(item.get("tasks", int(params["height"]) * width))))
        rows.append(
            {
                "backend": backend,
                "runner": params["runner"],
                "threads": int(round(float(item.get("threads", params["threads"] if params["threads"] != "auto" else 1)))),
                "height": int(params["height"]),
                "width": width,
                "graphs": int(params["graphs"]),
                "pattern": params["pattern"],
                "radix": int(params["radix"]),
                "kernel": params["kernel"],
                "iterations": int(params["iterations"]),
                "benchmark_iterations": int(round(float(item.get("iterations", 1)))),
                "output_bytes": int(params["output-bytes"]),
                "scratch_bytes": int(params["scratch-bytes"]),
                "imbalance": float(params["imbalance"]),
                "validate": params["validate"],
                "tasks": tasks,
                "edges": int(round(float(item.get("edges", 0)))),
                "wall_s": f"{wall_s:.12g}",
                "task_granularity_us": f"{(wall_s * 1e6 / tasks) if tasks > 0 else 0.0:.12g}",
                "throughput": f"{(tasks / wall_s) if wall_s > 0 else 0.0:.12g}",
                "ok": "true" if float(item.get("ok", 1.0)) >= 0.5 else "false",
            }
        )
    return rows


def run_taskbench(root, build_dir, out_dir, mode, args):
    implementations = [("oox", "bench_taskbench_TBB.exe", "oox")]
    if mode == "comparison":
        implementations.extend(
            [
                ("tbb-flow", "bench_taskbench_TBB.exe", "tbb-flow"),
                ("taskflow", "bench_taskbench_TF.exe", "taskflow"),
                ("openmp", "bench_taskbench_OMP.exe", "openmp"),
            ]
        )

    taskbench_dir = out_dir / "taskbench"
    taskbench_dir.mkdir(parents=True, exist_ok=True)
    dimensions = {
        "height": split_csv(args.height),
        "width": split_csv(args.width),
        "threads": split_csv(args.threads),
        "graphs": ["1"],
        "pattern": split_csv(args.patterns),
        "radix": split_csv(args.radix),
        "kernel": split_csv(args.kernel),
        "iterations": split_csv(args.iterations),
        "output-bytes": split_csv(args.output_bytes),
        "scratch-bytes": split_csv(args.scratch_bytes),
        "imbalance": split_csv(args.imbalance),
    }

    for label, target, runner in implementations:
        exe = build_dir / "benchmarks" / "taskbench" / target
        if not exe.exists():
            print(f"Skipping {label}: {exe} was not built.", file=sys.stderr)
            continue
        csv_path = taskbench_dir / f"{label}.csv"
        with csv_path.open("w", newline="") as f:
            writer = None
            for values in itertools.product(*dimensions.values()):
                params = dict(zip(dimensions.keys(), values))
                params["runner"] = runner
                cmd = [
                    str(exe),
                    f"--height={params['height']}",
                    f"--width={params['width']}",
                    f"--threads={params['threads']}",
                    f"--graphs={params['graphs']}",
                    f"--pattern={params['pattern']}",
                    f"--radix={params['radix']}",
                    f"--kernel={params['kernel']}",
                    f"--iterations={params['iterations']}",
                    f"--output-bytes={params['output-bytes']}",
                    f"--scratch-bytes={params['scratch-bytes']}",
                    f"--imbalance={params['imbalance']}",
                    f"--validate={args.validate}",
                    "--repetitions=1",
                    "--warmups=0",
                    f"--runner={runner}",
                    "--benchmark_format=json",
                    "--benchmark_report_aggregates_only=false",
                    f"--benchmark_repetitions={args.repetitions}",
                    f"--benchmark_min_time={benchmark_duration(args.min_time)}",
                    f"--benchmark_min_warmup_time={args.warmups}",
                ]
                printable = " ".join(cmd)
                print(f"+ {printable}", flush=True)
                proc = subprocess.run(cmd, cwd=root, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                if proc.returncode != 0:
                    sys.stderr.write(proc.stderr)
                    raise RuntimeError(f"taskbench run failed: {printable}")
                rows = rows_from_benchmark_json(proc.stdout, {**params, "validate": args.validate})
                if not rows:
                    raise RuntimeError(f"taskbench run returned no rows: {printable}")
                if writer is None:
                    writer = csv.DictWriter(f, fieldnames=rows[0].keys())
                    writer.writeheader()
                writer.writerows(rows)
                f.flush()


def run_if_exists(root, exe, cmd_args):
    if not exe.exists():
        print(f"Skipping missing benchmark executable: {exe}", file=sys.stderr)
        return
    run([exe, *cmd_args], root)


def benchmark_duration(value):
    text = str(value)
    if text.endswith(("s", "ms", "us", "ns")):
        return text
    return f"{text}s"


def big_graph_smoke(root, build_dir, out_dir, quick, min_time, warmups):
    result_dir = out_dir / "big-graph-smoke"
    result_dir.mkdir(parents=True, exist_ok=True)
    min_time = "0.001s" if quick else benchmark_duration(min_time)
    warmup_time = "0" if quick else str(warmups)

    run_if_exists(
        root,
        build_dir / "benchmarks" / "bench_exceptions_TBB.TBB_SIMPLE_exc",
        [
            "--benchmark_filter=OOX_(Chain_RootThrows|Diamond_ThrowMiddle|Fanout_RootThrows|LateConsumers_AfterRootThrows)/256",
            f"--benchmark_min_time={min_time}",
            f"--benchmark_min_warmup_time={warmup_time}",
            "--benchmark_repetitions=1",
            f"--benchmark_out={result_dir / 'exceptions_256.json'}",
            "--benchmark_out_format=json",
        ],
    )
    run_if_exists(
        root,
        build_dir / "benchmarks" / "bench_cancellation_TBB.exc",
        [
            "--benchmark_filter=OOX_Cancel_(Chain|Fanout)/1024",
            f"--benchmark_min_time={min_time}",
            f"--benchmark_min_warmup_time={warmup_time}",
            "--benchmark_repetitions=1",
            f"--benchmark_out={result_dir / 'cancellation_1024.json'}",
            "--benchmark_out_format=json",
        ],
    )
    run_if_exists(
        root,
        build_dir / "benchmarks" / "bench_taskbench_alt_failures",
        [
            "--benchmark_filter=BM_(TbbFlow_((Early|Mid|Late)(Throw|Cancel))|Taskflow_(Early|Mid|Late)Throw)/(128|512|2048)/(32|64)",
            f"--benchmark_min_time={min_time}",
            f"--benchmark_min_warmup_time={warmup_time}",
            "--benchmark_repetitions=1",
            f"--benchmark_out={result_dir / 'taskbench_alt_failures_widths_up_to_128k.json'}",
            "--benchmark_out_format=json",
        ],
    )
    run_if_exists(
        root,
        build_dir / "benchmarks" / "bench_branching_cancel",
        [
            "--benchmark_filter=/22",
            f"--benchmark_min_time={min_time}",
            f"--benchmark_min_warmup_time={warmup_time}",
            "--benchmark_repetitions=1",
            f"--benchmark_out={result_dir / 'branching_cancel_depth22.json'}",
            "--benchmark_out_format=json",
        ],
    )


def main():
    parser = argparse.ArgumentParser(description="Configure, build and run OOX benchmark suites.")
    parser.add_argument("--profile", choices=["auto", "apple-m4", "x86"], default="auto")
    parser.add_argument("--mode", choices=["auto", "comparison", "oox-only"], default="auto")
    parser.add_argument("--build-dir", default="")
    parser.add_argument("--out-dir", default="")
    parser.add_argument("--jobs", type=int, default=os.cpu_count() or 8)
    parser.add_argument("--allocator", choices=["jemalloc", "system", "tbb"], default="system")
    parser.add_argument("--skip-configure", action="store_true")
    parser.add_argument("--skip-ctest", action="store_true")
    parser.add_argument("--skip-taskbench", action="store_true")
    parser.add_argument("--skip-big-graph-smoke", action="store_true")
    parser.add_argument("--quick", action="store_true")
    parser.add_argument("--height", default="64")
    parser.add_argument("--width", default="16,32,64")
    parser.add_argument("--threads", default="auto")
    parser.add_argument("--patterns", default="stencil")
    parser.add_argument("--radix", default="3")
    parser.add_argument("--kernel", default="compute")
    parser.add_argument("--iterations", default="1")
    parser.add_argument("--output-bytes", default="16")
    parser.add_argument("--scratch-bytes", default="0")
    parser.add_argument("--imbalance", default="0")
    parser.add_argument("--validate", default="1")
    parser.add_argument("--repetitions", default="1")
    parser.add_argument("--warmups", default="1.0")
    parser.add_argument("--min-attempts", default="1")
    parser.add_argument("--min-time", default="1.0")
    args = parser.parse_args()

    if not args.quick:
        args.height = "1000" if args.height == "64" else args.height
        args.patterns = "stencil,sweep,nearest,spread,random,fft,tree" if args.patterns == "stencil" else args.patterns
        args.validate = "0" if args.validate == "1" else args.validate

    root = repo_root()
    profile = profile_name(args.profile)
    mode = branch_mode(current_branch(root), args.mode)
    stamp = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    build_dir = Path(args.build_dir) if args.build_dir else root / f"build-benchmarks-{profile}"
    out_dir = Path(args.out_dir) if args.out_dir else root / "results" / f"benchmarks-{profile}-{stamp}"
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "machine.json").write_text(json.dumps(machine_info(root, profile, mode, args.allocator), indent=2) + "\n")

    if not args.skip_configure:
        configure(root, build_dir, out_dir, mode, args.jobs, args.allocator)
    if not args.skip_ctest:
        ctest_smoke(root, build_dir)
    if not args.skip_taskbench:
        run_taskbench(root, build_dir, out_dir, mode, args)
    if not args.skip_big_graph_smoke:
        big_graph_smoke(root, build_dir, out_dir, args.quick, args.min_time, args.warmups)

    print(out_dir)


if __name__ == "__main__":
    main()
