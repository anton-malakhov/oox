#!/usr/bin/env python3
"""Configure, build and execute a versioned cross-CPU calibration protocol."""
import argparse
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import platform
import shutil
import shlex
import subprocess
import sys

from calibration_protocol import (MODES, build_metadata, check_threads,
                                  clean_environment, command_output, hardware, make_plan)

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]


def cpu_set(value):
    result = set()
    try:
        for item in value.split(","):
            ends = item.split("-")
            if not 1 <= len(ends) <= 2:
                raise ValueError()
            first, last = int(ends[0]), int(ends[-1])
            if not 0 <= first <= last <= 65535:
                raise ValueError()
            result.update(range(first, last + 1))
    except ValueError:
        raise argparse.ArgumentTypeError("CPU set must look like 0-7,16-23")
    if not result:
        raise argparse.ArgumentTypeError("Empty CPU set")
    return result


def compact_profile(data, label):
    body = {str(work): (data["serial_cases"][f"balanced64_{work}"]["median"] - data["serial_cases"]["loop64"]["median"]) / 64
            for work in (100, 1000)}
    if any(not math.isfinite(v) or v <= 0 for v in body.values()):
        raise ValueError("Nonpositive or nonfinite body estimate; calibration rejected")
    return {"schema": "scheduler-calibration-profile-v2", "label": label,
            "recorded_at": data["recorded_at"], "hardware": data["hardware"],
            "toolchain": data["toolchain"], "protocol": data["protocol"],
            "binaries": data["binaries"], "sources": data["sources"],
            "body_ns": body, "primitives_ns": data["primitives_ns"],
            "measurements": data["measurements"], "paired": data["paired"],
            "meaning": "Primitive throughput and body service estimates, not a fully calibrated scheduler.",
            "unmeasured": ["cross-core steal latency", "rapid-only publication", "migration", "join-only cost", "tail contention"]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True, help="New result directory; never overwritten")
    parser.add_argument("--build", type=Path, default=ROOT / "build-scheduler-calibration")
    parser.add_argument("--label", help="Optional machine/profile label, not a hostname discovery")
    parser.add_argument("--threads", type=int, nargs="+", help="Default: supported subset of 2,4,8; max 64")
    parser.add_argument("--kernel", choices=("arithmetic_v1", "cpu_relax"), default="arithmetic_v1")
    parser.add_argument("--repetitions", type=int, default=9)
    parser.add_argument("--serial-repetitions", type=int, default=5)
    parser.add_argument("--min-ms", type=int, default=40)
    parser.add_argument("--seed", type=int, default=20261014)
    parser.add_argument("--timeout", type=float, default=300)
    parser.add_argument("--cpus", type=cpu_set, help="Linux process CPU set, e.g. 0-7; inherited by all workers")
    parser.add_argument("--allow-oversubscription", action="store_true")
    parser.add_argument("--no-configure", action="store_true", help="Reuse an existing Release build; still rebuild targets")
    parser.add_argument("--cmake-arg", action="append", default=[], help="Extra CMake argument, use --cmake-arg=-D...")
    parser.add_argument("--build-jobs", type=int, default=4)
    parser.add_argument("--plan-only", action="store_true", help="Print exact job plan; no writes/build/benchmarks")
    args = parser.parse_args()
    if platform.system() not in ("Darwin", "Linux") or platform.machine().lower() not in ("arm64", "aarch64", "x86_64", "amd64"):
        parser.error("Currently supports macOS/Linux x86-64 and ARM64, GCC/Clang toolchains")
    info = hardware()
    if args.cpus:
        if not hasattr(os, "sched_setaffinity"):
            parser.error("--cpus requires Linux; macOS has no supported CPU-set binding here")
        if not args.cpus.issubset(set(info["affinity_cpus"] or [])):
            parser.error("--cpus must be a subset of the current allowed CPU set")
        # Plan-only must not change this process's placement either.
        if not args.plan_only:
            os.sched_setaffinity(0, args.cpus)
            info = hardware()
        else:
            info["available_cpus"] = min(info["available_cpus"], len(args.cpus))
    threads = args.threads or [p for p in (2, 4, 8) if p <= info["available_cpus"]] or [1]
    try:
        plan = make_plan(threads, args.repetitions, args.serial_repetitions, args.seed, args.kernel, args.min_ms)
        check_threads(plan["threads"], info, args.allow_oversubscription)
        env = clean_environment()
        if args.build_jobs < 1 or args.build_jobs > 128:
            raise ValueError("build-jobs must be 1..128")
        if not math.isfinite(args.timeout) or args.timeout <= 0:
            raise ValueError("timeout must be positive and finite")
        if args.no_configure and args.cmake_arg:
            raise ValueError("--cmake-arg cannot be used with --no-configure")
    except ValueError as error:
        parser.error(str(error))
    if args.plan_only:
        print(json.dumps(plan, indent=2))
        return
    out, build = args.output.resolve(), args.build.resolve()
    if build == ROOT:
        parser.error("Use an out-of-source build directory")
    out.mkdir(parents=True, exist_ok=False)
    (out / "plan.json").write_text(json.dumps(plan, indent=2))
    status = {"state": "started", "plan_id": plan["id"], "started_at": datetime.now(timezone.utc).isoformat()}

    def checkpoint(state, **extra):
        status.update(state=state, **extra)
        (out / "status.json").write_text(json.dumps(status, indent=2))

    def run(command, name):
        print(name + "…", flush=True)
        with (out / (name + ".log")).open("w") as log:
            subprocess.run(command, cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT, check=True)

    try:
        checkpoint("configuring")
        if not args.no_configure:
            run(["cmake", "-S", str(ROOT), "-B", str(build), "-DCMAKE_BUILD_TYPE=Release",
                 "-DCMAKE_EXPORT_COMPILE_COMMANDS=ON", "-DOOX_BUILD_BENCHMARKS=ON",
                 "-DOOX_BUILD_TESTS=OFF", "-DOOX_BUILD_EXAMPLES=OFF", "-DOOX_BUILD_TASKBENCH=OFF",
                 "-DOOX_BUILD_SCHEDULER_EVALS=ON", "-DOOX_SCHEDULER_TRACE_TOOLS=ON",
                 "-DOOX_SCHEDULER_EVAL_STATS=OFF", "-DOOX_SCHEDULER_EVAL_PAPI=OFF",
                 "-DOOX_ENABLE_EIGEN=ON", "-DOOX_ENABLE_TBB=ON", "-DOOX_ENABLE_TF=OFF",
                 "-DOOX_ENABLE_FOLLY=OFF", "-DOOX_ENABLE_OMP=OFF", *args.cmake_arg], "configure")
        metadata = build_metadata(build)
        if Path(metadata["cache"].get("CMAKE_HOME_DIRECTORY", "")).resolve() != ROOT:
            raise ValueError("Build directory belongs to a different checkout")
        checkpoint("building")
        run(["cmake", "--build", str(build), "--parallel", str(args.build_jobs), "--target",
             *["measure_scheduler_" + mode for mode in (*MODES, "SERIAL_ELISION")]], "build")
        metadata.update(hardware=info, requested_cpus=sorted(args.cpus) if args.cpus else None,
                        git_head=command_output(["git", "-C", str(ROOT), "rev-parse", "HEAD"]),
                        git_status=command_output(["git", "-C", str(ROOT), "status", "--short"]),
                        environment={k: env[k] for k in ("CC", "CXX", "CXXFLAGS", "LDFLAGS", "MallocNanoZone", "MALLOC_CONF", "GLIBC_TUNABLES") if k in env})
        (out / "environment.json").write_text(json.dumps(metadata, indent=2))
        checkpoint("measuring")
        command = [sys.executable, "-B", str(HERE / "measure.py"), "--build", str(build), "--output", str(out / "raw"),
                   "--threads", *map(str, plan["threads"]), "--repetitions", str(args.repetitions),
                   "--serial-repetitions", str(args.serial_repetitions), "--seed", str(args.seed),
                   "--kernel", args.kernel, "--min-ms", str(args.min_ms), "--timeout", str(args.timeout)]
        if args.allow_oversubscription:
            command.append("--allow-oversubscription")
        run(command, "measurement")
        data = json.loads((out / "raw/measurement.json").read_text())
        if data["protocol"]["plan_id"] != plan["id"]:
            raise RuntimeError("Executed plan differs from requested plan")
        profile = compact_profile(data, args.label or info["cpu"])
        (out / "profile.json").write_text(json.dumps(profile, indent=2))
        node = shutil.which("node")
        node_version = command_output([node, "--version"]) if node else None
        node_major = (node_version or "").lstrip("v").split(".")[0]
        can_analyze = node_major.isdigit() and int(node_major) >= 18
        if can_analyze:
            checkpoint("analyzing")
            with (out / "report.json").open("w") as report, (out / "analysis.log").open("w") as log:
                subprocess.run([node, str(HERE / "analyze_measurements.cjs"), str(out / "raw/measurement.json")],
                               cwd=ROOT, env=env, stdout=report, stderr=log, check=True)
        checkpoint("complete", profile="profile.json", report="report.json" if can_analyze else None,
                   analysis_skipped=None if can_analyze else "Optional model analysis requires Node.js 18+",
                   completed_at=datetime.now(timezone.utc).isoformat())
        print("Profile: " + str(out / "profile.json"), flush=True)
        view_profile = out / ("report.json" if (out / "report.json").exists() else "profile.json")
        print("Viewer: " + shlex.join([sys.executable, str(HERE / "run.py"), "--profile", str(view_profile)]), flush=True)
    except BaseException as error:
        checkpoint("failed", phase=status["state"], error=str(error), error_type=type(error).__name__, partial_data_retained=True)
        raise


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, RuntimeError, subprocess.CalledProcessError, KeyboardInterrupt) as error:
        raise SystemExit("Calibration stopped; inspect status.json and logs. " + str(error))
