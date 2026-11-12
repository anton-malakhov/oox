#!/usr/bin/env python3
"""Sequential, fresh-process A/B; no runtime trace hooks or per-call clocks."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import statistics
import subprocess
import zipfile
from calibration_protocol import (CASES, MODES, build_metadata, check_threads,
                                  clean_environment, hardware, make_plan)

ROOT = Path(__file__).resolve().parents[2]


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def summary(values):
    q = statistics.quantiles(values, n=4, method="inclusive")
    return {"median": statistics.median(values), "q25": q[0], "q75": q[2], "n": len(values)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--repetitions", type=int, default=9)
    parser.add_argument("--min-ms", type=int, default=40)
    parser.add_argument("--threads", type=int, nargs="+", default=[4, 8])
    parser.add_argument("--seed", type=int, default=20261014)
    parser.add_argument("--serial-repetitions", type=int, default=5)
    parser.add_argument("--kernel", choices=("cpu_relax", "arithmetic_v1"), default="cpu_relax")
    parser.add_argument("--timeout", type=float, default=300)
    parser.add_argument("--allow-oversubscription", action="store_true")
    args = parser.parse_args()
    try:
        plan = make_plan(args.threads, args.repetitions, args.serial_repetitions, args.seed, args.kernel, args.min_ms)
        machine = hardware()
        check_threads(plan["threads"], machine, args.allow_oversubscription)
        toolchain = build_metadata(args.build)
        env = clean_environment()
        if not math.isfinite(args.timeout) or args.timeout <= 0:
            raise ValueError("timeout must be positive and finite")
    except ValueError as error:
        parser.error(str(error))
    out = args.output.resolve()
    out.mkdir(parents=True, exist_ok=False)
    (out / "plan.json").write_text(json.dumps(plan, indent=2))
    binaries = {mode: args.build.resolve() / "benchmarks/scheduler_eval" / ("measure_scheduler_" + mode)
                for mode in (*MODES, "SERIAL_ELISION")}
    before = {mode: sha(path) for mode, path in binaries.items()}
    source_paths = [Path(__file__), Path(__file__).with_name("measure_scheduler.cpp")]
    source_paths.append(Path(__file__).with_name("calibration_protocol.py"))
    source_paths += [Path(__file__).with_name(name) for name in ("calibrate.py", "analyze_measurements.cjs", "lab-engine.js")]
    source_paths += [ROOT / name for name in ("CMakeLists.txt", "benchmarks/CMakeLists.txt", "benchmarks/scheduler_eval/CMakeLists.txt")]
    source_paths += list((ROOT / "oox/eigen").glob("*.h"))
    source_paths += list((ROOT / "benchmarks/scheduler_eval/runtime").glob("*.h"))
    source_paths += list((ROOT / "benchmarks/eigen").glob("*.h"))
    source_paths.append(ROOT / "benchmarks/rapid_start.h")
    sources = {str(p.relative_to(ROOT)): sha(p) for p in source_paths}
    with zipfile.ZipFile(out / "sources.zip", "x", compression=zipfile.ZIP_DEFLATED) as archive:
        for relative in sorted(sources):
            entry = zipfile.ZipInfo(relative, date_time=(1980, 1, 1, 0, 0, 0))
            entry.compress_type = zipfile.ZIP_DEFLATED
            archive.writestr(entry, (ROOT / relative).read_bytes())
    env.update(OOX_RAPID_AUTOCALIBRATE="0")
    jobs = []

    def execute(mode, workers, repetition):
        if sha(binaries[mode]) != before[mode]:
            raise RuntimeError("Binary changed")
        run_env = dict(env, BENCH_NUM_THREADS=str(workers), OOX_RAPID_RESIDENT_LIMIT=str(workers))
        command = [str(binaries[mode]), "--min-ms", str(args.min_ms), "--kernel", args.kernel]
        name = f"{len(jobs):03d}-{mode}-p{workers}-r{repetition}"
        try:
            raw = subprocess.run(command, env=run_env, capture_output=True, text=True, timeout=args.timeout)
        except subprocess.TimeoutExpired as error:
            (out / (name + ".log")).write_text("Timed out: " + str(error))
            raise
        (out / (name + ".json")).write_text(raw.stdout)
        (out / (name + ".log")).write_text(raw.stderr)
        if raw.returncode:
            raise RuntimeError(f"{name} failed, exit {raw.returncode}; see raw log")
        data = json.loads(raw.stdout)
        if data["mode"] != mode or data["workers"] != workers or data["instrumented"] or not data["oracle_ok"]:
            raise ValueError("Invalid measurement metadata")
        if data.get("suite_version") != 2 or data.get("kernel") != args.kernel or data.get("grain") != 2:
            raise ValueError("Benchmark suite/kernel mismatch; rebuild current targets")
        if tuple((r["case"], r["n"], r["work"], r["pattern"]) for r in data["cases"]) != CASES:
            raise ValueError("Invalid case set")
        if any(not math.isfinite(r["ns"]) or r["ns"] <= 0 or r["calls"] < 16 for r in data["cases"]):
            raise ValueError("Invalid timing")
        if mode == "SERIAL_ELISION" and (any(not math.isfinite(v) or v <= 0 for v in data["primitives_ns"].values())
                                         or data["primitives_ns"].get("sc_store_acquire_load", 0) <= .001):
            raise ValueError("Invalid primitive probe; calibration rejected")
        job = {"mode": mode, "workers": workers, "repetition": repetition, "command": command,
               "raw": name + ".json", "data": data}
        jobs.append(job)
        (out / "progress.json").write_text(json.dumps({"completed": len(jobs), "total": len(plan["jobs"]), "latest": name}))
        print(name, flush=True)

    for job in plan["jobs"]:
        execute(job["mode"], job["workers"], job["repetition"])
    after = {mode: sha(path) for mode, path in binaries.items()}
    if after != before or any(sha(ROOT / path) != value for path, value in sources.items()):
        raise RuntimeError("Sources or binaries changed during measurement")
    machine_after = hardware()
    for key in ("cpu", "machine", "logical_cpus", "affinity_cpus", "cgroup_quota_cpus"):
        if machine.get(key) != machine_after.get(key):
            raise RuntimeError("Machine allocation changed during measurement: " + key)
    paired = []
    measurements = []
    for workers in plan["threads"]:
        for case in [r["case"] for r in jobs[0]["data"]["cases"]]:
            values = {}
            for mode in MODES:
                samples = {j["repetition"]: next(r["ns"] for r in j["data"]["cases"] if r["case"] == case)
                           for j in jobs if j["mode"] == mode and j["workers"] == workers}
                values[mode] = samples
                measurements.append({"workers": workers, "case": case, "mode": mode, "ns": summary(list(samples.values()))})
            for reference in ("RAPID_START", "TBB_AUTO"):
                ratios = [values["RAPID_AUTO"][r] / values[reference][r] for r in range(args.repetitions)]
                paired.append({"workers": workers, "case": case, "reference": reference,
                               "ratio": summary(ratios), "slower_pairs": sum(r > 1 for r in ratios), "pairs": ratios})
    serial = [j["data"] for j in jobs if j["mode"] == "SERIAL_ELISION"]
    primitives = {key: summary([d["primitives_ns"][key] for d in serial]) for key in serial[0]["primitives_ns"]}
    serial_cases = {r["case"]: summary([next(x["ns"] for x in d["cases"] if x["case"] == r["case"]) for d in serial])
                    for r in serial[0]["cases"]}
    result = {"schema": "scheduler-local-measurement-v1", "recorded_at": datetime.now(timezone.utc).isoformat(),
              "hardware": machine, "hardware_after": machine_after, "toolchain": toolchain,
              "protocol": {"repetitions": args.repetitions, "minimum_ms": args.min_ms, "seed": args.seed,
                           "version": plan["version"], "plan_id": plan["id"], "suite_version": 2,
                           "threads": plan["threads"], "serial_repetitions": args.serial_repetitions,
                           "kernel": args.kernel, "adaptive_calls": True,
                           "warmup_calls": 64, "grain": 2, "instrumented": False, "resident_limit": "full",
                           "autocalibration": False, "cost_model_validated": False,
                           "note": "Medians of within-process warm-call means; paired ratio dispersion is not a confidence interval."},
              "binaries": before, "sources": sources, "source_archive_sha256": sha(out / "sources.zip"),
              "jobs": jobs, "measurements": measurements,
              "paired": paired, "serial_cases": serial_cases, "primitives_ns": primitives}
    (out / "measurement.json").write_text(json.dumps(result, indent=2))
    print(out / "measurement.json", flush=True)


if __name__ == "__main__":
    main()
