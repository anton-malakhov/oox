#!/usr/bin/env python3
"""Build and run paired Rapid-group experiments using only the Python standard library."""
import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import statistics
import subprocess
import zipfile

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
POLICIES = ("lazy", "auto", "simple", "static", "affinity")


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def plan(threads, groups, policies, repetitions, seed, residents):
    contrasts = []
    for p in sorted(set(threads)):
        if not 2 <= p <= 1024:
            raise ValueError("threads must be 2..1024")
        selected = sorted(set(groups or [g for g in (2, 4, 8, 16, 32, 64) if g <= p]))
        if not groups and min(p, 64) not in selected:
            selected.append(min(p, 64))
        if not selected or any(g < 2 or g > min(p, 64) for g in selected):
            raise ValueError("groups must be 2..min(threads,64); 128 is a pool size, not a supported initial group size")
        r = p if residents is None else residents
        if not max(selected) <= r <= p:
            raise ValueError("residents must cover every selected group and not exceed the pool")
        for policy in policies:
            def config(group, placement):
                return dict(workers=p, group=group, residents=r, policy=policy, placement=placement)
            for g in selected:
                if g != max(selected):
                    contrasts.append(dict(kind="group_size", a=config(max(selected), "local"), b=config(g, "local")))
                if policy in ("auto", "simple"):
                    for placement in ("all", "outsiders"):
                        if placement == "outsiders" and g == p:
                            continue
                        contrasts.append(dict(kind="placement", a=config(g, "local"), b=config(g, placement)))
    if not contrasts or not 3 <= repetitions <= 30:
        raise ValueError("need at least one contrast and 3..30 repetitions")
    blocks = [(i, r) for i in range(len(contrasts)) for r in range(repetitions)]
    blocks.sort(key=lambda pair: hashlib.sha256(f"rapid-group-v1/{seed}/{pair[0]}/{pair[1]}".encode()).digest())
    jobs = [dict(contrast=i, repetition=r, side=side, config=contrasts[i][side])
            for i, r in blocks for side in (("a", "b") if r % 2 == 0 else ("b", "a"))]
    return dict(schema="rapid-group-plan-v1", seed=seed, repetitions=repetitions, contrasts=contrasts, jobs=jobs)


def command(binary, config, minimum, diagnose=False):
    result = [str(binary)]
    for key, value in config.items():
        result += ["--" + key, str(value)]
    result += ["--min-ms", str(minimum)]
    return result + (["--diagnose"] if diagnose else [])


def hardware():
    result = dict(platform=platform.platform(), machine=platform.machine(), logical_cpus=os.cpu_count(),
                  affinity_cpus=sorted(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else None,
                  load_average=list(os.getloadavg()) if hasattr(os, "getloadavg") else None,
                  per_thread_pinning=False, cpu=platform.processor(), cpu_quota=None)
    if platform.system() == "Darwin":
        result["cpu"] = subprocess.run(["sysctl", "-n", "machdep.cpu.brand_string"],
                                       capture_output=True, text=True, timeout=5).stdout.strip()
    elif platform.system() == "Linux":
        try:
            lines = Path("/proc/cpuinfo").read_text().splitlines()
            result["cpu"] = next(line.split(":", 1)[1].strip() for line in lines if line.startswith("model name"))
        except (OSError, StopIteration):
            pass
        try:
            quota, period = Path("/sys/fs/cgroup/cpu.max").read_text().split()
            if quota != "max": result["cpu_quota"] = int(quota) / int(period)
        except (OSError, ValueError, ZeroDivisionError):
            pass
    return result


def clean_environment():
    if any(os.environ.get(k) for k in ("LD_PRELOAD", "DYLD_INSERT_LIBRARIES")):
        raise ValueError("unset loader injection variables before measurement")
    prefixes = ("OOX_RAPID_", "OOX_BENCH_", "BENCH_NUM_THREADS", "OMP_", "KMP_", "PARLAY_", "TBB_")
    return {**{k: v for k, v in os.environ.items() if not k.startswith(prefixes)}, "LC_ALL": "C", "TZ": "UTC"}


def validate(data, config, diagnostic):
    if data.get("schema") != "rapid-group-experiment-v1" or data.get("oracle_ok") is not True:
        raise ValueError("invalid result or failed correctness oracle")
    expected = {"workers": config["workers"], "group_limit": config["group"],
                "resident_limit": config["residents"], "policy": config["policy"],
                "placement": config["placement"], "instrumented": diagnostic, "grain": 2, "kernel": "arithmetic_v1"}
    if any(data.get(k) != v for k, v in expected.items()):
        raise ValueError("configuration mismatch")
    if diagnostic:
        counts = data["callbacks_by_worker"]
        if len(counts) != config["workers"] or sum(counts) != 5 * 4096:
            raise ValueError("diagnostic worker accounting")
        if len(data["captured_helpers"]) != 5 or any(not 0 <= n < config["group"] for n in data["captured_helpers"]):
            raise ValueError("diagnostic capture accounting")
    else:
        cases = data.get("cases", [])
        if len(cases) != 10 or len({c["case"] for c in cases}) != 10 or any(
                not math.isfinite(c["ns"]) or c["ns"] <= 0 or c["calls"] < 16 for c in cases):
            raise ValueError("invalid timings")


def summarize(experiment, measurements):
    results = []
    for i, contrast in enumerate(experiment["contrasts"]):
        cases = []
        for name in sorted(measurements[i, 0, "a"]):
            a, b = ([measurements[i, r, side][name] for r in range(experiment["repetitions"])] for side in ("a", "b"))
            ratios = [y / x for x, y in zip(a, b)]
            q = statistics.quantiles(ratios, n=4, method="inclusive")
            cases.append(dict(case=name, a_ns=statistics.median(a), b_ns=statistics.median(b),
                              time_ratio=statistics.median(ratios), ratio_q25=q[0], ratio_q75=q[2],
                              paired_ratios=ratios, slower_pairs=sum(x > 1 for x in ratios)))
        results.append(dict(**contrast, cases=cases,
                            geomean_time_ratio=math.exp(statistics.mean(math.log(c["time_ratio"]) for c in cases))))
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build", type=Path, default=ROOT / "build-rapid-group-experiment")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--threads", nargs="+", type=int, default=[8])
    parser.add_argument("--groups", nargs="+", type=int)
    parser.add_argument("--policies", nargs="+", choices=POLICIES, default=list(POLICIES))
    parser.add_argument("--residents", type=int, help="fixed resident limit; default is full pool for every group")
    parser.add_argument("--repetitions", type=int, default=7)
    parser.add_argument("--min-ms", type=int, default=40)
    parser.add_argument("--seed", type=int, default=20261015)
    parser.add_argument("--jobs", type=int, default=4, help="build parallelism only; measurements are sequential")
    parser.add_argument("--stress-workers", type=int, help="add generated C++ oracle tests using this many real workers")
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument("--plan-only", action="store_true")
    parser.add_argument("--allow-oversubscription", action="store_true")
    args = parser.parse_args()
    try:
        experiment = plan(args.threads, args.groups, list(dict.fromkeys(args.policies)), args.repetitions, args.seed, args.residents)
        if not 5 <= args.min_ms <= 1000 or not 1 <= args.jobs <= 64:
            raise ValueError("min-ms=5..1000 and jobs=1..64")
        if args.stress_workers is not None and not 4 <= args.stress_workers <= 1024:
            raise ValueError("stress-workers must be 4..1024")
        if args.plan_only:
            print(json.dumps(experiment, indent=2)); return
        if args.output is None:
            raise ValueError("--output must name a new directory")
        machine = hardware()
        available = len(machine["affinity_cpus"]) if machine["affinity_cpus"] else machine["logical_cpus"] or 1
        if machine["cpu_quota"] is not None:
            available = min(available, max(1, math.floor(machine["cpu_quota"])))
        if max(args.threads + [args.stress_workers or 0]) > available and not args.allow_oversubscription:
            raise ValueError("requested pool exceeds available logical CPUs; use the intended machine or explicitly allow oversubscription")
        env = clean_environment()
    except ValueError as error:
        parser.error(str(error))
    build = args.build.resolve()
    if not args.skip_build:
        subprocess.run(["cmake", "-S", str(HERE), "-B", str(build), "-DCMAKE_BUILD_TYPE=Release"], check=True)
        subprocess.run(["cmake", "--build", str(build), "--parallel", str(args.jobs)], check=True)
    cache = (build / "CMakeCache.txt").read_text()
    if "CMAKE_BUILD_TYPE:STRING=Release" not in cache or any(flag in cache for flag in
            ("-fsanitize", "OOX_SCHEDULER_TRACE", "OOX_EIGEN_ENABLE_STATS", "OOX_GROUP_DIAGNOSTICS")):
        raise ValueError("use a clean Release build without global tracing, statistics or sanitizers")
    subprocess.run(["ctest", "--test-dir", str(build), "--output-on-failure"], check=True)
    if args.stress_workers:
        subprocess.run([str(build / "rapid_group_tests"), "--workers", str(args.stress_workers)], check=True, timeout=900)
    out = args.output.resolve(); out.mkdir(parents=True, exist_ok=False)
    binaries = {name: build / name for name in ("rapid_group_probe", "rapid_group_diagnose", "rapid_group_tests")}
    hashes = {name: digest(p) for name, p in binaries.items()}
    sources = sorted(set(list(HERE.glob("*.cpp")) + list(HERE.glob("*.h")) + list(HERE.glob("*.py")) +
                         [HERE / "CMakeLists.txt", ROOT / "benchmarks/eigen/arguments.h", ROOT / "test_support/eigen_resident.h", ROOT / "tests/eigen_test_wait.h"] + list((ROOT / "oox/eigen").glob("*.h"))))
    source_hashes = {str(p.relative_to(ROOT)): digest(p) for p in sources}
    with zipfile.ZipFile(out / "sources.zip", "x", compression=zipfile.ZIP_DEFLATED) as archive:
        for p in sources: archive.write(p, str(p.relative_to(ROOT)))
    manifest = dict(plan=experiment, hardware=machine, binary_sha256=hashes, source_sha256=source_hashes,
                    cmake_cache=cache, minimum_ms=args.min_ms, warmup_calls=64, grain=2, kernel="arithmetic_v1",
                    note="Adjacent fresh-process pairs, alternating order. Group limit is a cap, not a guarantee of capture. IQR is dispersion, not a confidence interval.")
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2))
    configs = {json.dumps(job["config"], sort_keys=True): job["config"] for job in experiment["jobs"]}
    diagnostics = []
    for number, config in enumerate(configs.values()):
        cmd = command(binaries["rapid_group_diagnose"], config, args.min_ms, True)
        run = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=120)
        (out / f"diagnostic-{number:03d}.log").write_text(run.stderr)
        (out / f"diagnostic-{number:03d}.json").write_text(run.stdout)
        run.check_returncode(); data = json.loads(run.stdout); validate(data, config, True)
        diagnostics.append(dict(config=config, data=data, command=cmd))
    (out / "diagnostics.json").write_text(json.dumps(diagnostics, indent=2))
    measurements = {}
    for number, job in enumerate(experiment["jobs"]):
        cmd = command(binaries["rapid_group_probe"], job["config"], args.min_ms)
        run = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=120)
        (out / f"raw-{number:04d}.json").write_text(run.stdout)
        (out / f"raw-{number:04d}.log").write_text(run.stderr)
        run.check_returncode(); data = json.loads(run.stdout); validate(data, job["config"], False)
        measurements[job["contrast"], job["repetition"], job["side"]] = {r["case"]: r["ns"] for r in data["cases"]}
        if (number + 1) % 10 == 0: print(f"{number + 1}/{len(experiment['jobs'])} processes", flush=True)
    if hashes != {name: digest(p) for name, p in binaries.items()} or source_hashes != {str(p.relative_to(ROOT)): digest(p) for p in sources}:
        raise ValueError("sources or binaries changed during experiment")
    after = hardware()
    if any(machine[k] != after[k] for k in ("machine", "cpu", "logical_cpus", "affinity_cpus", "cpu_quota")):
        raise ValueError("CPU allocation changed during measurement")
    report = dict(schema="rapid-group-report-v1", contrasts=summarize(experiment, measurements),
                  hardware_after=after, all_oracles_passed=True, manifest="manifest.json")
    (out / "report.json").write_text(json.dumps(report, indent=2))
    print(out / "report.json")


if __name__ == "__main__":
    main()
