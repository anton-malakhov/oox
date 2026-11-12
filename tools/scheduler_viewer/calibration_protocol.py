"""Versioned plans and bounded environment metadata for local calibration."""
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import subprocess

VERSION = "scheduler-calibration-v2"
MODES = ("RAPID_AUTO", "RAPID_START", "TBB_AUTO")
CASES = (
    ("loop64", 64, 0, 0), ("loop4096", 4096, 0, 0),
    ("balanced64_100", 64, 100, 0), ("balanced64_1000", 64, 1000, 0),
    ("tail64_100", 64, 100, 1), ("tail64_1000", 64, 1000, 1),
    ("cluster64_1000", 64, 1000, 2), ("single64_1000", 64, 1000, 3),
    ("balanced4096_100", 4096, 100, 0), ("tail4096_100", 4096, 100, 1))


def digest(value):
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    return hashlib.sha256(encoded).hexdigest()


def make_plan(threads, repetitions, serial_repetitions, seed, kernel, minimum_ms):
    """No Python PRNG dependency: SHA ordering is stable across Python versions."""
    threads = sorted(set(threads))
    if not threads or any(type(p) is not int or not 1 <= p <= 64 for p in threads):
        raise ValueError("threads must be 1..64 (bitmask prototype limit)")
    if not 3 <= repetitions <= 30 or not 3 <= serial_repetitions <= 30:
        raise ValueError("repetitions must be 3..30")
    if type(seed) is not int or not 0 <= seed <= 0xffffffff:
        raise ValueError("seed must be uint32")
    if kernel not in ("arithmetic_v1", "cpu_relax") or not 5 <= minimum_ms <= 1000:
        raise ValueError("invalid kernel or minimum-ms")
    jobs = [{"mode": "SERIAL_ELISION", "workers": 1, "repetition": r} for r in range(serial_repetitions)]
    blocks = [(p, r) for p in threads for r in range(repetitions)]
    blocks.sort(key=lambda b: (digest([VERSION, seed, b[0], b[1]]), b))
    for workers, rep in blocks:
        order = list(MODES[:2]) if rep % 2 == 0 else list(reversed(MODES[:2]))
        order.insert(rep % 3, "TBB_AUTO")
        jobs.extend({"mode": mode, "workers": workers, "repetition": rep} for mode in order)
    plan = {"version": VERSION, "suite_version": 2, "threads": threads,
            "repetitions": repetitions, "serial_repetitions": serial_repetitions,
            "seed": seed, "kernel": kernel, "minimum_ms": minimum_ms,
            "grain": 2, "warmup_calls": 64, "cases": CASES, "jobs": jobs,
            "adaptive_calls": True}
    plan["id"] = digest(plan)
    return plan


def read_text(path, limit=65536):
    try:
        with Path(path).open("r", errors="replace") as source:
            return source.read(limit)
    except OSError:
        return None


def command_output(command):
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=10)
        return result.stdout[:8192].strip() if result.returncode == 0 else None
    except (OSError, subprocess.TimeoutExpired):
        return None


def hardware():
    system = platform.system()
    result = {"system": system, "platform": platform.platform(), "machine": platform.machine(),
              "logical_cpus": os.cpu_count(), "cpu": platform.processor() or "unknown",
              "affinity_cpus": sorted(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else None,
              "cgroup_quota_cpus": None, "pinned": False,
              "load_average": list(os.getloadavg()) if hasattr(os, "getloadavg") else None}
    if system == "Darwin":
        keys = ("machdep.cpu.brand_string", "hw.model", "hw.logicalcpu", "hw.physicalcpu",
                "hw.perflevel0.name", "hw.perflevel0.physicalcpu", "hw.perflevel1.name", "hw.perflevel1.physicalcpu")
        result["sysctl"] = {key: command_output(["sysctl", "-n", key]) for key in keys}
        result["cpu"] = result["sysctl"]["machdep.cpu.brand_string"] or result["cpu"]
    elif system == "Linux":
        info = read_text("/proc/cpuinfo") or ""
        names = [line.split(":", 1)[1].strip() for line in info.splitlines()
                 if ":" in line and line.split(":", 1)[0].strip() in ("model name", "Hardware")]
        result["cpu"] = names[0] if names else result["cpu"]
        result["lscpu"] = command_output(["lscpu", "--json"])
        result["numa_policy"] = command_output(["numactl", "--show"])
        result["cgroup"] = read_text("/proc/self/cgroup")
        candidates = [Path("/sys/fs/cgroup")]
        for line in (result["cgroup"] or "").splitlines():
            if line.startswith("0::"):
                relative = Path(line[3:].lstrip("/"))
                if ".." not in relative.parts:
                    current = Path("/sys/fs/cgroup") / relative
                    while current != Path("/sys/fs/cgroup"):
                        candidates.append(current)
                        current = current.parent
        limits = []
        for directory in candidates:
            value = read_text(directory / "cpu.max")
            if value:
                parts = value.split()
                if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit() and int(parts[1]) > 0:
                    limits.append(int(parts[0]) / int(parts[1]))
        # v1 common mounts; deeper/nonnormal mounts remain explicitly unverified.
        quota, period = read_text("/sys/fs/cgroup/cpu/cpu.cfs_quota_us"), read_text("/sys/fs/cgroup/cpu/cpu.cfs_period_us")
        if quota and period and quota.strip().isdigit() and period.strip().isdigit() and int(period) > 0:
            limits.append(int(quota) / int(period))
        result["cgroup_quota_cpus"] = min(limits) if limits else None
        result["quota_detection"] = "best effort; v2 ancestors and common v1 mount"
    limits = [result["logical_cpus"] or 1]
    if result["affinity_cpus"]:
        limits.append(len(result["affinity_cpus"]))
    if result["cgroup_quota_cpus"] is not None:
        limits.append(max(1, math.floor(result["cgroup_quota_cpus"])))
    result["available_cpus"] = min(limits)
    result["cpu_set_restricted"] = bool(result["affinity_cpus"] and len(result["affinity_cpus"]) < (result["logical_cpus"] or 1))
    return result


def check_threads(threads, info, allow_oversubscription=False):
    if any(p > info["available_cpus"] for p in threads) and not allow_oversubscription:
        raise ValueError("Requested workers exceed available CPUs; choose fewer or explicitly --allow-oversubscription")


def clean_environment():
    for key in ("LD_PRELOAD", "DYLD_INSERT_LIBRARIES"):
        if os.environ.get(key):
            raise ValueError(key + " is set; unset it explicitly for uncontaminated calibration")
    prefixes = ("OOX_RAPID_", "OOX_BENCH_", "BENCH_NUM_THREADS", "PARLAY_", "OMP_", "KMP_", "TBB_")
    result = {key: value for key, value in os.environ.items() if not key.startswith(prefixes)}
    result.update(LC_ALL="C", TZ="UTC")
    return result


def cache_values(build):
    result = {}
    for line in (read_text(Path(build) / "CMakeCache.txt", 2**20) or "").splitlines():
        if line.startswith(("#", "//")) or ":" not in line or "=" not in line:
            continue
        key, value = line.split("=", 1)
        result[key.split(":", 1)[0]] = value
    return result


def build_metadata(build):
    cache = cache_values(build)
    if cache.get("CMAKE_BUILD_TYPE") != "Release":
        raise ValueError("Calibration requires a single-config Release build")
    if cache.get("CMAKE_CONFIGURATION_TYPES"):
        raise ValueError("Use a single-config generator (Ninja or Unix Makefiles), not Xcode/Multi-Config")
    for key in ("OOX_SANITIZE", "OOX_SCHEDULER_EVAL_STATS", "OOX_SCHEDULER_EVAL_PAPI"):
        if cache.get(key, "").upper() not in ("", "OFF", "FALSE", "0", "NO"):
            raise ValueError("Calibration requires " + key + " disabled")
    flags = " ".join(v for k, v in cache.items() if k.startswith("CMAKE_CXX_FLAGS"))
    if any(flag in flags for flag in ("-fsanitize", "OOX_SCHEDULER_TRACE", "OOX_EIGEN_ENABLE_STATS")):
        raise ValueError("Instrumented compiler flags are not allowed")
    selected = {k: v for k, v in cache.items() if k.startswith(("CMAKE_CXX_", "CMAKE_BUILD_TYPE", "CMAKE_GENERATOR", "CMAKE_OSX_", "OOX_")) or k in ("TBB_DIR", "CMAKE_HOME_DIRECTORY")}
    compiler = cache.get("CMAKE_CXX_COMPILER")
    return {"cache": selected, "compiler_version": command_output([compiler, "--version"]) if compiler else None,
            "cmake_version": command_output(["cmake", "--version"]),
            "tbb_config_version": read_text(Path(cache["TBB_DIR"]) / "TBBConfigVersion.cmake") if cache.get("TBB_DIR") else None}
