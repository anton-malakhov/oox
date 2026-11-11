#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Clone, build, validate and benchmark the scheduler on the local machine."""
import argparse
import datetime
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import random
import shlex
import shutil
import signal
import statistics
import subprocess
import sys
import tarfile
import tempfile


MODES = ["RAPID_AUTO", "RAPID_GROUP", "EIGEN_AUTO", "EIGEN_AUTO_RESIDENT",
         "EIGEN_STATIC", "TBB_AUTO", "TBB_STATIC", "RAPID_START"]
FOCUSED = (r"^NestedFiveSkew|^Launch/(64|4096|262144)/|^SpmvBenchmark.*/4096/|"
           r"^VariableCost.*(Constant|Clustered|Pareto).*/4096/|^Matrix")


def positive(value):
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError("must be positive")
    return number


def arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", default="https://github.com/anton-malakhov/oox.git")
    parser.add_argument("--ref", default="kkalpha-pr37-on-pr35")
    parser.add_argument("--workdir", type=Path, default=Path("scheduler-runs"))
    parser.add_argument("--threads", nargs="+", type=positive, default=[4, 8])
    parser.add_argument("--mode", nargs="+", choices=MODES, default=MODES)
    parser.add_argument("--repetitions", type=positive, default=5)
    parser.add_argument("--jobs", type=positive, default=4)
    parser.add_argument("--seed", type=int, default=20261104)
    parser.add_argument("--min-time", default="0.1s")
    parser.add_argument("--timeout", type=positive, default=900,
                        help="seconds per correctness or benchmark process")
    filters = parser.add_mutually_exclusive_group()
    filters.add_argument("--filter", default=FOCUSED)
    filters.add_argument("--full", action="store_true", help="run every registered case")
    parser.add_argument("--install-deps", action="store_true",
                        help="install Debian/Ubuntu build dependencies using apt-get")
    parser.add_argument("--resident", choices=["fixed", "auto"], default="fixed")
    parser.add_argument("--cpu-set", help="Linux CPU list passed to taskset, e.g. 0-7")
    parser.add_argument("--skip-nested-lazy", action="store_true")
    return parser.parse_args()


def run(command, log, env=None, timeout=3600):
    command = list(map(str, command))
    print("+ " + shlex.join(command), flush=True)
    with log.open("w") as output:
        output.write(shlex.join(command) + "\n")
        output.flush()
        process = subprocess.Popen(command, stdout=output, stderr=subprocess.STDOUT,
                                   env=env, start_new_session=True)
        try:
            status = process.wait(timeout=timeout)
        except BaseException:
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGKILL)
            process.wait()
            raise
    if status:
        raise RuntimeError(f"exit {status}; see {log}")


def capture(command):
    return subprocess.check_output(command, text=True, stderr=subprocess.STDOUT).strip()


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def benchmark_records(path, threads, variant, mode):
    rows = json.loads(path.read_text())["benchmarks"]
    if not rows or any(row.get("error_occurred") for row in rows):
        raise ValueError(f"empty or failed benchmark results in {path}")
    records = []
    names = set()
    units = {"ns": 1e-9, "us": 1e-6, "ms": 1e-3, "s": 1}
    for row in rows:
        if row.get("run_type", "iteration") != "iteration":
            continue
        seconds = row["real_time"] * units[row["time_unit"]]
        if not math.isfinite(seconds) or seconds <= 0 or row["name"] in names:
            raise ValueError(f"invalid or duplicate timing in {path}: {row['name']}")
        names.add(row["name"])
        records.append(dict(threads=threads, variant=variant, mode=mode,
                            name=row["name"], seconds=seconds))
    if not records:
        raise ValueError(f"no iteration measurements in {path}")
    return records


def execute(args, directory, metadata):
    logs = directory / "logs"
    logs.mkdir()
    if args.install_deps:
        if platform.system() != "Linux" or not shutil.which("apt-get"):
            raise RuntimeError("--install-deps requires Debian/Ubuntu with apt-get")
        prefix = [] if os.geteuid() == 0 else ["sudo"]
        run(prefix + ["apt-get", "update"], logs / "apt-update.log")
        run(prefix + ["apt-get", "install", "-y", "build-essential", "cmake",
                      "ninja-build", "git", "ca-certificates", "libgtest-dev",
                      "libeigen3-dev"], logs / "apt-install.log")
    for tool in ["git", "cmake", "ninja", os.environ.get("CXX", "c++")]:
        if not shutil.which(tool):
            raise RuntimeError(f"missing {tool}; install build dependencies or use --install-deps")
    placement = []
    if args.cpu_set:
        if platform.system() != "Linux" or not shutil.which("taskset"):
            raise RuntimeError("--cpu-set requires Linux taskset")
        placement = ["taskset", "--cpu-list", args.cpu_set]
        run(placement + ["true"], logs / "cpu-set.log")
    metadata["compiler"] = capture([os.environ.get("CXX", "c++"), "--version"])
    metadata["cmake"] = capture(["cmake", "--version"])
    if shutil.which("lscpu"):
        run(["lscpu", "--json"], logs / "lscpu.log")
    if hasattr(os, "sched_getaffinity"):
        metadata["inherited_cpu_affinity"] = sorted(os.sched_getaffinity(0))
    for name in ["cpu.max", "cpuset.cpus.effective"]:
        path = Path("/sys/fs/cgroup") / name
        if path.is_file():
            metadata[name] = path.read_text().strip()
    source = directory / "source"
    run(["git", "clone", "--no-checkout", args.repo, source], logs / "clone.log")
    revision = None
    for candidate in ["refs/remotes/origin/" + args.ref, args.ref]:
        try:
            revision = capture(["git", "-C", str(source), "rev-parse", "--verify",
                                candidate + "^{commit}"])
            break
        except subprocess.CalledProcessError:
            pass
    if revision is None:
        run(["git", "-C", source, "fetch", "origin", args.ref], logs / "fetch.log")
        revision = "FETCH_HEAD"
    run(["git", "-C", source, "checkout", "--detach", revision], logs / "checkout.log")
    metadata["commit"] = capture(["git", "-C", str(source), "rev-parse", "HEAD"])
    run(["git", "-C", source, "submodule", "update", "--init", "thirdparty/oneTBB"],
        logs / "submodule.log")
    metadata["submodules"] = capture(["git", "-C", str(source), "submodule", "status"])
    variants = {"default": list(dict.fromkeys(args.mode))}
    if not args.skip_nested_lazy and "RAPID_AUTO" in args.mode:
        variants["nested-lazy"] = ["RAPID_AUTO"]
    binaries = {}
    for variant, modes in variants.items():
        build = directory / ("build-" + variant)
        options = ["-DOOX_BUILD_TESTS=OFF", "-DOOX_BUILD_EXAMPLES=OFF",
                   "-DOOX_BUILD_BENCHMARKS=ON", "-DOOX_BUILD_TASKBENCH=OFF",
                   "-DOOX_BUILD_SCHEDULER_EVALS=ON", "-DOOX_ENABLE_EIGEN=ON",
                   "-DOOX_ENABLE_TBB=ON", "-DOOX_LOCAL_TBB=ON",
                   "-DOOX_ENABLE_TF=OFF", "-DOOX_ENABLE_FOLLY=OFF",
                   "-DOOX_ENABLE_OMP=OFF", "-DOOX_SCHEDULER_EVAL_STATS=OFF"]
        if variant == "nested-lazy":
            flags = os.environ.get("CXXFLAGS", "")
            options.append(f"-DCMAKE_CXX_FLAGS={flags} -DOOX_RAPID_EXPERIMENT_NESTED_LAZY=1")
        run(["cmake", "-S", source, "-B", build, "-G", "Ninja",
             "-DCMAKE_BUILD_TYPE=Release"] + options, logs / (variant + "-configure.log"))
        targets = [prefix + mode for mode in modes
                   for prefix in ["bench_scheduler_eval_", "test_scheduler_eval_"]]
        targets += ["test_scheduler_eval_nested_" + mode for mode in modes if mode != "RAPID_START"]
        run(["cmake", "--build", build, "--parallel", args.jobs, "--target"] + targets,
            logs / (variant + "-build.log"))
        shutil.copy2(build / "CMakeCache.txt", logs / (variant + "-CMakeCache.txt"))
        for mode in modes:
            binary = build / "benchmarks/scheduler_eval" / ("bench_scheduler_eval_" + mode)
            binaries[variant, mode] = binary
    metadata["binaries"] = {f"{v}/{m}": digest(p) for (v, m), p in binaries.items()}
    env = dict(os.environ)
    # Inherited research knobs must not silently change the selected campaign.
    for key in list(env):
        if key.startswith(("OOX_", "BENCH_", "PARLAY_")):
            env.pop(key)
    env.update(OMP_PROC_BIND="false", KMP_AFFINITY="disabled")
    env.pop("GOMP_CPU_AFFINITY", None)
    env.pop("KMP_HW_SUBSET", None)
    metadata["checks"] = []
    records = []
    rng = random.Random(args.seed)
    for threads in dict.fromkeys(args.threads):
        env.update(BENCH_NUM_THREADS=str(threads), OMP_NUM_THREADS=str(threads))
        if args.resident == "fixed":
            env["OOX_RAPID_RESIDENT_LIMIT"] = str(threads)
        for (variant, mode), binary in binaries.items():
            prefixes = ["test_scheduler_eval_"]
            if mode != "RAPID_START":
                prefixes.append("test_scheduler_eval_nested_")
            for prefix in prefixes:
                label = f"{threads}t-{variant}-{prefix}{mode}"
                run(placement + [binary.parent / (prefix + mode)], logs / (label + ".log"),
                    env, args.timeout)
                metadata["checks"].append(label)
        for repeat in range(args.repetitions):
            order = list(binaries)
            rng.shuffle(order)
            for variant, mode in order:
                label = f"{threads}t-{repeat}-{variant}-{mode}"
                output = directory / (label + ".json")
                command = [binaries[variant, mode], "--benchmark_out_format=json",
                           f"--benchmark_out={output}", f"--benchmark_min_time={args.min_time}"]
                if not args.full:
                    command.append(f"--benchmark_filter={args.filter}")
                run(placement + command, logs / (label + ".log"), env, args.timeout)
                records.extend(benchmark_records(output, threads, variant, mode))
    if metadata["binaries"] != {f"{v}/{m}": digest(p) for (v, m), p in binaries.items()}:
        raise RuntimeError("benchmark binaries changed during the campaign")
    groups = {}
    for record in records:
        key = tuple(record[k] for k in ["threads", "variant", "mode", "name"])
        groups.setdefault(key, []).append(record["seconds"])
    summary = [dict(zip(["threads", "variant", "mode", "name"], key),
                    median_seconds=statistics.median(values), samples_seconds=values)
               for key, values in sorted(groups.items())]
    (directory / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")


def main():
    args = arguments()
    args.workdir = args.workdir.expanduser().resolve()
    args.workdir.mkdir(parents=True, exist_ok=True)
    directory = Path(tempfile.mkdtemp(prefix="scheduler-", dir=args.workdir))
    metadata = dict(complete=False, started_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                    platform=platform.platform(), machine=platform.machine(),
                    python=sys.version, logical_cpus=os.cpu_count(), runner_sha256=digest(Path(__file__)),
                    options={**vars(args), "workdir": str(args.workdir)})
    (directory / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
    shutil.copy2(__file__, directory / "runner.py")
    status = 1
    print(f"Results: {directory}", flush=True)
    try:
        execute(args, directory, metadata)
        metadata["complete"] = True
        status = 0
    except (Exception, KeyboardInterrupt) as error:
        metadata["error"] = str(error) or type(error).__name__
        print(metadata["error"], file=sys.stderr)
    finally:
        (directory / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
        archive = directory.with_suffix(".tar.gz")
        with tarfile.open(archive, "w:gz") as output:
            for path in sorted(directory.iterdir()):
                if path.name in ("logs", "runner.py") or path.suffix == ".json":
                    output.add(path, arcname=directory.name + "/" + path.name)
        print(f"Archive: {archive}", flush=True)
    return status


if __name__ == "__main__":
    sys.exit(main())
