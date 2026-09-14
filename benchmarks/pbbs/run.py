#!/usr/bin/env python3
"""Run the in-tree PBBS snapshot in an isolated build directory."""

import argparse
import ast
import os
from pathlib import Path
import platform
import shutil
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
import snapshot

COMMIT = "396a299f03c58dbe9e7604daab38a65781227b75"
DEFAULT_BENCHMARKS = [
    "breadthFirstSearch/backForwardBFS",
    "convexHull/quickHull",
    "removeDuplicates/parlayhash",
    "integerSort/parallelRadixSort",
    "comparisonSort/sampleSort",
    "suffixArray/parallelRange",
    "nearestNeighbors/octTree",
    "delaunayTriangulation/incrementalDelaunay",
    "delaunayRefine/incrementalRefine",
    "rayCast/kdTree",
    "minSpanningForest/parallelFilterKruskal",
    "spanningForest/ndST",
]
CI_SMOKE_BENCHMARKS = [
    "removeDuplicates/parlayhash",
    "integerSort/parallelRadixSort",
    "comparisonSort/sampleSort",
    "suffixArray/parallelRange",
    "nearestNeighbors/octTree",
    "delaunayTriangulation/incrementalDelaunay",
    "delaunayRefine/incrementalRefine",
    "rayCast/kdTree",
    "minSpanningForest/parallelFilterKruskal",
    "spanningForest/ndST",
]
SERIAL_BENCHMARKS = [
    "breadthFirstSearch/serialBFS", "convexHull/serialHull",
    "removeDuplicates/serial_hash", "integerSort/serialRadixSort",
    "comparisonSort/serialSort", "suffixArray/serialDivsufsort",
    "minSpanningForest/serialMST", "spanningForest/serialST",
]
SERIAL_SMOKE_BENCHMARKS = SERIAL_BENCHMARKS[2:]
BACKENDS = ["reference", "oox", "oox-tasks", "serial"]
REFERENCE_MODES = [
    "EIGEN_SIMPLE",
    "EIGEN_TIMESPAN",
    "EIGEN_TIMESPAN_GRAINSIZE",
    "EIGEN_STATIC",
    "EIGEN_RAPID",
]
OOX_MODES = [
    "EIGEN_STEALING",
    "EIGEN_SHARING",
    "EIGEN_STEALING_GRAINSIZE",
    "EIGEN_SHARING_STEALING",
]


def log_tail(path: Path, line_count: int = 100) -> str:
    lines = path.read_text(errors="replace").splitlines()
    return "\n".join(lines[-line_count:])


def failure(message: str) -> RuntimeError:
    if os.environ.get("GITHUB_ACTIONS") == "true":
        escaped = message.replace("%", "%25")
        escaped = escaped.replace("\r", "%0D").replace("\n", "%0A")
        print(f"::error title=PBBS application failure::{escaped}", file=sys.stderr)
    return RuntimeError(message)


def adapter_text():
    return r'''#ifndef PARLAY_INTERNAL_SCHEDULER_PLUGINS_EIGEN_H_
#define PARLAY_INTERNAL_SCHEDULER_PLUGINS_EIGEN_H_
#include <benchmarks/eigen/parallel_for.h>
namespace parlay {
inline size_t num_workers() { return static_cast<size_t>(GetNumThreads()); }
inline size_t worker_id() { return static_cast<size_t>(GetThreadIndex()); }
template <typename F>
inline void parallel_for(size_t first, size_t last, F&& f, long grain, bool) {
  ParallelFor(first, last, std::forward<F>(f), grain > 0 ? grain : 1);
}
template <typename L, typename R>
inline void par_do(L&& left, R&& right, bool) {
  EigenPartitioner::ParallelDo(std::forward<L>(left), std::forward<R>(right));
}
inline void init_plugin_internal() { InitParallel(GetNumThreads()); }
template <typename... Fs> void execute_with_scheduler(Fs...) {
  struct unsupported;
  static_assert((std::is_same_v<unsupported, Fs> && ...),
                "execute_with_scheduler is unavailable for Eigen");
}
}  // namespace parlay
#endif
'''


def initialization_text():
    return r'''#pragma once
#include <benchmarks/eigen/poor_barrier.h>
namespace parlay::internal {
struct InitOnce {
  template <typename F> InitOnce(F&& f) { f(); }
};
using SpinBarrier = ::SpinBarrier;
}  // namespace parlay::internal
'''


def task_adapter_text():
    return r'''#ifndef PARLAY_INTERNAL_SCHEDULER_PLUGINS_EIGEN_H_
#define PARLAY_INTERNAL_SCHEDULER_PLUGINS_EIGEN_H_
#include <benchmarks/scheduler_eval/runtime/oox_task_adapter.h>
#ifndef OOX_PBBS_TASK_GRAIN
#define OOX_PBBS_TASK_GRAIN 1024
#endif
namespace parlay {
// PBBS's main thread also uses worker-specific scratch storage.
inline size_t num_workers() { return static_cast<size_t>(GetNumThreads()) + 1; }
inline size_t worker_id() {
  const auto id = GetThreadIndex();
  return id < 0 ? num_workers() - 1 : static_cast<size_t>(id);
}
template <typename F>
inline void parallel_for(size_t first, size_t last, F&& f, long grain, bool) {
  ParallelFor(first, last, std::forward<F>(f),
              grain > 0 ? grain : OOX_PBBS_TASK_GRAIN);
}
template <typename L, typename R>
inline void par_do(L&& left, R&& right, bool) {
  auto task = oox::run([&] { left(); return 0; });
  right();
  static_cast<void>(oox::wait_and_get(task));
}
inline void init_plugin_internal() { InitParallel(GetNumThreads()); }
template <typename... Fs> void execute_with_scheduler(Fs...) {
  struct unsupported;
  static_assert((std::is_same_v<unsupported, Fs> && ...),
                "execute_with_scheduler is unavailable for OOX tasks");
}
}
#endif
'''


def upstream_file(source: Path, path: str) -> bytes:
    # Read pristine bytes even when source is a patched build copy.
    return (snapshot.VENDOR / path).read_bytes()


def restore_reference_file(source: Path, path: str):
    if source.resolve() == snapshot.VENDOR.resolve():
        raise ValueError("cannot patch the checked-in PBBS snapshot")
    destination = source / path
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(upstream_file(source, path))


def restore_reference_tree(source: Path, prefix: str):
    paths = [path for path in snapshot.manifest()["files"]
             if path == prefix or path.startswith(prefix + "/")]
    for path in paths:
        restore_reference_file(source, path)


def apply_reference_portability(source: Path):
    """Keep the historical scheduler intact while disabling Linux-only pinning."""
    util = source / "parlaylib/include/parlay/internal/scheduler_plugins/eigen/util.h"
    text = util.read_text()
    text = text.replace("#include <sched.h>", "#if defined(__linux__)\n#include <sched.h>\n#endif")
    text = text.replace(
        "inline void PinThread(size_t slot_number) {\n  cpu_set_t mask;",
        "inline void PinThread(size_t slot_number) {\n#if defined(__linux__)\n  cpu_set_t mask;",
    )
    text = text.replace(
        "  }\n}\n\nnamespace detail {",
        "  }\n#else\n  (void)slot_number;\n#endif\n}\n\nnamespace detail {",
        1,
    )
    util.write_text(text)


def select_backend(source: Path, backend: str):
    parallel_path = "parlaylib/include/parlay/parallel.h"
    restore_reference_file(source, parallel_path)
    if backend == "serial":
        plugin = source / "parlaylib/include/parlay/internal/scheduler_plugins/eigen.h"
        plugin.write_text('#pragma once\n#include "sequential.h"\n')
        initialization = source / "parlaylib/include/parlay/internal/scheduler_plugins/common/initialization.h"
        initialization.write_text(
            '#pragma once\n#include <atomic>\n#include <cstddef>\n'
            'namespace parlay::internal {\n'
            'struct InitOnce { template <typename F> InitOnce(F&& f) { f(); } };\n'
            'struct SpinBarrier { std::atomic<size_t> remaining;\n'
            'explicit SpinBarrier(size_t n) : remaining(n) {}\n'
            'void Notify() { --remaining; }\n'
            'void Wait() { while (remaining.load()) {} } };\n}\n')
        return
    if backend == "reference":
        restore_reference_tree(
            source, "parlaylib/include/parlay/internal/scheduler_plugins/eigen"
        )
        restore_reference_file(
            source, "parlaylib/include/parlay/internal/scheduler_plugins/eigen.h"
        )
        restore_reference_file(
            source,
            "parlaylib/include/parlay/internal/scheduler_plugins/common/initialization.h",
        )
        if platform.system() != "Linux":
            apply_reference_portability(source)
    else:
        plugin = source / "parlaylib/include/parlay/internal/scheduler_plugins/eigen.h"
        plugin.write_text(task_adapter_text() if backend == "oox-tasks"
                          else adapter_text())
        initialization = source / (
            "parlaylib/include/parlay/internal/scheduler_plugins/common/initialization.h"
        )
        if backend == "oox-tasks":
            parallel = source / parallel_path
            parallel.write_text(parallel.read_text().replace(
                "  static internal::InitOnce warmup{[] { internal::warmup(num_workers()); }};",
                "  // The OOX adapter initializes its own pool without a slot barrier.",
            ))
            initialization.write_text(
                '#pragma once\n#include <benchmarks/scheduler_eval/runtime/oox_task_adapter.h>\n'
                'namespace parlay::internal {\n'
                'struct InitOnce { template <typename F> InitOnce(F&& f) { f(); } };\n'
                'struct SpinBarrier { std::atomic<size_t> remaining;\n'
                'explicit SpinBarrier(size_t n) : remaining(n) {}\n'
                'void Notify() { --remaining; }\n'
                'void Wait() { while (remaining.load()) CpuRelax(); } };\n}\n')
        else:
            initialization.write_text(initialization_text())


def validate_source(source: Path):
    snapshot.validate(source)
    if snapshot.manifest()["revision"] != COMMIT:
        raise ValueError("unexpected PBBS snapshot revision")


def configure_checkout(source: Path, root: Path, compiler: str):
    if source.resolve() == snapshot.VENDOR.resolve():
        raise ValueError("cannot configure the checked-in PBBS snapshot")
    runall = source / "runall"
    runall_text = upstream_file(source, "runall").decode()
    runall_text = runall_text.replace(
        '    # ["nearestNeighbors/octTree",True,0],',
        '    ["nearestNeighbors/octTree",True,0],',
    )
    runall_text = runall_text.replace("int(maxcpus / 2)", "max(1, int(maxcpus / 2))")
    runall_text = runall_text.replace(
        "    os.system(ss)",
        '    if os.system(ss):\n        raise NameError("compilation failed: " + ss)',
    )
    runall_text = runall_text.replace(
        "    if (procs==1) : rounds = 1",
        "    if \"PBBS_ROUNDS\" in os.environ : "
        "rounds = int(os.environ[\"PBBS_ROUNDS\"])\n"
        "    elif (procs==1) : rounds = 1",
    )
    runall.write_text(runall_text)

    neighbors = source / "benchmarks/nearestNeighbors/octTree/neighbors.h"
    neighbors_text = upstream_file(
        source, "benchmarks/nearestNeighbors/octTree/neighbors.h"
    ).decode()
    neighbors_text = neighbors_text.replace(
        "size_t n2 = 2000000;", "size_t n2 = 0;"
    )
    neighbors_text = neighbors_text.replace(
        "bool report_stats = true;", "bool report_stats = false;"
    )
    neighbors_text = neighbors_text.replace(
        "int algorithm_version = 0;", "int algorithm_version = 2;"
    )
    neighbors_text = neighbors_text.replace(
        "knn_tree T(v1, whole_box);", "knn_tree T(v, whole_box);"
    )
    neighbors_text = neighbors_text.replace(
        "    T.batch_insert(v2, root, bd.first, bd.second);",
        "    // Static nearest-neighbor benchmark: no dynamic insertion phase.",
    )
    neighbors_text = neighbors_text.replace(
        "    knn_tree R(v, whole_box);",
        "    // The PBBS checker validates the query results.",
    )
    neighbors_text = neighbors_text.replace("    T.are_equal(R.tree.get(), dims);", "")
    neighbors.write_text(neighbors_text)

    dedup_time = source / "benchmarks/removeDuplicates/bench/dedupTime.C"
    dedup_text = upstream_file(
        source, "benchmarks/removeDuplicates/bench/dedupTime.C"
    ).decode()
    dedup_text = dedup_text.replace(
        "  return 1;\n}\n\nint main", "  return 0;\n}\n\nint main", 1
    )
    dedup_time.write_text(dedup_text)

    suffix_check = source / "benchmarks/suffixArray/bench/SACheck.C"
    suffix_text = upstream_file(
        source, "benchmarks/suffixArray/bench/SACheck.C"
    ).decode()
    marker = "  return 0;\n}\n\nint main"
    suffix_text = suffix_text.replace(marker, "  return 1;\n}\n\nint main", 1)
    suffix_check.write_text(suffix_text)

    runner = source / "common/runTests.py"
    runner_text = upstream_file(source, "common/runTests.py").decode()
    runner_text = runner_text.replace(
        "if (len(err) > 0):", "if process.returncode != 0:"
    )
    runner.write_text(runner_text)

    if platform.system() == "Darwin":
        sdk = subprocess.check_output(["xcrun", "--show-sdk-path"], text=True).strip()
        compiler = f"{compiler} -isysroot {sdk}"

    defs = source / "common/parallelDefs"
    text = upstream_file(source, "common/parallelDefs").decode()
    text = text.replace(
        "CCFLAGS = -O2 -g -std=c++17", "CCFLAGS = -O2 -g -std=c++20", 1
    )
    text = text.replace(
        "EIGENFLAGS = -DPARLAY_EIGEN",
        f"EIGENFLAGS = -I {root} -DPARLAY_EIGEN",
    )
    eigen_block = "else ifdef EIGEN\nCC = g++"
    text = text.replace(eigen_block, f"else ifdef EIGEN\nCC = {compiler}")
    if platform.system() == "Darwin":
        text = text.replace("CLFLAGS = -ldl $(JEMALLOC)", "CLFLAGS = $(JEMALLOC)")
        text = text.replace(
            "EIGENFLAGS =",
            "EIGENFLAGS = -D_LIBCPP_ENABLE_CXX17_REMOVED_UNARY_BINARY_FUNCTION",
        )
    defs.write_text(text)
    sequential = upstream_file(source, "common/seqDefs").decode()
    sequential = sequential.replace("CC = g++", f"CC = {compiler}")
    sequential = sequential.replace("-std=c++17", "-std=c++20")
    if platform.machine() not in ("x86_64", "AMD64"):
        sequential = sequential.replace("-mcx16 ", "")
    if platform.system() == "Darwin":
        sequential = sequential.replace("CCFLAGS =", "CCFLAGS = -D_LIBCPP_ENABLE_CXX17_REMOVED_UNARY_BINARY_FUNCTION")
    (source / "common/seqDefs").write_text(sequential)


def parse_args():
    root = Path(__file__).resolve().parents[2]
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path,
                        default=snapshot.VENDOR)
    parser.add_argument("--archives", type=Path,
                        default=root / "results/pbbs-archives",
                        help="checksum-verified original data archives, prepared separately")
    parser.add_argument("--output", type=Path,
                        default=root / "cmake-build-pbbs/results")
    parser.add_argument("--compiler", default=shutil.which("clang++") or "c++")
    parser.add_argument("--threads", type=int, default=os.cpu_count() or 1)
    parser.add_argument("--task-grain", type=int, default=1024,
                        help="OOX task leaf size when PBBS supplies no grain")
    parser.add_argument("--backend", action="append", choices=BACKENDS,
                        help="Backend to run; defaults to OOX")
    parser.add_argument("--mode", action="append",
                        help="Mode for a single selected backend")
    parser.add_argument("--benchmark", action="append")
    parser.add_argument("--all-benchmarks", action="store_true",
                        help="Run all vendored implementations for the selected backend")
    parser.add_argument("--ci-smoke", action="store_true",
                        help="Run the bounded checker-backed CI workload set")
    parser.add_argument("--full", action="store_true",
                        help="Use full PBBS inputs; the default uses testInputs_small")
    parser.add_argument("--prepare-only", action="store_true")
    parser.add_argument("--compile-only", action="store_true",
                        help="Compile the selected suite without generating inputs")
    parser.add_argument("--numa", action="store_true",
                        help="Use PBBS's numactl interleave policy on Linux")
    parser.add_argument("--timeout", type=int,
                        help="Maximum seconds allowed for one PBBS suite run")
    return parser.parse_args(), root


def main():
    args, root = parse_args()
    if args.threads <= 0 or args.task_grain <= 0:
        raise ValueError("threads and task-grain must be positive")
    source = args.source.resolve()
    validate_source(source)
    if args.prepare_only:
        print(f"PBBS snapshot verified at {source}")
        return

    source = snapshot.build_copy(source, args.output, args.archives,
                                 require_archives=args.ci_smoke)
    print(f"PBBS build copy: {source}", flush=True)
    try:
        compiler = args.compiler
        if "oox-tasks" in (args.backend or []):
            compiler += f" -DHAVE_EIGEN=1 -DOOX_EIGEN_NUM_THREADS={args.threads}"
            compiler += f" -DOOX_PBBS_TASK_GRAIN={args.task_grain}"
        configure_checkout(source, root, compiler)
        registered = set()
        for statement in ast.parse((source / "runall").read_text()).body:
            if isinstance(statement, ast.Assign) and any(
                    isinstance(target, ast.Name) and target.id == "tests"
                    for target in statement.targets):
                registered = {entry[0] for entry in ast.literal_eval(statement.value)}
                break
        registered &= set(snapshot.manifest()["applications"])
        unknown_benchmarks = set(args.benchmark or []) - registered
        if unknown_benchmarks:
            raise ValueError(f"unregistered PBBS benchmark(s): {sorted(unknown_benchmarks)}")
        args.output.mkdir(parents=True, exist_ok=True)
        backends = args.backend or ["oox"]
        if args.mode and len(backends) != 1:
            raise ValueError("--mode requires exactly one --backend")
        selectors = sum(bool(value) for value in
                        (args.all_benchmarks, args.ci_smoke, args.benchmark))
        if selectors > 1:
            raise ValueError("choose only one benchmark selection option")
        if args.ci_smoke and (args.full or args.compile_only):
            raise ValueError("--ci-smoke requires executable small inputs")
        if args.timeout is not None and args.timeout <= 0:
            raise ValueError("--timeout must be positive")
        if args.all_benchmarks:
            benchmarks = DEFAULT_BENCHMARKS
        elif args.ci_smoke:
            benchmarks = CI_SMOKE_BENCHMARKS
        else:
            benchmarks = args.benchmark or DEFAULT_BENCHMARKS
        for backend in backends:
            select_backend(source, backend)
            supported = (REFERENCE_MODES if backend == "reference" else
                         ["SERIAL"] if backend == "serial" else
                         ["OOX_TASKS"] if backend == "oox-tasks" else OOX_MODES)
            modes = args.mode or supported
            unknown = set(modes) - set(supported)
            if unknown:
                raise ValueError(f"unsupported {backend} mode(s): {sorted(unknown)}")
            for mode in modes:
                env = os.environ.copy()
                env.update({
                    "EIGEN": "1",
                    "EIGEN_MODE": mode,
                    "BENCH_NUM_THREADS": str(args.threads),
                })
                if backend == "serial":
                    env.update(BENCH_NUM_THREADS="1", PARLAY_NUM_THREADS="1", OMP_NUM_THREADS="1")
                if args.ci_smoke:
                    env["PBBS_ROUNDS"] = "1"
                phases = [(benchmarks, args.compile_only, True, "")]
                if backend == "serial" and not args.benchmark:
                    phases = [(SERIAL_BENCHMARKS, args.compile_only, True, "")]
                if args.ci_smoke:
                    full_suite = SERIAL_BENCHMARKS if backend == "serial" else DEFAULT_BENCHMARKS
                    smoke_suite = SERIAL_SMOKE_BENCHMARKS if backend == "serial" else benchmarks
                    phases = [(full_suite, True, True, "_compile"),
                              (smoke_suite, False, False, "")]
                for selected, compile_only, force_compile, suffix in phases:
                    command = [sys.executable, "runall"]
                    if backend != "serial":
                        command.append("-par")
                    if force_compile:
                        command.append("-force")
                    if not args.numa:
                        command.append("-nonuma")
                    if compile_only:
                        command.append("-notime")
                    if selected:
                        command.extend(["-only", *selected, "-ext"])
                    if not args.full:
                        command.append("-small")
                    output = args.output / f"{backend}_{mode}{suffix}.txt"
                    print(f"Writing {output}", flush=True)
                    try:
                        with output.open("w") as stream:
                            subprocess.run(
                                command, cwd=source, env=env, stdout=stream,
                                stderr=subprocess.STDOUT, check=True,
                                timeout=args.timeout)
                    except subprocess.TimeoutExpired as error:
                        raise failure(
                            f"PBBS timed out after {args.timeout}s. Log tail:\n"
                            f"{log_tail(output)}"
                        ) from error
                    except subprocess.CalledProcessError as error:
                        raise failure(
                            f"PBBS exited with status {error.returncode}. "
                            f"Log tail:\n{log_tail(output)}"
                        ) from error
                    log = output.read_text(errors="replace")
                    if ("TEST TERMINATED ABNORMALLY" in log or
                            "make: ***" in log):
                        raise failure(
                            f"PBBS reported a failure. Log tail:\n"
                            f"{log_tail(output)}"
                        )
    finally:
        # Keep the isolated copy and logs for inspection; never rewrite vendor/.
        validate_source(snapshot.VENDOR)


if __name__ == "__main__":
    main()
