#!/usr/bin/env python3
"""Run the pinned PBBS submodule with historical or OOX Eigen."""

import argparse
import os
from pathlib import Path
import platform
import shutil
import subprocess
import sys

COMMIT = "396a299f03c58dbe9e7604daab38a65781227b75"
DEFAULT_BENCHMARKS = [
    "integerSort/parallelRadixSort",
    "comparisonSort/sampleSort",
    "removeDuplicates/parlayhash",
    "histogram/parallel",
    "breadthFirstSearch/backForwardBFS",
    "maximalIndependentSet/incrementalMIS",
]
BACKENDS = ["reference", "oox"]
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


def git_file(source: Path, path: str) -> bytes:
    return subprocess.check_output(["git", "show", f"{COMMIT}:{path}"], cwd=source)


def restore_reference_file(source: Path, path: str):
    destination = source / path
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(git_file(source, path))


def restore_reference_tree(source: Path, prefix: str):
    paths = subprocess.check_output(
        ["git", "ls-tree", "-r", "--name-only", COMMIT, prefix],
        cwd=source,
        text=True,
    ).splitlines()
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
        plugin.write_text(adapter_text())
        initialization = source / (
            "parlaylib/include/parlay/internal/scheduler_plugins/common/initialization.h"
        )
        initialization.write_text(initialization_text())


def validate_source(source: Path):
    if not (source / ".git").exists():
        raise RuntimeError(
            "PBBS submodule is not initialized; run: "
            "git submodule update --init thirdparty/pbbsbench"
        )
    actual = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=source, text=True
    ).strip()
    if actual != COMMIT:
        raise RuntimeError(
            f"{source} is at {actual}; expected pinned eigen-mailbox commit {COMMIT}"
        )


def configure_checkout(source: Path, root: Path, compiler: str):
    runner = source / "common/runTests.py"
    runner_text = subprocess.check_output(
        ["git", "show", f"{COMMIT}:common/runTests.py"], cwd=source, text=True
    )
    runner_text = runner_text.replace(
        "if (len(err) > 0):", "if process.returncode != 0:"
    )
    runner.write_text(runner_text)

    if platform.system() == "Darwin":
        sdk = subprocess.check_output(["xcrun", "--show-sdk-path"], text=True).strip()
        compiler = f"{compiler} -isysroot {sdk}"

    defs = source / "common/parallelDefs"
    text = subprocess.check_output(
        ["git", "show", f"{COMMIT}:common/parallelDefs"], cwd=source, text=True
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


def restore_checkout(source: Path):
    restore_reference_tree(
        source, "parlaylib/include/parlay/internal/scheduler_plugins/eigen"
    )
    for path in [
        "parlaylib/include/parlay/internal/scheduler_plugins/eigen.h",
        "parlaylib/include/parlay/internal/scheduler_plugins/common/initialization.h",
        "common/runTests.py",
        "common/parallelDefs",
    ]:
        restore_reference_file(source, path)


def parse_args():
    root = Path(__file__).resolve().parents[2]
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path,
                        default=root / "thirdparty/pbbsbench")
    parser.add_argument("--output", type=Path,
                        default=root / "cmake-build-pbbs/results")
    parser.add_argument("--compiler", default=shutil.which("clang++") or "c++")
    parser.add_argument("--threads", type=int, default=os.cpu_count() or 1)
    parser.add_argument("--backend", action="append", choices=BACKENDS,
                        help="Backend to run; defaults to OOX")
    parser.add_argument("--mode", action="append",
                        help="Mode for a single selected backend")
    parser.add_argument("--benchmark", action="append")
    parser.add_argument("--all-benchmarks", action="store_true",
                        help="Run PBBS's complete default application suite")
    parser.add_argument("--full", action="store_true",
                        help="Use full PBBS inputs; the default uses testInputs_small")
    parser.add_argument("--prepare-only", action="store_true")
    return parser.parse_args(), root


def main():
    args, root = parse_args()
    source = args.source.resolve()
    validate_source(source)
    if args.prepare_only:
        print(f"PBBS submodule is ready at {source}")
        return

    try:
        configure_checkout(source, root, args.compiler)
        args.output.mkdir(parents=True, exist_ok=True)
        backends = args.backend or ["oox"]
        if args.mode and len(backends) != 1:
            raise ValueError("--mode requires exactly one --backend")
        if args.all_benchmarks and args.benchmark:
            raise ValueError("--all-benchmarks cannot be combined with --benchmark")
        benchmarks = None if args.all_benchmarks else (args.benchmark or DEFAULT_BENCHMARKS)
        for backend in backends:
            select_backend(source, backend)
            supported = REFERENCE_MODES if backend == "reference" else OOX_MODES
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
                command = [sys.executable, "runall", "-force", "-par", "-nonuma"]
                if benchmarks:
                    command.extend(["-only", *benchmarks, "-ext"])
                if not args.full:
                    command.append("-small")
                output = args.output / f"{backend}_{mode}.txt"
                print(f"Writing {output}", flush=True)
                with output.open("w") as stream:
                    subprocess.run(command, cwd=source, env=env, stdout=stream,
                                   stderr=subprocess.STDOUT, check=True)
                log = output.read_text(errors="replace")
                if "TEST TERMINATED ABNORMALLY" in log or "make: ***" in log:
                    raise RuntimeError(f"PBBS reported a failure; inspect {output}")
    finally:
        restore_checkout(source)


if __name__ == "__main__":
    main()
