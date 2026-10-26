# Scheduler correctness reproduction in Linux

This runner builds the shared scheduler-eval correctness workloads, not performance
benchmarks. It tests serial elision, TBB Simple/Auto, Eigen Auto/resident/stealing,
Rapid Group and Rapid Auto. It also tests Rapid Auto with a fixed resident limit.
Each test stops at its first failing repeat; the runner still checks other modes
and exits nonzero if any series fails. Every repeat's verbose CTest output is saved.

Prerequisites: Docker with a running Linux engine and at least eight CPUs available
to its VM. An eight-thread request is not a CPU allocation. The TBB startup barrier
may stall if the runtime cannot supply all participants. Do not restrict the
container to fewer CPUs for the baseline run.

From the repository root (Bash):

```sh
platform=linux/amd64
docker build --platform "$platform" -t oox-scheduler-repro tools/scheduler_repro
out=$(mktemp -d "$PWD/repro-output.XXXXXX")
git rev-parse HEAD > "$out/source-commit.txt"
git diff --binary HEAD > "$out/source-changes.patch"
git ls-files --others --exclude-standard > "$out/untracked-files.txt"
docker image inspect oox-scheduler-repro > "$out/image.json"
docker run --rm --platform "$platform" -e BENCH_NUM_THREADS=8 -e REPEATS=20 \
  --mount "type=bind,src=$PWD,dst=/src,readonly" \
  --mount "type=bind,src=$out,dst=/out" oox-scheduler-repro
```

Use `linux/arm64` for native Linux execution on Apple Silicon. AMD64 execution on
an ARM host uses emulation: successful tests do not rule out native-x86 failures,
and elapsed times are not benchmark results. Keep source files unchanged during
the run. Prefer a dedicated checkout for a reproducible snapshot.

The Ubuntu base is pinned by digest. Apt package versions are recorded in the
image/environment logs rather than claimed to be immutable across future builds.
Retain the built image (or its registry digest) to reproduce exact dependencies.

Outputs: `logs/environment.txt`, `CMakeCache.txt`, dynamic TBB library paths,
`session.log`, per-mode verbose logs, and `status.txt`. The general OOX test suite
is disabled; the scheduler-eval tests remain enabled. CMake downloads Google Test
and Google Benchmark unless cached source directories are mounted at
`/deps/googletest` and `/deps/benchmark`.

For a separate sanitizer build, use a **fresh** output directory and pass
`-e SANITIZE=address,undefined` or `-e SANITIZE=thread`; `-e CXX=clang++` selects
Clang. Sanitizer support under emulation is not guaranteed. A failure during
sanitizer initialization is not an application race report. The distribution's
TBB binary is not instrumented by this flag.

Other controls: `BUILD_JOBS` (default 4), `TEST_TIMEOUT` (default 180 seconds),
`REPEATS` (default 20). Calibration settings are reset for the ordinary series;
the final Rapid series explicitly disables calibration and sets the resident
limit to `BENCH_NUM_THREADS`.

Pass `-e OOX_EVAL_CHECK=bfs` to isolate one shared workload check. Supported checks
are `scan`, `spmv`, `bfs`, `synthetic`, `primary`, `extended`, `reduce`, `metrics`,
`intrusive`, and `granularity`. An invalid/empty explicit filter exits 2 rather
than silently running no checks. Unset it for a full correctness run. Each
selected check prints START and PASS/FAIL; generated BFS cases additionally
identify flat/nested/adaptive, graph kind, and the first differing vertex.

The independent deterministic dedup regression is also available through CMake:
target `test_eval_dedup_spurious`, CTest name `scheduler_eval_dedup_spurious`.
It forces a permitted weak-CAS failure and compares the real workload against
sort/unique, without relying on a particular concurrent execution schedule.

The short `test_eval_unpinned_TBB_SIMPLE` and `test_eval_unpinned_TBB_AUTO` tests
exercise startup with the configured thread count and one loop. Run them with `SANITIZE=address,undefined`
to check shutdown leaks as well. They are included in the runner. Unpinned
scheduler-eval runs no longer register a pinning observer with an empty callback;
pinned benchmark runs retain their observer.

## Investigation notes (2026-10-26)

Two independent issues were reproduced, neither yet established as the original
`.194` failure:

* Integer dedup advanced past an empty hash slot after a spurious weak-CAS failure.
  With input `{7,7}`, a deterministic injected first failure produced `{7,7}` instead
  of `{7}`. Retrying the empty slot fixes it. The regression covers 329 cases with
  and without injection (658 oracle checks), including three large random cases.
* Linux ARM64, GCC 13.3, oneTBB 2021.11 and ASan/UBSan reported a 40-byte observer
  proxy leak in TBB Auto after numerically correct execution. A minimal eight-thread
  probe failed 20/20 before skipping the unused observer, and passed 20/20 after,
  without disabling LeakSanitizer. The full TBB Auto workload also passed afterward
  with ASan/UBSan and leak detection still enabled. The dedup and two TBB startup
  regressions each passed ten sanitizer repetitions.

The clean diagnostic commit also passed a complete Linux AMD64-emulated sweep;
200 additional filtered runs (five checks, four scheduler modes, ten repeats) passed.
This does not establish native-x86 correctness or explain a non-sanitized failure.

### Native ARM Twist feasibility

A native macOS ARM64 probe with LLVM 21.1.3 successfully ran the Twist simulator
with isolation disabled. It also explored both first-CAS choices on the **real**
serial dedup implementation through the test hook: the old algorithm asserted,
the fixed algorithm passed. This is a CAS-semantics test, not a Rapid scheduler test.

Compatible probe revisions (floating upstream heads had API incompatibilities):

* Twist: `407b44e51a9d53c9fd5cbada08ada7a4a1cf029d`
* Sure: `0de37d2c7a97aa3764d58b319f89d066b72d9e22`
* sure-stack: `510783c501d9162082fed7e20c7e6c579007bec0`

The existing `tests/twist` backend exercises OOX dependency/lifetime state, not
Eigen/Rapid. Direct Rapid simulation still needs simulator-aware atomics, waits,
threads and spin hooks; merely enabling `HAVE_TWIST` does not provide that coverage.
