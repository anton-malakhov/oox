# OOX documentation

Out-of-Order Executor — a C++ library for continuation-based, dependency-graph
tasking. This document describes the whole project: philosophy, architecture,
API, build, testing, and benchmarks. Detailed design documents are indexed at
the bottom.

## 1. Overview

OOX implements **continuation-focus tasking**: instead of blocking-style
parallelism (`tbb::parallel_invoke`, `taskflow` subflow `join`), the user
describes the *data flow* and OOX builds a dependency graph in which every
task runs exactly when its inputs are ready. Nested parallelism does not block
worker threads; the graph reuses threads for ready continuations, which avoids
the deadlock-prone and latency-heavy nesting of blocking style.

```cpp
oox::var<int> Fib(int n) {                 // continuation style, no blocking
    if (n < 2) return n;
    auto left = oox::run(Fib, n - 1);
    return oox::run(std::plus<int>(), std::move(left), Fib(n - 2));
}
```

### 1.1 History and motivation

OOX is the implementation of the **OOX 2.0 proposal** by Anton Malakhov
(Intel), created in 2014 as his last contribution to TBB as a core developer
(see the original article:
<https://habr.com/en/company/intel/blog/542908/>). The name comes from the
original "Out of Order eXecution" idea of Arch Robison; since 2021 it
officially stands for "Out-of-Order Executor".

The motivating problem is **nested blocking**: the blocking style used by
TBB's parallel algorithms and TaskFlow's subflows can deadlock when user
functions take locks (one outermost task gets stuck waiting for another), and
workarounds (`tbb::task_arena`, `tbb::this_task_arena::isolate()`) are either
inefficient or incomplete. Stack-switching (coroutines) and thread-per-task
avoid the deadlock but add scheduler complexity and resources. OOX 2.0
proposes a purely **semantical** way out: terminate the blocking scope and
split it into a sequence of continuations, expressing dependencies through
the function arguments of `oox_run` instead of blocking calls.

`std::async`/`std::future` were the closest starting point, but cannot express
recursive continuation graphs directly (no implicit conversion of the result,
no collapsing of `future<future<...>>`, blocking `get()`). OOX extends the
idea with four requirements:

1. directly initialize a `var` variable by a value;
2. collapse template recursion of `var` variables;
3. build task dependencies based on `var` arguments instead of blocking;
4. unpack `var` types into plain types before calling the user functor.

The two design pillars are: **abstract the user functor from task
dependencies** and **reuse functor argument types as the dependency type
specification** — the user states the minimally necessary access types and OOX
extracts as much parallelism as the types allow.

Key properties of the model:

- **No blocking in algorithms**: dependencies are expressed by arguments, not
  by `wait()`; waiting is an explicit graph edge.
- **Lock-free graph core**: the dependency machinery (`arc_list`,
  `output_node`, `countdown`, `start_count`, `head`) is built on atomics and
  verified by deterministic testing (Twist).
- **Waiting is a graph edge**: blocking waits (`get()`/`wait()` on
  `shared_var`) register a waiter node into the graph's lock-free arc queue
  and block on the *pool's own* native mechanism (TBB `wait_context`,
  the Eigen pool's own worker wait, `std::future`, or C++20 `std::atomic`
  wait/notify for external threads) — worker threads never wait on foreign primitives
  (no condition variables, no spin loops in the library).
- **Clear serialization semantics**: writers chained onto the same `var` /
  `shared_var` are serialized; readers run in parallel.

## 2. Core concepts

### 2.1 The dependency graph

- **`task_node`** — a node; holds the incoming prerequisite counter
  (`start_count`), the outgoing arc list (`head`), per-output writer chains
  (`output_node::next_writer`), and the lifetime reference count
  (`life_count`).
- **`arc`** — a directed edge. `flow_back`/`flow_copy` edges notify the
  consumer when the producer completes (prerequisite accounting); `flow_only`
  edges notify without data.
- **`remove_prerequisite`** — when the last prerequisite is satisfied, the
  node is spawned on the backend pool.
- **Lifetime** — every node has a reference count; the "owner end" of a
  handle and the graph's notifications keep nodes alive until all consumers
  are done. Raw `release(n)` accounting is what makes retains (bumping
  `life_count` as an external hold) invalid — see the shared_var design doc.
- **Submission rejection** — when compiler exception handling is available,
  an attached successor rejected by a backend is completed inline while the
  producer continues traversing its detached arc list. Lazy `shared_var`
  publication preserves its caller-visible submission exception after the
  installed materializer and its waiters reach terminal states. With compiler
  exceptions disabled (`-fno-exceptions`), these recovery catches are not
  compiled and publication uses the direct backend path.
- **Writer chain** — multiple writers on one handle are linked through
  `next_writer`; the last registered writer is the *current* one, and each
  writer runs after the previous completes (anti-dependency via `countdown`).

### 2.2 Handles

- **`oox::var<T>`** — single-owner, move-only handle to a value produced by a
  task (or stored directly). Read via `get()`/`wait()`/`wait_and_get()`;
  consumed by `oox::run` as an argument.
- **`oox::shared_var<T>`** — thread-safe, copyable, reference-counted
  counterpart. Multiple threads may register readers/writers through
  `oox::run`, call `get()`/`wait()`, copy the handle, and assign `T` values.
  Copy/move assignment that rebinds the same handle object requires external
  synchronization, as it does for one `std::shared_ptr` object. See
  `docs/design-shared-var.md`.
- **`oox::node`** — `var<void>`; carries only dependency info.

### 2.3 Matching rules for `oox::run` arguments

- plain arguments: decay-copied; `std::ref`/`std::cref` for references
  (lifetime is the caller's responsibility);
- `var`/`shared_var` arguments: the functor's parameter type selects the
  access category. The taxonomy (from the OOX 2.0 proposal):

  - **read-write** (`A&`) — the value is exclusively owned by a single
    producer task which can modify it; writers are serialized, and
    completion unlocks all pending shared consumers;
  - **final-write** (`A&&`) — a read-write task with no consumers or next
    producers — useful for optimizations (the storage can be moved out);
  - **read-only** (`const A&` or `A`) — the value can be shared concurrently
    by tasks that start after the producer finishes and that prevent the
    next producer from running until all consumers complete;
  - **copy-only** (`A` with a copy-optimized implementation) — the producer
    copies the value into the consumer and does **not** depend on the
    consumer's completion, unlocking more parallelism.

  This automatic deduction builds the dependency graph without any
  specification beyond the `run()` arguments: **flow** (read-after-write),
  **anti** (write-after-read), and **output** (write-after-write)
  dependencies are derived from the access categories. For example:

  ```cpp
  oox::var<T> a, b, c;
  oox::run([](T& A)      { A = f(); }, a);            // read-write
  oox::run([](T& B)      { B = g(); }, b);            // read-write
  oox::run([](T& C)      { C = h(); }, c);            // read-write
  oox::run([](T& A, T B) { A += B; }, a, b);          // write + copy-only
  oox::run([](T& B, T C) { B += C; }, b, c);          // write + copy-only
  oox::wait_for_all(b);  // this thread joins the computation
  ```

- `var`/`shared_var` arguments are always stored by reference (they guarantee
  lifetime and access synchronization); a `var<A>&&` argument is moved into
  the functor (final consumption, like a plain rvalue).

### 2.4 Backends

The same library compiles against exactly one execution backend selected at
build time: outside serial-debug builds, enabling more than one
asynchronous backend macro is a compile error (selecting by incidental
preprocessor order would make consumer behavior non-portable). The
`oox/backends/select.h` priority chain runs through **OpenMP**, **TBB**
(recommended), **TaskFlow**, **Twist** (deterministic testing),
**Folly fibers**, and the bundled **Eigen** work-stealing scheduler, and
falls back to **std** (`std::async`, thread-per-task) when no macro is
defined; `OOX_SERIAL_DEBUG` intentionally overrides everything to the
**serial** debug backend. All semantics are backend-independent; waits are
always performed through the backend's own native mechanism.

The **Eigen** backend is bundled (no external dependency): OOX's private
scheduler port, all symbols in `oox::detail::eigen_pool`, selected through
the `OOX::eigen` target (`HAVE_EIGEN=1`). Workers briefly spin only when
requested, then park with C++20 atomic wait/notify; the wait path is the
pool's own mechanism (`pool.Wait` for pooled workers, C++20 `std::atomic`
wait/notify for external threads). The worker count comes from
`OOX_EIGEN_THREADS` (CMake; `0` = `std::thread::hardware_concurrency()`,
one-worker fallback) or `OOX_EIGEN_NUM_THREADS` (headers-only builds).
Provenance, OOX additions, and per-file licenses: `oox/eigen/README.md`.

### 2.5 Storage rules and dynamic dependencies

Storage rules (from the OOX 2.0 proposal):

- `oox::run` returns `var<T>` for the **decay type** of the functor return
  type, copy- or move-initialized;
- `var<T>` never stores references — use `std::reference_wrapper` or pointer
  types instead;
- `var<T>` is rejected at compile time unless
  `std::is_same_v<T, std::decay_t<T>>`.

Since a `var` can go out of scope before the task finishes with its value, it
is always a pointer to a separate storage; the implementation may embed the
storage into the writing task (fewer allocations, but no stable location
guarantee) or keep it in the initiating task (keeps the producing task
allocated).

Dynamic (runtime-determined) dependency graphs are expressed either as a
**chain of small tasks** (a serialized reduction — deterministic) or with the
**anti-dependence trick** (parallel writes into a shared container, then one
final read-write task that aggregates). The proposal also defines the join
primitives `oox_join(node...)` / `oox_join(begin, end)` (a flow-only node that
completes when all its inputs complete) and `oox::run(dep, f, args...)` (a
task additionally flow-dependent on `dep`) — the building blocks of the
recursive examples (MergeSort, Quicksort, NBody, Wavefront).

## 3. API reference

```cpp
namespace oox {

template <typename T, bool CanThrow = default_exception_policy>
class var;                                   // single-owner handle

using node = var<void>;                      // dependency-only handle

template <typename T, bool CanThrow = default_exception_policy>
class shared_var;                            // thread-safe, copyable handle

// Spawn a task; returns a var promise of the result. Var/shared_var
// arguments build continuations (no blocking).
template <typename F, typename... Args, bool CanThrow = default_exception_policy>
var<std::invoke_result_t<F, Args...>, CanThrow> run(F&& f, Args&&... args);

// var<T>
[[nodiscard]] T get();                       // wait + read (copy for shared_var)
void wait();
void cancel();
template <bool ThrowOnCancellation = true> wait_status wait_for_all_status();
void wait_for_all(const var<T, CanThrow>&);
[[nodiscard]] T wait_and_get(var<T, CanThrow>&);
[[nodiscard]] T wait_and_get(var<T, CanThrow>&&);

// shared_var<T>
[[nodiscard]] T get() const;                 // copy; requires copyable T
void wait() const;
void cancel() noexcept;
template <bool ThrowOnCancellation = true> wait_status wait_for_all_status() const;
void wait_for_all(const shared_var<T, CanThrow>&);
[[nodiscard]] T wait_and_get(const shared_var<T, CanThrow>&);

} // namespace oox
```

`CanThrow` (with `OOX_EXCEPTIONS_ENABLED=ON`) adds exception-aware state
machinery: failed tasks propagate through the graph and `get()` rethrows.
This policy starts after a task has been published. Dependency registration in
`run()` is synchronous and intentionally provides no exception-safety
guarantee: if argument `setup()` throws, OOX does not roll back registrations
or reclaim the unpublished task. Applications must treat graph-construction
failures as fatal or prevent them at that boundary.
For `CanThrow == false`, operations performed by argument consumption are also
part of the non-throwing contract: deferred/forwarded materialization and a
copy/move or cross-type conversion from the actual stored-value reference must
be `noexcept`. Omitted defaulted callable parameters are allowed; checks cover
only the supplied argument prefix. Lazy materialization prefers a nothrow move,
then a nothrow copy, and runs in a graph task so exception-enabled failures are
propagated asynchronously. Unsafe non-throwing combinations are rejected at
compile time even when exception machinery is disabled.

## 4. Build

Requirements: CMake ≥ 3.18, a C++20 compiler, GTest and Google Benchmark
(both fetched via FetchContent when not installed), and optionally TBB /
TaskFlow / Folly / OpenMP / Twist for the corresponding backends. The
**Eigen** backend is bundled — no external dependency.

```sh
make            # release build + tests + bench_fib (TBB allocator)
make debug      # Debug build
make test       # ctest
make install    # cmake --install
```

CMake options (root `CMakeLists.txt`):

- `OOX_BUILD_TESTS` (ON), `OOX_BUILD_BENCHMARKS` (ON),
  `OOX_BUILD_EXAMPLES` (ON);
- `OOX_ENABLE_EIGEN` (ON) — build the bundled Eigen work-stealing backend
  (target `OOX::eigen`; defines `HAVE_EIGEN=1`, links `OOX::OOX` +
  `Threads::Threads`); `OOX_EIGEN_THREADS` fixes the worker count
  (`0` = `std::thread::hardware_concurrency()`, one-worker fallback;
  headers-only builds use `OOX_EIGEN_NUM_THREADS`);
- `OOX_ENABLE_TBB` (ON), `OOX_ENABLE_TF` (ON), `OOX_ENABLE_FOLLY` (ON),
  `OOX_ENABLE_OMP` (ON) — enable/disable backends
  (`OOX_LOCAL_*` force fetching a local copy);
- `OOX_BUILD_TWIST_TESTS` (OFF) — build the Twist-based concurrency tests;
  `OOX_TWIST_GIT_TAG`, `OOX_TWIST_RANDOM_SEEDS` (32),
  `OOX_TWIST_MAX_STEPS` (10000), `OOX_TWIST_MAX_PREEMPTS` (3);
- `OOX_SANITIZE` (e.g. `tsan`/`asan`) — compile with `-fsanitize=`;
- `OOX_EXCEPTIONS_ENABLED` (OFF) — exception-aware machinery;
- `OOX_ALLOCATOR` — task allocation backend (`tbb`/`system`/`jemalloc`).

Twist-based builds require a Clang compiler (the fetched `sure` library uses
`__has_feature`); presets in `CMakePresets.json` configure
`twist-fault`/`twist-sim`/`tsan` builds.

## 5. Testing

- **Unit tests** (`tests/test_oox.cpp`, `tests/test_shared_var.cpp`) — gtest,
  built per backend: `test_*` (std), `test_*_serial`, `test_*_tbb` (when
  TBB is found), `test_*_eigen` (when `OOX_ENABLE_EIGEN`, links
  `OOX::eigen`), and `test_*_tf` (when TaskFlow is found);
- **Eigen pool tests** (when `OOX_ENABLE_EIGEN`) — `eigen_pool_test`
  (`MakeTask` copy/move, fail-fast on an unhandled task exception,
  rejection of non-positive thread counts, single-worker submission),
  `eigen_pool_instantiation`, and `eigen_include_oox_first` /
  `eigen_include_upstream_first` (coexistence with upstream Eigen 3.4 in
  either include order);
- **Compile-policy tests** — `NonThrowConsumeCompile` and
  `NestedCMakePreservesListValues` (always built), plus
  `ExceptionPolicyCompile` when `OOX_EXCEPTIONS_ENABLED=ON`;
- **Twist tests** (`tests/twist/`) — deterministic concurrency testing:
  - `twist-fault` — real threads + fault injection, randomized seeds;
  - `twist-sim` — full state-space exploration (RandomSeeds; DFS requires
    `TWIST_SIM_ISOLATION=ON`, blocked in some environments by an upstream
    sure/twist gap);
  - `OOX_SANITIZE=thread` (`twist-fault-tsan` preset) — TSAN gate.
  Scenarios cover the graph semantics, lifetime, publication, exceptions, and
  the shared_var countertests (deferred publication, opposite-order multi-var
  registration, fast-writer/reader race, forwarding).

## 6. Benchmarks

`benchmarks/` (Google Benchmark; one target per mode, e.g.
`bench_fib_TBB.exe`, `bench_loops_TBB.TBB_SIMPLE`, plus
`bench_fib_<MODE>_noexc`/`_exc` exception-policy variants):

- `bench_fib`, `bench_loops`, `bench_accounts` — classic patterns
  (Fibonacci, loop parallelism, account transfers);
- `bench_shared_var_get_heavy` — concurrent `get()` + writer registration on
  one shared_var (the wait-path pattern);
- `bench_taskbench` (`OOX_BUILD_TASKBENCH=ON`) — TaskBench-style kernels with
  per-mode runners (oox/serial/tbb-flow/taskflow/openmp/folly);
- `bench_taskbench_alt_failures` — additional failure-path benchmark
  (TBB only);
- `benchmarks/scheduler_eval/` (`OOX_BUILD_SCHEDULER_EVALS=OFF`) — native
  scheduler evaluations of Rapid Start, Launch, and SpMV workloads across
  the OOX Eigen modes and the reference Eigen schedulers (driver `run.py`);
- `benchmarks/pbbs/` — the PBBS application suite reproduced from the pinned
  `EgorkaZ/pbbsbench` `eigen-mailbox` snapshot (commit `396a299`): the
  original serial baselines, OOX's `oox::run`/`oox::var` port, the OOX
  Eigen port (modes `EIGEN_STEALING`, `EIGEN_SHARING`,
  `EIGEN_STEALING_GRAINSIZE`, `EIGEN_SHARING_STEALING`), and the untouched
  historical Eigen plugin for reference (driver `run.py`).

## 7. Examples

- `examples/accounts.cpp` — the shared_var showcase: an array of
  `shared_var<account>`, 100 000 random point transfers between accounts,
  per-account writer serialization, total conservation check (also built as
  the `accounts_example` executable);
- `examples/fibonacci.h` — the Fibonacci recursion in three styles (serial,
  concise OOX, task-order-optimized OOX);
- `examples/filesystem.h` — recursive `disk_usage` over a tree (a simple
  serialized-writer version, plus a TBB `concurrent_vector` anti-dependence
  variant);
- `examples/wavefront.h` — the wavefront LCS dynamic program (serial and
  straight OOX versions).

## 8. Directory layout

- `oox/oox.h` — the core library (graph, `var`, `run`);
- `oox/shared_var.h` — the shared_var layer (waiter-based waits,
  multi-state registration);
- `oox/backends/` — per-backend adapters (`select.h` plus `std`, `serial`,
  `openmp`, `tbb`, `taskflow`, `twist`, `folly`, and `eigen`);
- `oox/eigen/` — the vendored Eigen-derived work-stealing scheduler
  (namespace `oox::detail::eigen_pool`; provenance and licenses in
  `oox/eigen/README.md`);
- `tests/` — gtest suites, compile-policy cases, and the CMake consumer
  project; `tests/twist/` — the Twist deterministic-testing suite;
- `benchmarks/` — the classic Google Benchmark binaries;
- `benchmarks/eigen/` — the experimental parallel-for layer on top of the
  Eigen pool (used by the PBBS and scheduler-eval drivers);
- `benchmarks/taskbench/` — TaskBench-style kernels with per-mode runners;
- `benchmarks/scheduler_eval/` — the native scheduler-evaluation suite
  (driver `run.py`);
- `benchmarks/pbbs/` — the pinned PBBS application reproduction
  (driver `run.py`, vendored sources under `vendor/`);
- `examples/` — example patterns (plus the built `accounts_example`
  executable);
- `scripts/` — benchmark drivers and branch comparisons;
- `research/egorkaz-eigen-mailbox/` — the archived `EgorkaZ/pbbsbench`
  mailbox-scheduler snapshot (reference material, not built by CMake);
- `thirdparty/` — fetched optional dependencies (oneTBB, TaskFlow, Folly,
  fast_float);
- `docs/` — this documentation.

## 9. Design documents

- `docs/design-shared-var.md` — the thread-safe shared handle design
  (thick-handle architecture, atomic multi-state registration,
  waiter-as-graph-edge waits, deferred/forwarding semantics).
- `oox/eigen/README.md` — provenance, OOX additions (guarded overflow
  queue, C++20 atomic wait/notify parking), and per-file licenses of the
  vendored Eigen-derived scheduler.


## 10. References

- A. Malakhov, "OOX 2.0: Out of order execution made easy", Intel corporate
  blog, 2021 — the original proposal this project implements:
  <https://habr.com/en/company/intel/blog/542908/>.
- The Eigen-derived work-stealing scheduler vendored in `oox/eigen/`
  originates from the `eigen-mailbox` experiment in
  [`EgorkaZ/pbbsbench`](https://github.com/EgorkaZ/pbbsbench), pinned at
  commit `396a299f03c58dbe9e7604daab38a65781227b75`
  <https://github.com/EgorkaZ/pbbsbench/tree/396a299f03c58dbe9e7604daab38a65781227b75/parlaylib/include/parlay/internal/scheduler_plugins/eigen>.
