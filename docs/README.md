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
  `std::future`, ...) — worker threads never wait on foreign primitives
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
- **Writer chain** — multiple writers on one handle are linked through
  `next_writer`; the last registered writer is the *current* one, and each
  writer runs after the previous completes (anti-dependency via `countdown`).

### 2.2 Handles

- **`oox::var<T>`** — single-owner, move-only handle to a value produced by a
  task (or stored directly). Read via `get()`/`wait()`/`wait_and_get()`;
  consumed by `oox::run` as an argument.
- **`oox::shared_var<T>`** — thread-safe, copyable, reference-counted
  counterpart. Multiple threads may register readers/writers through
  `oox::run`, call `get()`/`wait()`, copy, and assign. See
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

The same library compiles against several execution backends, selected at
build time (priority order): **TBB** (default, recommended), **TaskFlow**,
**Folly fibers**, **OpenMP** (comparison), **Twist** (deterministic testing),
**std** (`std::async`, thread-per-task), and **serial** (debug). All
semantics are backend-independent; waits are always performed through the
backend's own native mechanism.

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

## 4. Build

Requirements: CMake ≥ 3.14, a C++17/20 compiler, GTest, Google Benchmark,
optionally TBB / TaskFlow / Folly / OpenMP / Twist.

```sh
make            # release build + tests + bench_fib (TBB allocator)
make debug      # Debug build
make test       # ctest
make install    # cmake --install
```

CMake options (root `CMakeLists.txt`):

- `OOX_BUILD_TESTS` (ON), `OOX_BUILD_BENCHMARKS` (ON),
  `OOX_BUILD_EXAMPLES` (ON);
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
  built per backend (std/serial/tbb/tf).
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

`benchmarks/` (Google Benchmark; `bench_*_TBB.exe` variants per backend):

- `bench_fib`, `bench_loops`, `bench_accessing`, `bench_accounts` — classic
  patterns (Fibonacci, loop parallelism, task access, account transfers);
- `bench_shared_var_get_heavy` — concurrent `get()` + writer registration on
  one shared_var (the wait-path pattern);
- `bench_taskbench` — TaskBench-style kernels with per-backend runners
  (oox/serial/openmp/tbb/taskflow/folly).

## 7. Examples

- `examples/accounts.cpp` — the shared_var showcase: an array of
  `shared_var<account>`, 100 000 random point transfers between accounts,
  per-account writer serialization, total conservation check.
- `examples/fibonacci.h`, `examples/filesystem.h`, `examples/wavefront.h` —
  example patterns compiled into `test_oox`.

## 8. Directory layout

- `oox/oox.h` — the core library (graph, var, run, backends);
- `oox/shared_var.h` — the shared_var layer (waiter-based waits,
  multi-state registration);
- `tests/`, `tests/twist/` — gtest and Twist suites;
- `benchmarks/`, `benchmarks/taskbench/` — Google Benchmark suites;
- `examples/` — examples;
- `scripts/run_benchmarks.py` — benchmark driver;
- `thirdparty/` — vendored/fetched dependencies (TBB, TaskFlow, Folly,
  fast_float, oneTBB);
- `docs/` — this documentation.

## 9. Design documents

- `docs/design-shared-var.md` — the thread-safe shared handle design
  (thick-handle architecture, atomic multi-state registration,
  waiter-as-graph-edge waits, deferred/forwarding semantics).


## 10. References

- A. Malakhov, "OOX 2.0: Out of order execution made easy", Intel corporate
  blog, 2021 — the original proposal this project implements:
  <https://habr.com/en/company/intel/blog/542908/>.
