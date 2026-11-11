# Eigen/Rapid synchronization exploration with Twist

This is a separate CMake project. It exercises the production Eigen queue and
scheduler headers through `OOX_EIGEN_TWIST`, which redirects their synchronization
operations to Twist. It does not define `HAVE_TWIST` or select OOX's separate
Twist task engine. No queue algorithm is reimplemented in these tests.

Use LLVM Clang 21 with a C++23 standard library. All fetched dependencies are
pinned in `CMakeLists.txt`, including the transitive Sure, sure-stack, function2,
fmt, backward-cpp, and JSON dependencies. First configuration needs network
access. Existing dependency checkouts can be supplied with CMake's standard
`FETCHCONTENT_SOURCE_DIR_<NAME>` overrides; record their revisions when doing so.

From the repository root on Linux:

```sh
cmake -S tests/eigen_twist -B build-eigen-twist -G Ninja \
  -DCMAKE_CXX_COMPILER=clang++-21 -DCMAKE_BUILD_TYPE=Debug
cmake --build build-eigen-twist --parallel 4
ctest --test-dir build-eigen-twist --output-on-failure -V
```

On macOS with Homebrew LLVM 21, use
`-DCMAKE_CXX_COMPILER=/opt/homebrew/opt/llvm@21/bin/clang++` and
`-DCMAKE_CXX_FLAGS=-stdlib=libc++`, with
`-DCMAKE_EXE_LINKER_FLAGS="-L/opt/homebrew/opt/llvm@21/lib/c++ -Wl,-rpath,/opt/homebrew/opt/llvm@21/lib/c++"`
to use its matching C++23 library. If the host's SDK discovery is unavailable,
also set `CMAKE_OSX_SYSROOT` to the installed macOS SDK directory.

## Coverage matrix

The counts below describe the configured deterministic campaign, not a claim
that every possible execution has been checked. Each target reports failures
and its actual exploration progress. Keep the complete CTest log with results.

| Target suffix | Production code exercised | Oracle / scenarios | Configured random runs |
| --- | --- | --- | ---: |
| `queues` | `RunQueue`, `MPMCQueue` | Successful insertions equal consumed IDs; last item, wraparound, owner/thief, remote producer, many-to-many | 5 x 128 = 640 |
| `scheduler` | Real thread pool, ordinary Auto, resident publication, Fixed/Lazy/Native paths | Per-index exactly-once counts and serial output formula; nesting, external callers, forced tail theft, exceptions/reuse, cancellation, delayed cohort finalization, outsiders | 17 x 32 + 8 = 552 |
| `allocator` | Real `SmallObjectPool` cache, return lists and owner closure | Payload identity, pointer reuse where expected, exactly-once destruction; remote return before/during/after owner exit | 4 x 128 = 512 |
| `ranges` | `StealableRange`, real `ActiveRange`, ordinary stolen `Work` and completion | Tagged publication lifetime, mutual exclusion, stack-address reuse, forced theft with independent per-index counts/values | 32 + 64 + 64 + 64 + 16 = 240 |
| `completion` | Real `PeerJoin`, `Region`, first-child `Work` promotion and allocator | Independent destruction counts and completion state; execution-before-peer, branches-before-execution, concurrent last releases, descendant retaining ancestor, first-child promotion | 5 x 64 = 320 |

The scheduler's eight-seed case uses nine workers to exercise the non-packed
completion path. Other scheduler cases normally use two to four workers, not
128 workers. `fixed_direct`, `fixed_packed`, and `fixed_epoch` select distinct
completion configurations. The Native cases use Auto, Simple, Static, and
Affinity policies. The exact-once requirements apply to successful loops;
`cancel_drain` instead checks at-most-once execution and orderly pool teardown.

To run one scheduler scenario, pass its name, for example
`build-eigen-twist/test_eigen_twist_scheduler lazy_tail_steal`.
The test prints every seed before executing it, so the last line identifies a
hang as well as a reported assertion failure.

Tiny DFS campaigns supplement random scheduling. Their observed counts in the
initial native ARM run were 64 schedules for the deque last-item case, 24 for the
mailbox last-item case, 34 for allocator closure, and one for nested range
publication. None were pruned or exhausted their schedule budget. The one-schedule
range case is a single-thread lifecycle sanity check, not concurrent DFS coverage.
The full pool and spin-waiting range scenarios use seeded random scheduling only.

## Queue, allocator, and range details

`test_eigen_twist_queues` uses the real `RunQueue` and `MPMCQueue` with tiny
capacities. An independent multiset oracle compares all successfully inserted
unique IDs with consumed IDs after joining threads and draining remaining work.
It detects lost, duplicated, and corrupted values, including the last-item race,
owner versus remote deque operations, wraparound, and multi-producer/multi-consumer
mailbox use. Failed best-effort pop or push attempts are permitted.

`test_eigen_twist_allocator` keeps the actual small-object cache enabled and
checks local reuse, remote return and reuse, returns after the allocating thread
exits, and remote return racing with owner closure. Payload IDs and independent
destructor counts check object integrity and exactly-once destruction. The
test directly allocates a small payload so instrumented scheduler objects that
exceed the 256-byte cache capacity cannot silently bypass this coverage.

Each queue scenario runs deterministic random seeds 1 through 128, with Twist's default
1/7 probability of weak-CAS/try-lock spurious failure. Last-item scenarios also run
DFS with at most two preemptions and 200 scheduling steps per simulation, capped
at 50,000 schedules. Logs print seeds, bounds, complete/pruned counts, and whether
the schedule budget was exhausted. Re-run the same executable and revisions to
reproduce the deterministic sequence; a failure reports the scenario and seed or
DFS schedule index.

Allocator scenarios also use random seeds 1 through 128. Its closure-race DFS
uses the same step/preemption/schedule bounds but disables spurious failures in
DFS to avoid an unbounded sequence of weak-CAS failures. Random allocator runs
retain Twist's default fault injection. Readiness for the closure race uses a
blocking condition variable so DFS cannot spend the entire bound in a spin loop.

The range campaign includes nested publication and restoration, concurrent thieves,
unpublication joining an in-flight thief, repeated stack-address reuse, and actual
tail reservation at grain sizes 1 and 3. It checks non-zero range offsets and a
17-element range ending at `SIZE_MAX`. Its real `ActiveRange` scenario holds the
first owner callback until a thief succeeds; a passing owner-only execution
cannot silently substitute for testing the theft path.

## Oracle sensitivity check

A private negative-control copy of the actual `ActiveRange::Steal` implementation
was built with only `end_.store(middle, release)` changed to
`end_.store(last, release)`. This intentionally leaves the owner's range intact
after a thief takes its tail. The unmodified range suite passed; the mutated
binary failed `active_range_exact_once` at seed 1 on `visits[index] == 1`, twice
reproducibly. The repository implementation was never mutated for this check.
This establishes sensitivity to duplicate execution caused by this particular
range-ownership defect; it does not establish detection of every concurrency bug.

## What passing means

This is bounded synchronization exploration, not verification of every possible
execution or CPU memory ordering. Non-isolated Twist simulation explores the
instrumented synchronization operations; ordinary payload accesses are checked
by the value oracle, not by a complete data-race detector. Tiny queue tests check
safety, not unbounded fairness or throughput. DFS-pruned schedules and a reached
schedule budget are reported explicitly and must not be called exhaustive proof.
The pinned non-isolated simulator can abort the process when its step bound is
exhausted. Such a run is incomplete, not a successful proof or necessarily a
scheduler bug. CTest also enforces a wall-clock timeout.
Twist's TLS destructor order differs from native C++ thread-local destruction.
The allocator uses one simulator-only TLS owner and exercises its real closure
protocol, but allocation from unrelated late TLS destructors is **not covered**
by this simulation. Keep the existing native allocator lifetime tests for that
case. Non-isolated simulation also does not establish absence of heap leaks.
Keep native x86/ARM correctness, sanitizers, and performance testing separately.

## Synchronization adapter limits

Without `OOX_EIGEN_TWIST`, the synchronization names are aliases of their original
standard-library types, native TLS remains native, and original memory orders
are retained. The simulation changes the implementation of those primitives:

- `atomic<T>` supplies explicit zero initialization for a pinned Twist default
  constructor limitation. Its larger metadata changes object sizes. Some actual
  scheduler tasks consequently exceed the small-object pool capacity; the
  allocator target separately forces the real cached-allocation path.
- The pinned simulator accepts acquire/release/acq_rel fences, but not an SC
  fence. The adapter maps SC fences to acq_rel inside the simulator. This campaign
  explores synchronization interleavings and must not be used to justify removal
  or weakening of production SC fences, or to claim hardware weak-memory proof.
- Twist has no `call_once` primitive here. The adapter uses a simulated mutex and
  completion flag, retaining retry after an exception. Native `std::call_once`
  implementation details are not covered.
- Virtual-thread IDs and TLS are modeled. OS scheduling, CPU affinity, cache
  coherence latency, SMT/NUMA behavior, energy use, and performance are not.
- Native standard-library internals such as `shared_ptr` reference counting,
  exception machinery, and heap allocation are not fully instrumented. Value and
  lifecycle checks complement but do not replace native sanitizers.

Hardware calibration (`rapid_auto_calibration.h`) uses wall-clock measurements
and still contains a native yield; it is deliberately not invoked by these
tests. The legacy native stack-bounds helper is also outside this campaign.
Tracing and the thread-pool/small-object-pool global testing-counter macros are
not enabled: their process-global state would require separate simulator-aware
registration. Per-object metrics used by the forced-tail test are instrumented.
Likewise the campaign does not claim counter wraparound, pools above 64 workers,
every mailbox overflow path, all affinity-history reuse patterns, or arbitrary
deep recursive nesting. Add explicit cases before claiming those properties.
