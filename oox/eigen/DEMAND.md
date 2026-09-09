# Experimental TBB-derived Eigen backend

Configure with OOX_ENABLE_EIGEN_DEMAND=ON and link OOX::eigen_demand instead
of OOX::eigen. The alternative defines HAVE_EIGEN_DEMAND=1 and uses the existing
OOX_EIGEN_THREADS setting. It is disabled by default and requires
OOX_ENABLE_EIGEN=ON. Direct header consumers can select HAVE_EIGEN_DEMAND=1.
Select one asynchronous backend consistently across translation units.

The OOX task API and dependency implementation are unchanged. The backend
collects up to 64 already-ready local, stolen, or overflow tasks and applies a TBB-derived range/depth
policy to those task pointers. Each original callback remains indivisible.
Grouping does not avoid allocations already performed by oox::run.

## Source and adaptation

tbb_partitioning.h ports the range_vector algorithm and automatic partitioner
budget/depth transitions from oneTBB 2021.5.0, commit
[70bf10c0a9e65e3a954156f801b0a11c96f7f6bd](https://github.com/uxlfoundation/oneTBB/tree/70bf10c0a9e65e3a954156f801b0a11c96f7f6bd).
The source files are
[partitioner.h](https://github.com/uxlfoundation/oneTBB/blob/70bf10c0a9e65e3a954156f801b0a11c96f7f6bd/include/oneapi/tbb/partitioner.h)
and
[parallel_for.h](https://github.com/uxlfoundation/oneTBB/blob/70bf10c0a9e65e3a954156f801b0a11c96f7f6bd/include/oneapi/tbb/parallel_for.h).
The imported code retains its Apache-2.0 notice; the license is included in
licenses/oneTBB-Apache-2.0.txt. The original Eigen files retain MPL-2.0 notices.

The pinned partitioner.h SHA-256 is
7a1ce6339030521b8ff402b3b7f61fda745556e702c53c30151eacded72dae6a.

The port preserves front donation/back execution order, range-relative depth,
the initial split budget, live-sibling eligibility for stolen feedback, and
fresh sibling state on a work offer. It replaces TBB storage and task APIs,
uses nonthrowing index ranges, and bounds depth to size_t's bit width.

demand_policy.h is the OOX-specific adapter. It uses reference-counted group
storage and a sibling tree independent of OOX's dependency graph. A callback
holds its branch alive until execution ends, so nested execution does not
prematurely retire its sibling relationship. Old stolen flags remain scoped to
the old sibling group.

Deliberate differences from TBB's algorithm-level partitioner:

- The range consists of already-created task pointers.
- Initial distribution is per collected group. The adapter starts with depth
  one and a split divisor of two. The isolated core retains TBB's depth-five
  default for source-comparison tests.
- Remaining spans live in a mutex-protected worker registry, accessible to
  nested helping and other workers.
- A scheduler resume can migrate a group's remaining work. It preserves the
  group's stolen-initialization state and attributes new offers to the current
  worker. No running callback is preempted, and execution returns to a scheduler
  checkpoint after one task.
- The adapter retains singleton and partial-group progress; it never waits for
  a group to fill. Optional metadata allocation failure falls back to ordinary
  task execution without abandoning extracted work.
- The alternative isolates unhandled exceptions from raw fire-and-forget pool
  callbacks. OOX exception propagation remains in its existing task adapter.

The default Eigen instantiation has no group registry and keeps its acquisition
policy. Both instantiations use an RAII owner in UniqueTask so a throwing
callable releases its closure during unwinding.

## Ownership and lifecycle

Each task is accounted for when originally published. Moving it into a group
or another registry does not call Schedule and does not modify its outstanding
counter. Every task still passes through ExecuteTask separately.

Pending group tasks are visible before any callback starts. The same acquisition
path serves normal workers and nested Wait. Group locks are never held across
user callbacks, and source/destination registry locks are not held together.
Publication of newly available groups participates in the existing worker
wake-up protocol.

Shutdown drains groups and original queues. Cancellation disposes of remaining
task pointers once, using their original accounting counters. Task pointers
inside consumed storage positions may be stale; only owned pending spans are
visited during draining.

## Validation and comparison

The optional target adds the existing pool and OOX semantic tests for the new
backend, deterministic TBB transition tests, generated range/group ownership
oracles, a 200,000-task contention case, and generated DAGs evaluated independently
in serial. The generated DAG tests run unchanged against both backend selections.

The comparison executable is benchmarks/bench_eigen_demand. Its arguments are
policy (legacy, fixed, or demand), background worker count, task count, and
ordinary task cost. It emits JSON and checks every result against a serial
oracle outside the timed region. Fixed uses the same grouping adapter with
feedback disabled. Each policy receives the same skewed ready-task workload.

GetDemandStatistics reports collected groups, work offers, remotely acquired
groups, eligible stolen feedback events, and tasks initially collected. Group
moves are not new application-task submissions. These counters are always
present in the experimental policy; the legacy instantiation reports zero.

This is an experimental backend, not a claim of improved performance.
Evaluate uniform work, skewed work, recursive dependencies, nested waits,
concurrent callers, and worker availability changes before choosing it for a
workload. Task granularity and application decomposition must match across
comparisons. Instrumentation and the reference-counted registry add overhead.
