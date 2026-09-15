# Patent-only range partitioning for Eigen

The operation oox::detail::eigen_pool::ParallelForPatent(pool, begin, end,
callback, grain) accepts the existing Eigen ThreadPool&. Include
patent_parallel_for.h. The call is synchronous and the callback must support
concurrent invocation.

## Partitioning and scheduling

A worker budget starts at min(P, ceil(N/grain)). An ordinary range task
splits that budget and its range proportionally, publishes its right child
with Schedule, and continues with its left range. This creates at most P
initial owners without per-worker activation or a separate launch runtime.

Each owner retains a bounded private range buffer. It consumes local ranges
from the back, publishes a signal task, and checks whether another thread
executed that signal. Demand permits further subdivision and donation from
the front. The signal identifies its owner by thread identity, including for
concurrent external callers. An indivisible remainder always executes.

These mechanisms follow the work-demand scheme in
[WO2013021223A1 / US9262230B2](https://patents.google.com/patent/WO2013021223A1/en).
The private range buffer is adapted from oneTBB 2021.5.0, pinned at
70bf10c0a9e65e3a954156f801b0a11c96f7f6bd; provenance is retained in
tbb_partitioning.h and licenses/oneTBB-Apache-2.0.txt.
Checking demand between callback invocations is an implementation extension;
the callback itself is not preempted. Initial relative depth is fixed at three.

## Runtime boundary and later adaptation

The implementation uses ordinary Task, Schedule, NumThreads, IsCancelled,
Wait, and NotifyTaskCompletion. It does not include rapid_start.h, construct
a Rapid group, publish Rapid activations, inherit Rapid worker domains, or
use a Rapid completion region. No OOX dependency or task API changes are required.

The enclosing branch retains its existing pool, which also supports separate
Rapid modes. Both baseline and patent-only measurements use that same pool;
this patch does not modify its implementation.

For later adaptation, carry patent_parallel_for.h, tbb_partitioning.h, and
licenses/oneTBB-Apache-2.0.txt. The included pool supplies the existing Task type.
Preserve these contracts when adapting to another pool revision:

- Schedule consumes the task, including exceptional publication and rejection.
- Queued tasks are either executed or discarded; either path completes their
  contribution to the operation's outstanding count.
- Cancellation drains rejected/pending tasks. Wait may return on cancellation,
  so the operation retains its callback and state until every task is finished.
- Registered callers help ordinary queued work while waiting, allowing nesting.
- Completion uses release/acquire ordering and notifies waiting callers.

Exceptions stop further operation work and are rethrown after completion.
Nested calls use ordinary task scheduling and a fresh worker-count budget.
Concurrent external roots and calls into another pool are covered by tests.

## Selection

The existing scheduler-evaluation mode is EIGEN_PATENT_DEMAND.
The direct-loop harness mode is EIGEN_PATENT_DEMAND_LOOP.
Both route through the ordinary Eigen range entry point. The former
RAPID_PATENT_DEMAND hybrid mode has been removed.

Only the default policy is retained: demand feedback and callback-boundary
checks are always enabled, with initial relative depth three. The earlier
feedback-disabled, block-end, and depth-selection controls have been removed.
The optional metrics pointer provides diagnostics without selecting another
policy.

## Validation and results

The corrected implementation passes 121 Release tests, 215 exception-enabled
UBSan tests, and four existing benchmark entry-point tests. Generated range
checks cover small and million-item ranges, origins near SIZE_MAX, nesting,
concurrent roots, cross-pool calls, unavailable workers, saturated queues,
cancellation, allocation failure, and reuse. Initial subdivision is checked
against a serial visitation oracle and its worker budget; callbacks are also
checked for absence of a Rapid region.

Default Eigen versus the current patent implementation: [PATENT_DEFAULT_COMPARISONS.md](../../benchmarks/scheduler_eval/PATENT_DEFAULT_COMPARISONS.md).
The earlier [hybrid experiment](PATENT.md) and its data are retained explicitly
as historical evidence. They are not results for this implementation.
