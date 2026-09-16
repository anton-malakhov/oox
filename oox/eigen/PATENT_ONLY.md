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

The pool follows main's task publication, queues, stealing, and cancellation
behavior. Its only added API is the read-only IsCancelled query. Registered
callers of Wait try helping before registering as waiters. If no task is
available, they retain the registration, recheck, and parking protocol.

For later adaptation, carry patent_parallel_for.h, tbb_partitioning.h, and
licenses/oneTBB-Apache-2.0.txt. The included pool supplies the existing Task type.
Preserve these contracts when adapting to another pool revision:

- Schedule consumes the task, including exceptional publication and rejection.
- Queued tasks are either executed or discarded; either path completes their
  contribution to the operation's outstanding count.
- Wait may return on cancellation while queued tasks remain in the pool.
  Each task retains the operation state until execution or discard. Closing
  the operation blocks callback/metrics access by queued tasks and waits for
  active callbacks before returning. The callback itself is not retained.
- Registered callers help ordinary queued work while waiting, allowing nesting.
- Completion uses release/acquire ordering and notifies waiting callers.

Exceptions stop further operation work and are rethrown after active work
finishes. A cancelled operation may leave inert tasks for the pool to discard;
those tasks cannot invoke the callback or access its metrics after return.
Nested calls use ordinary task scheduling and a fresh worker-count budget.
Concurrent external roots and calls into another pool are covered by tests.

## Selection

The existing scheduler-evaluation suite selects the port with
EIGEN_PATENT_DEMAND. The branch adds a mode selector, not new benchmark
workloads, harnesses, or workflows.

Only the default policy is exposed: demand feedback and callback-boundary
checks are always enabled, with initial relative depth three. The optional
metrics pointer provides diagnostics without selecting another policy.

## Validation

Local checks passed: 205 Release tests, 351 exception-enabled UBSan tests,
and 59 existing scheduler-evaluation checks. Seven serial-backend blocking-wait
tests were intentionally skipped across the two unit-test configurations.

The dedicated EigenPatentRangeOracle and EigenPatentAllocationFailure tests
cover small and million-item ranges, origins near SIZE_MAX, nesting,
concurrent roots, cross-pool calls, unavailable workers, saturated queues,
cancellation, allocation failure, and reuse. Visitation and computed values
are checked against a serial reference. Cancellation tests also cover queued
tasks outliving the callback and waiting for an active callback to finish.

Existing scheduler-evaluation validators exercise this mode through the same
workloads and harness as default Eigen. Earlier experimental reports and raw
data are archived locally under build-patent-cleanup/archive; they are not
measurements of this cleaned implementation on main's pool.
