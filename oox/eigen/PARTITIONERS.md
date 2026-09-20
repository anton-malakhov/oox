# Range partitioners for Eigen

Include parallel_for.h and use oox::detail::eigen_pool::ParallelFor with the
existing ThreadPool. The call is synchronous and the callback must support
concurrent invocation.

ParallelFor(pool, begin, end, callback, grain) selects AutoPartitioner.
ParallelFor(pool, begin, end, callback, partitioner, grain) selects an explicit
policy. Grain defaults to one; an optional partitioner_detail::Metrics pointer
can follow it.

| Policy object | Benchmark mode | Behavior |
| --- | --- | --- |
| AutoPartitioner | EIGEN_AUTO | Adaptive subdivision driven by stolen siblings |
| SimplePartitioner | EIGEN_SIMPLE | Binary subdivision until the grain limit |
| StaticPartitioner | EIGEN_STATIC | Proportional worker-count subdivision and placement hints |
| AffinityPartitioner | EIGEN_AFFINITY | Adaptive subdivision with placement history across calls |

AutoPartitioner, SimplePartitioner and StaticPartitioner are stateless tags.
Keep an AffinityPartitioner object across repeated calls over the same iteration
space to retain its history. Each independent operation should normally have
its own object.

## Partitioning and scheduling

The partition state follows oneTBB 2021.5.0 auto_partitioner. Its initial
divisor is 2*P, its depth limit is five, and its private range buffer holds
eight entries. Binary splitting halves both the range and the divisor.
When the divisor reaches one, an additional useful sibling is published and
the local depth limit decreases by one, as in upstream is_divisible.

Each split creates a join node shared by its left and right subtrees.
A stolen task whose divisor is zero signals demand through that node only
while its peer subtree is unfinished. The stolen task increases its depth
limit and executes its own useful range. There are no empty probe tasks.
Demand stays set on the current join node until another split attaches the
owner and its new sibling to a fresh node.

Owners fill the private buffer, test sibling demand, donate from the front,
and execute from the back. Donated children inherit the parent's depth limit
minus the donated range's relative depth. Demand checks occur between whole
private chunks. Each nested call starts a fresh auto_partitioner state from
the pool size, just as a fresh upstream partitioner does.

These mechanisms implement the work-demand scheme in
[WO2013021223A1 / US9262230B2](https://patents.google.com/patent/WO2013021223A1/en).
Partition-state transitions, the private buffer, and sibling join feedback
are adapted from oneTBB 2021.5.0 partitioner.h and parallel_for.h, revision
70bf10c0a9e65e3a954156f801b0a11c96f7f6bd. The Apache-2.0 notice is retained in
tbb_partitioning.h and licenses/oneTBB-Apache-2.0.txt.

The executor remains Eigen. Stealing is identified by execution on a different
thread from the publishing thread; Eigen has no TBB execution-slot metadata.
Cancellation and callback lifetime retain the Region/WorkLease protocol below.
One-worker and indivisible root ranges keep the existing serial fast path.
The size_t range adapter caps depth at the number of size_t bits. These are
runtime adaptations. The pool implements the two-location affinity-proxy
protocol; it does not depend on a TBB arena or runtime.

## Task layout and compile-time specialization

The policy type controls task metadata and publication through if constexpr.
Simple tasks store neither a publishing-thread ID nor a placement hint; static
tasks store only the hint; auto tasks store only the thread ID. Affinity tasks
need both. Empty state and context use no_unique_address.

Auto and affinity tasks embed their sibling join node in the task allocation.
A typed parent chain shares a counter between the two subtree references and
the queued/executing task's lifetime. Both must end before storage is reclaimed.
The parent pointer is captured before releasing the last subtree reference,
because task completion can concurrently reclaim that allocation.

The adaptive tree retains one Region reference instead of updating Region's
shared count for every published task. A tagged root link releases that
reference after the final branch completes. The caller owns a separate
reference. Callback/metrics admission ends before branch completion, and an
adaptive task's final execution release never touches Region: cancellation
may already have let the caller and tree reclaim it.

When the adaptive tree completes, the first task's existing publishing-thread
ID identifies the synchronous caller. Completion on that same thread skips the
pool notification, since it cannot also be parked waiting for itself. Remote
completion still notifies, including external callers. The parent link and
thread ID are copied before releasing the final subtree reference, which can
allow concurrent reclamation. Direct root completion precedes the caller's
wait and also needs no notification. No task or Region fields are added.

Simple and static policies have no sibling-demand feedback. Their completion
already uses Region's task count, so join storage and join reference operations
are compiled out. Publication dispatch lives in the task's Publish method.
Callback admission uses one atomic increment. If closure has already begun,
it rolls back through the normal release path, including the notification
needed by a closing caller. Closing still waits for admitted callbacks.

Pure range arithmetic, non-owning partition-state transitions, and private
range-buffer operations support constexpr evaluation. Compile-time assertions
check representative results. Runtime thread availability and affinity history
still supply runtime data; constexpr does not make those values static.

## Static and affinity placement

StaticPartitioner splits the worker budget proportionally and publishes a hint
for each resulting range. AffinityPartitioner uses oneTBB's 16 history slots per
worker, delayed demand checks, binary balancing splits, and the observed worker
when an affinity task migrates. A new pool or worker count resets the history.

An overlapping call on the same AffinityPartitioner uses separate temporary
history. The shared mutex protects the cache decision; temporary history is
allocated after releasing that mutex. This keeps nested and concurrent calls
safe without serializing their temporary allocations. History slots are atomic; only root-state copies own the allocation.
Child states borrow it under the existing Region lease. After cancellation,
queued tasks cannot access the callback, metrics, or history.

For a different target worker, a registered sender publishes one proxy to
both its local LIFO queue and a dedicated recipient mailbox, following oneTBB's
two-location affinity protocol. A packed atomic word contains the task pointer
and the two location bits. Extraction claims the useful task once and releases
that queue location; the final location recycles the proxy. Rejected sender publication claims and discards any still-unclaimed work, so
an unavailable recipient cannot strand a failed operation. The recipient
location then owns only proxy cleanup.

The affinity mailbox uses intrusive links, an atomic incoming list, and an
owner-only FIFO batch. It has no fixed slot capacity and does not route remote
proxies through the ordinary overflow deque. Thieves use the sender's local
proxy; ordinary external submissions retain their existing mailbox. Already-
claimed proxies are cleaned up during queue search rather than executed as
ordinary tasks. This preserves the sender's ability to help its newest child
during nested waits. A mailbox-only adaptation can produce unbounded helping
recursion. External callers publish directly to the target.

Placement remains a hint; it is not CPU pinning. Pool task counters count the
useful task once, and Metrics::range_tasks counts useful range tasks. Proportional range splitting uses exact rounded integer arithmetic
instead of the upstream float approximation, preserving large size_t origins
and avoiding loss of iterations.

## Small-object storage

NewSmallObject<T> and DeleteSmallObject<T> select storage at compile time.
Objects up to 256 bytes with fundamental alignment use the backend's own
per-thread cache. Oversized or over-aligned types use ordinary new/delete.
UniqueTask, partitioner Work, and Region use this storage. Final task types
also supply matching allocation/deallocation hooks, so deletion through a
virtual Task pointer remains valid. TBB is not linked or required by the allocator.

Affinity proxies use ordinary allocation. Paired measurements found that the
bounded 256-byte cache regressed their allocation pattern; keeping their
16-byte allocations separate retained the gains for useful tasks and regions.

Each owner has a private free list and an atomic list for remote returns.
Each list retains at most 256 blocks. Blocks retain their allocator owner
independently of task, pool, and thread-local lifetimes. On thread exit the
owner closes its remote list, frees cached blocks, and drops its reference.
Outstanding objects can still be freed by another thread; the final block
reclaims the closed owner. Later thread-local destructors can allocate using
standalone blocks after the local owner has closed.

Task execution and embedded peer-node completion still jointly control Work's
lifetime. Region can still outlive its synchronous caller after cancellation.
Recycling happens only after the same final releases as ordinary deletion.
Throwing object construction returns its storage to the allocator.

Define OOX_EIGEN_SMALL_OBJECT_POOL=0 consistently across a program to select
ordinary allocation for comparison or allocation-failure injection. The
allocation-failure executable uses this setting so a warmed cache cannot hide
injection points. The allocator has separate tests for constructor failure,
remote returns, cache bounds, alignment, and shutdown races.

AffinityHistory retains its shared-ownership implementation and variable-size
slot arrays. Those history allocations are separate from small task storage.

## Runtime boundary and later adaptation

The implementation uses Task, Schedule, ScheduleWithAffinity, CurrentThreadId,
NumThreads, IsCancelled, Wait, and NotifyTaskCompletion. It does not include rapid_start.h, construct
a Rapid group, publish Rapid activations, inherit Rapid worker domains, or
use a Rapid completion region. No OOX dependency or task API changes are required.

The pool retains its task-publication and cancellation contracts. Affinity
publication is integrated through ScheduleWithAffinity. Queue searches check an
atomic overflow hint before taking the overflow mutex and skip a victim whose
steal mutex is busy. Registered callers of Wait try helping before registering
as waiters. If no task is available, they retain the registration, recheck, and
parking protocol.

For later adaptation, carry parallel_for.h, tbb_partitioning.h,
small_object_pool.h, the affinity-aware pool changes, and
licenses/oneTBB-Apache-2.0.txt. The included pool supplies the existing Task type.
Preserve these contracts when adapting to another pool revision:

- Schedule, RunOnThread and ScheduleWithAffinity consume the task, including exceptional publication
  and rejection.
- Queued tasks are either executed or discarded; either path completes their
  contribution to the operation's outstanding count.
- Wait may return on cancellation while queued tasks remain in the pool.
  Adaptive tasks retain operation state through their join tree; other tasks
  retain it individually until execution or discard. Closing
  the operation blocks callback/metrics access by queued tasks and waits for
  active callbacks before returning. The callback itself is not retained.
- Registered callers help ordinary queued work while waiting, allowing nesting.
- Completion uses release/acquire ordering and notifies waiting callers.

Exceptions stop further operation work and are rethrown after active work
finishes. A cancelled operation may leave inert tasks for the pool to discard;
those tasks cannot invoke the callback or access its metrics after return.
Nested calls use ordinary task scheduling and a fresh state for their policy.
Concurrent external roots and calls into another pool are covered by tests.

## Selection

The existing scheduler-evaluation suite and the OOX PBBS adapter expose
EIGEN_AUTO, EIGEN_SIMPLE, EIGEN_STATIC and EIGEN_AFFINITY. The existing
EIGEN_STEALING baseline remains available. The oneTBB comparison modes also
include TBB_STATIC.

All modes reuse the same workloads, numeric validators, and deep-nesting cases.
The Eigen affinity adapter retains one policy object per callback type; its
overlap fallback handles reentrant invocations of that type.

## Validation

Each policy runs the generated range oracle and allocation-failure suite:
1,516 cases and 32 failure attempts per policy, including nested/concurrent
roots, cross-pool calls, unavailable workers, saturated queues, cancellation,
and reuse. Values and visits are checked against a simple serial reference.
A Boolean reference model checks all 5,040 completion orders of a three-node
embedded join tree, including storage lifetime and peer-active state.
A separate oracle checks all 40,320 orders including caller release, verifying
Region reclamation independently of task-storage reclamation, including reads
of the publishing-thread ID before the final reference release. Completion
regressions check Auto/Affinity visitation against a serial reference, suppress
same-thread notifications, and require remote notifications after registered
and external callers arm their waits. Test-only counters are compiled out of
normal builds.

Affinity-specific checks cover learned placement, sequential history reuse,
overlap isolation, pool changes, and queued tasks outliving a temporary history.
Proxy regressions cover both claim orders, a blocked recipient with more than
65,000 sender-completed tasks, overflowed sender entries, and cancellation.
The small-object allocator runs generated value/destructor oracles for local
returns, remote returns, closed owners, and owner-exit races, plus large cases.

Differential checks against the pinned oneTBB build cover auto, static and
affinity state transitions, all four policies' private-chunk counts without
stealing, and private range-buffer operations. ASan validation remains
outstanding because the local runtime stalled before main, also for an
independent minimal program. UBSan and the existing scheduler-evaluation
validators provide the local correctness checks.
