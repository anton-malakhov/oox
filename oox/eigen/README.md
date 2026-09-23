# Vendored Eigen-derived work-stealing scheduler

This directory contains OOX's private scheduler port. Its immediate source is
[`EgorkaZ/pbbsbench`](https://github.com/EgorkaZ/pbbsbench/tree/396a299f03c58dbe9e7604daab38a65781227b75/parlaylib/include/parlay/internal/scheduler_plugins/eigen),
pinned at commit `396a299f03c58dbe9e7604daab38a65781227b75`.
The mailbox changes first appeared there in commit `e857cdd`, although OOX now
uses inline backpressure when bounded publication paths fill.

All implementation symbols live in `oox::detail::eigen_pool`; none are part of
Eigen's namespace or OOX's public API. Workers briefly spin only when requested,
then park with C++20 atomic wait/notify. Queue publication advances a worker
generation without taking a global mutex, and task completion only notifies
registered waiters. Published-task accounting keeps workers alive during
destructor draining and nested waits. Local queues and affinity mailboxes are
bounded. A rejected task runs inline after publication admission is released.
Cancellation closes publication before draining, including concurrent or
reentrant cancellation; arbitrary nested callbacks can still recurse inline.

## File provenance and license

| File | Source | License retained |
| --- | --- | --- |
| `nonblocking_thread_pool.h` | Eigen `NonBlockingThreadPool.h`, with PBBS and OOX adaptations | MPL-2.0 |
| `run_queue.h` | Eigen `RunQueue.h` | MPL-2.0 |
| `max_size_vector.h` | Eigen `MaxSizeVector.h` | MPL-2.0 |
| `memory.h` | Eigen `Memory.h` aligned allocation helpers | MPL-2.0 |
| `stl_thread_env.h` | Eigen `ThreadEnvironment.h` | MPL-2.0 |
| `mpmc_queue.h` | Erik Rigtorp's MPMCQueue, imported through PBBS | MIT |
| `stack_depth.h` | OOX portability helper; no longer used for scheduler progress | Apache-2.0 |

The full MIT notice for Rigtorp's queue remains in `mpmc_queue.h`; MPL notices
remain in every Eigen-derived file.

`OOX_EIGEN_CACHE_LINE_SIZE` can override the stable 64-byte cache-line layout
constant used by the vendored MPMC queue. Its value must be a power of two no
greater than 128, matching the vendored aligned allocator's supported range.

## Range partitioners

[ParallelFor and partitioners](PARTITIONERS.md) provide auto, simple, static,
and affinity policies adapted from oneTBB on the ordinary scheduler. They reuse
the existing scheduler-evaluation workloads.

## Fast resident groups

An opt-in pool using `WorkerIdleMode::ResidentBusy` advertises idle workers.
`rapid::ParallelForResidentRanges(group, begin, end, callback)` captures
available helpers and invokes `callback(first, last)` once per participant,
partitioning by the captured count rather than the nominal pool size.
A warm launch uses a stack descriptor and no ordinary task allocation.

No readiness barrier is required: launching does not wait for busy workers to
become available. Test and benchmark startup may explicitly call
`eigen_test_support::WaitForResidentWorkers` from
`benchmarks/eigen/resident_test_support.h`, with a caller-supplied timeout.
That polling helper observes an idle snapshot, reserves nothing, and is not
installed with the scheduler. A timed-out warmup throws; ordinary launches
have no such readiness timeout. They still join their captured work below.

The caller joins helpers before returning or rethrowing. Same-state nested
calls run directly; concurrent roots reserve disjoint helpers. With no helper
available, the caller executes the range. One launch uses at most 64 participants;
larger pools are supported. Default parking pools reject resident-group calls.

Ordinary publication releases residents into the scheduler. Idle workers rejoin
immediately; periodic queue probes cover publication/registration races. The
caller waits with processor hints and periodically helps queued work. Callbacks
may use the ordinary demand partitioner, and own their cancellation safe points:
cancellation prevents new callback entry but does not interrupt a running body.

Busy residence consumes idle CPU and is not the default policy. The group
performs initial distribution only; it adds no stealing or grain-size policy.

## Experimental mailbox handoff

`rapid_mailbox.h` adds `rapid::ParallelForMailbox`. Its Rapid participants
produce ordinary, stealable tasks: self-submissions use the owner's local deque,
while remote submissions use bounded worker mailboxes. As an exception, captured
participants process their own initial range directly. Range owners donate unfinished work
when resident or parked workers appear idle, using measured callback cost to
adjust private chunks. Ranges no larger than the grain run inline without
activating a group.

* `MailboxHandoff::Immediate` leaves Rapid participation after production and
  resumes the normal scheduler. Benchmark mode: `RAPID_MAILBOX_EAGER`.
* `MailboxHandoff::LocalFirst` processes local queues first and leaves before
  attempting a remote steal. Benchmark mode: `RAPID_MAILBOX_LOCAL`.

The producer domain is not an execution-affinity restriction: seeds target all
pool workers, including uncaptured ones, and can be stolen. The opt-in
`prefer_nonmembers` argument gives workers outside the captured-membership
snapshot two seeds and twice the initial work share instead of one. Benchmark
runs enable it with `OOX_RAPID_MAILBOX_PREFER_NONMEMBERS=1`. This is a hypothesis
to measure, not a load estimate: an uncaptured worker may already be busy.

Queue bounds are unchanged. Saturation ends participation before running the
rejected task inline. A nested wait also leaves before its first remote steal.
Calls made from an ordinary task use auto partitioning directly instead of
recursively activating more groups. Thread registration and these dynamic
execution contexts are tracked separately.

Deregistration does not release borrowed callback storage: the caller first
joins every producer command, then joins/closes the adaptive task region before
returning or rethrowing. Both modes can allocate seed tasks, unlike fixed-range
`ParallelForResidentRanges`; their startup costs must be measured separately.

## Automatic hardware calibration

`rapid_calibration.h` provides `rapid::CalibrateMailbox(group, policy, options)`.
Call it once, from a serialized control/startup context, with the full pool's
domain. It measures launch, uniform-work and skewed-work probes on the current
machine and load, then selects a resident-cohort limit and polling-cost multiplier.
The benchmark mailbox runtimes call it automatically during initialization.
Ordinary default thread pools still use parking; calibration is opt-in for
applications using these internal scheduler APIs.

Workers outside the selected cohort park when idle but remain available for
ordinary tasks and stealing. `ResidentLimit()` is a logical worker-ID prefix,
not CPU affinity; with a main-thread slot, a limit of two normally means one
background spinner plus the caller. Limit zero uses the ordinary auto partitioner.
In-flight resident commands finish even if eligibility is reduced.

Each thread measures polling/clock cost and scales the selected budget by that
cost. Chunks start at the requested grain and adapt from elapsed callback time;
there is no CPU-model table or fixed microsecond chunk target. The local cost
cache is keyed by pool generation and resident limit, including pool-address reuse.
The probe grid and a default 5% tolerance are policy choices, not a guarantee of
optimal performance for every workload. Calibration favors the smaller cohort
within that band and, for equal cohorts, less frequent polling.

The default startup probe budget is 50 ms, checked between synchronous probes.
Every started probe is joined, so OS preemption can extend total wall-clock time.
Unavailable cohorts are skipped instead of waiting five seconds or throwing a
busy-pool error. With no completed trial, calibration selects ordinary auto.
Loop launches never run this startup search. Recalibrate explicitly at a quiet
control point if the machine's load or resource allocation changes significantly.

The result records completed trials, selected settings, elapsed startup time and
budget/cancellation status. Benchmark output additionally records these settings
outside the timed loop. For reproducible diagnostic runs,
`OOX_RAPID_AUTOCALIBRATE=0` keeps fixed settings;
`OOX_RAPID_RESIDENT_LIMIT=N` selects a fixed cohort and disables startup search.
These environment controls belong to the benchmark adapter, not the core API.

Queued seeds keep the callback as an opaque pointer until the callback lease is
acquired. Cancellation may end callback storage before a dequeued-but-not-started
seed runs; that seed must reject admission without forming a callback reference.
