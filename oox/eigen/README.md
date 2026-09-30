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

## Rapid activation with the existing AutoPartitioner

`rapid_auto.h` adds `rapid::ParallelForAuto(group, begin, end, callback, grain)`.
It creates one ordinary AutoPartitioner root. Child tasks inherit its split
state, private range buffer and sibling-demand feedback through the existing
executor. It does not implement a second range controller, measure callback
duration or use a calibration multiplier.

If one binary split produces two grain-sized, indivisible halves, the launch
uses a stack resident command and captures exactly one helper. It preserves
Auto's floor/ceil midpoint and joins the helper even after cancellation or an
exception. There are no descendant tasks from that terminal split, so it needs
neither a heap Region nor a heap frontier. Nested calls still use ordinary Auto.
If no helper can be captured, ordinary Auto handles the call. This is a range/
grain rule, not a workload-name or timing heuristic.

The initial frontier is built in one launch-owned allocation. Available workers
are captured in one batch, with at most 63 helpers. Prefix ranges and states use
the existing binary split rules. Where a branch has at most one worker's Auto
budget, its remaining initial subdivision is also prebuilt. Larger budgets stay
with ordinary Auto so that work spreads to other workers. This bounds the
prebuilt frontier by 256 leaves, including for larger pools. Terminal
sibling pairs have a direct owner continuation and a stealable continuation
embedded in the same allocation. No readiness barrier runs in a launch.
Each owner publishes its queued leaves
in one batch and shares callback admission only across synchronous direct leaves.
A single-helper launch prefers the first ready recipient; multi-helper launches
rotate their starting point. A busy preferred recipient is never awaited.

Direct and locally recovered initial continuations resume after initial division
without repeating task-entry steal checks. A genuinely stolen continuation runs
the usual Auto steal check and can signal sibling demand. Subsequent subdivision
uses ordinary Auto Work tasks with no dispatcher pointer or publication hook.
This changes initial activation and ownership handling. The existing range buffer
also responds to a waiting helper after its sibling has completed, allowing the
last busy branch to donate its tail through ordinary bounded queues. The waiter
count is a demand hint, not a reservation or a guarantee that stealing succeeds.

There are still a Region allocation and a frontier allocation; this is not an
allocation-free loop API. Frontier storage is selected from capacities of
2, 8, 16 or 64 participants using the maximum eligible helper count and range
size. A later eligibility change cannot exceed the capacity passed to capture.
Completion on the originating caller skips notification, as in ordinary Auto;
remote completion still wakes registered or external waits. External initial
publishers use ordinary scheduling instead of treating an invalid worker ID as
an affinity hint.

There is one shared Auto completion tree, not one new pool-sized Auto root per
resident. A command executes its initial ranges under task-ancestry tracking;
nested calls use the ordinary Auto path. The shared prefix remains alive until
the caller, Auto tree and embedded queued continuations have all released it.
Cancellation can return before queued tasks are discarded, so the prefix cannot
live on the caller's stack. Pinned join nodes keep their execution bit set; their
shared completion record releases the tree reference without treating those
nodes as allocated Work objects.

Helpers use the fixed-command resident return path: they advertise availability
before acknowledging completion. There is no mandatory ordinary steal round
between a finished command and renewed availability. Existing resident checks
still release them for cancellation and residency changes. Ordinary publication
only signals a queue probe: an idle resident first acquires an ordinary task,
then atomically withdraws its availability before executing it. A failed steal
does not withdraw the worker. If a Rapid publisher wins that arbitration, its
command takes priority; the acquired ordinary task is published in one bounded
atomic slot on that worker, available to other thieves and nested helping.
This avoids hiding a task that the winning command may need to join. The slot
retains the task's original accounting and participates in cancellation draining.
No unbounded overflow queue is introduced.

Benchmark modes `RAPID_AUTO` and `EIGEN_AUTO_RESIDENT` use resident-capable pools.
The former calibrates its cohort at startup by default; the latter runs ordinary
Auto without Rapid transport or automatic calibration. Explicitly fixing the
same cohort in both provides a matched idle-policy control.
`OOX_RAPID_RESIDENT_LIMIT=N` sets the cohort in both.
`OOX_RAPID_AUTOCALIBRATE=0` keeps fixed settings; an explicit resident limit
also disables calibration.
The usual `EIGEN_AUTO` remains a separate parking-pool comparison.

Ordinary pool task counters exclude tasks transported directly through resident
commands. The optional `activated` output of `ParallelForAuto` counts those
commands; partitioner `Metrics::range_tasks` counts allocated Auto Work objects,
not compact prefix nodes or embedded continuation tasks. Ordinary task counters
include the latter when queued. Do not confuse these distinct counts with the
total number of logical splits.

## Automatic hardware calibration

`rapid_auto_calibration.h` provides `rapid::CalibrateAutoGroup(group, options)`
for the batched Auto kernel. Applications call it explicitly from a
serialized, quiet startup/control context on the full pool. It compares launch,
uniform and two-ended skew probes, taking a median of three measurements after
warmup. Cohorts zero through sixteen are considered individually, followed by
larger powers of two and the maximum, capped at 64. Within the default 5%
geometric-mean band it prefers fewer resident workers. This is an empirical
startup policy, not workload profiling or a per-loop search. No timed-chunk
multiplier is selected for Auto.

The Auto search has a soft 50 ms budget checked between synchronous probes;
started work is always joined. Unready cohorts are skipped after a short
readiness attempt. With no completed trial, or on cancellation, it selects zero
residents (ordinary Auto on the resident-capable pool). Allocation failure resets
residency to zero before propagating. Results and benchmark metadata record
selected limits, trial counts, startup time and budget exhaustion. Recalibrate
explicitly at a quiet point if resource allocation or machine load changes.

Short startup probes can select a cohort that performs poorly on later workloads.
Do not treat a completed calibration as a performance guarantee; validate its
choice on representative workloads and retain an explicit limit override.

Workers outside the selected cohort park when idle but remain available for
ordinary tasks and stealing. `ResidentLimit()` is a logical worker-ID prefix,
not CPU affinity; with a main-thread slot, a limit of two normally means one
background spinner plus the caller. Limit zero uses the ordinary auto partitioner.
In-flight resident commands finish even if eligibility is reduced.

Queued seeds keep the callback as an opaque pointer until the callback lease is
acquired. Cancellation may end callback storage before a dequeued-but-not-started
seed runs; that seed must reject admission without forming a callback reference.
