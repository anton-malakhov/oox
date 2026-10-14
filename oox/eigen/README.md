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
| `resident_command.h` | Resident command types extracted from the pool | MPL-2.0 |
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

Launches do not wait for busy workers to become available. Tests and benchmarks
may explicitly call `eigen_test_support::WaitForResidentWorkers` from
`test_support/eigen_resident.h`, with a caller-supplied startup timeout.
That helper observes readiness but reserves nothing; it is not installed with
the scheduler. Ordinary launches have no readiness barrier or timeout.

The caller joins helpers before returning or rethrowing. Same-state nested
calls run directly; concurrent roots reserve disjoint helpers. With no helper
available, the caller executes the range. One launch uses at most 64 participants;
larger pools are supported. Default parking pools reject resident-group calls.

An idle resident acquires ordinary work before withdrawing from the group.
Failed stealing does not deregister it. If a Rapid command wins that race, the
ordinary task remains visible in a bounded deferred slot for other helpers.
The caller also helps queued work while joining. Cancellation prevents new
callback entry but does not interrupt a running body.

Busy residence consumes idle CPU and is not the default policy. The group
performs initial distribution only; it adds no stealing or grain-size policy.

## Rapid activation and lazy range stealing

`rapid_auto.h` adds `rapid::ParallelForAuto(group, begin, end, callback, grain)`.
Its execution has four stages:

1. Capture available helpers and divide the range equally among them and the
   caller. A stack descriptor computes each initial range on demand; it builds
   no task frontier or join tree.
2. Registered owners expose their unreserved tail through a protected worker
   slot. An idle helper can steal half that tail. Only successful theft allocates
   an ordinary task and attaches a completion tree to `Region`.
3. Each finished helper advertises availability again. Eligible shared cohorts
   on pools of at most eight workers combine readiness and completion in one
   bitmap update; other cohorts use per-worker epochs or a counter.
4. The caller joins captured commands, then any ordinary descendants. Descendants
   retain heap-owned `Region`, never the stack descriptor. Cancellation closes
   callback admission and waits for running callbacks before returning.

This reuses Auto task storage, budgets and lifetime machinery, but local batch
growth and tail theft are a lazy range policy, not the unmodified Auto buffer
algorithm. Ordinary Auto retains its private range buffer; on resident pools,
waiting-helper hints can also trigger donation after a sibling finishes.
Nested calls and unavailable helpers use ordinary Auto; two grain-sized halves
have a stack-only single-helper fast path.
An unregistered caller instead shares work through bounded ordinary publication.
The general path still allocates `Region`. No unbounded queue is introduced.

See [PARTITIONERS.md](PARTITIONERS.md#runtime-boundary-and-later-adaptation) for
reservation ordering, descriptor lifetime, callback admission and completion.
The optional `activated` output counts captured helper commands;
`Metrics::range_tasks` counts allocated range tasks, not logical splits.

## Rapid with an explicit partitioner

Include `rapid_parallel_for.h` and call
`rapid::ParallelFor(group, begin, end, callback, policy, grain)` with
`AutoPartitioner`, `SimplePartitioner`, `StaticPartitioner`, or a persistent
`AffinityPartitioner`. This is a separate entry point, not a replacement for
the optimized lazy `ParallelForAuto` above.

The policy creates **one** initial state. Rapid splits a bounded prefix of that
state's tree and delivers those seeds directly to captured helpers and the
caller. Each owner continues through the existing `Process` executor. Initial
activation replaces task publication for that prefix only; it does not give
each worker a new pool-sized split budget.

| Explicit policy | Prefix and subsequent work |
| --- | --- |
| Auto | Binary prefix; remaining budget, range buffer and ordinary task stealing |
| Simple | Binary prefix; ordinary recursive splitting to grain |
| Static | Proportional prefix, including odd worker counts; fixed remaining splits |
| Affinity | Proportional prefix; history-based placement, then adaptive range buffer |

Static/Affinity seeds prefer their recorded worker if that worker was captured;
unavailable workers are hints, never a reason to wait. Affinity history is shared
across sequential calls and isolated for overlapping calls, as in ordinary
execution. Nested calls and unavailable helpers use the **selected** ordinary
policy. The resident domain limits initial capture, not later ordinary stealing.

Explicit Auto is not upstream Eigen's loop partitioner: it is this backend's
ported Auto policy. It also is not the lazy-tail policy of `ParallelForAuto`.
Keep those names separate when interpreting benchmarks. The generic entry has
additional seed preparation and completion costs; it is not a universal speedup.

## Implementation map

Include `parallel_for.h`, `rapid_auto.h`, or `rapid_parallel_for.h` to execute loops. Supporting headers
are internal; their standalone syntax does not make them independent loop APIs.

| Header | Responsibility |
| --- | --- |
| `rapid_auto.h` | Capture owners, assign initial ranges, join the launch |
| `rapid_parallel_for.h` | Explicit-policy launch, admission and two-stage join |
| `rapid_partitioning.h` | Bounded policy prefix and captured-worker placement |
| `rapid_start.h` | Group API, resident ancestry and captured-command completion |
| `parallel_for.h` | Policy API, shared `Process` boundary and `RunAdaptiveRange` |
| `partitioner_region.h` | Callback admission, cancellation and operation lifetime |
| `partitioner_completion.h` | Completion-tree references and task reclamation |
| `partitioner_task.h` | Range/task representation and ordinary publication |
| `lazy_range_executor.h` | Owner batching/tail theft and separate `RunCooperativeRange` fallback |
| `resident_command.h` | Pool-independent resident command types |
| `stealable_range.h` | Tagged worker slot and stack-descriptor lifetime |

For review, start with `ParallelForAuto` and `batch_detail::Execute`, then follow
`RunLazyRange`, `Process`, and the completion types. Read the pool's claim,
publication and `WaitResident` paths last, using these contracts as checkpoints:

| Contract | Where it is enforced |
| --- | --- |
| Every captured helper receives a command, including during cancellation | Claim followed by non-throwing publication; `WaitResident` cannot exit with a claimed bit |
| A thief cannot access a destroyed stack descriptor | `RunLazyRange` closes registration before destruction; `StealableRange` serializes theft and unpublication |
| No initial owner can create a tree after the final Region check | `Execute` joins `ResidentCohort` first, then joins or closes `Region` |
| Callback/metrics access ends before the tree may release Region | `Process` destroys `WorkLease` before `Branch` |
| Worker reuse does not erase completion of an older cohort | Sticky done bits or monotonic epochs; readiness alone is not completion |

## Automatic hardware calibration

`rapid_auto_calibration.h` provides explicit `rapid::CalibrateAutoGroup` for a
quiet, serialized startup context. It compares launch, uniform and skew probes
under a soft 50 ms budget, preferring smaller cohorts within a 5% score band.
It is not per-loop workload profiling or a performance guarantee. No completed
trial or cancellation selects zero residents; allocation failure also resets
residency to zero before propagating.

`ResidentLimit()` selects a logical worker-ID prefix, not CPU affinity. Other
workers park when idle but can execute ordinary tasks. Reducing the limit does
not interrupt captured commands. Busy residence consumes CPU and is opt-in.

Benchmark `RAPID_AUTO` enables startup calibration by default;
`EIGEN_AUTO_RESIDENT` uses ordinary Auto on the same resident-capable pool.
Set `OOX_RAPID_RESIDENT_LIMIT=N` for a matched fixed-cohort comparison; it disables
calibration. `OOX_RAPID_AUTOCALIBRATE=0` also keeps fixed settings. `EIGEN_AUTO`
remains the separate parking-pool baseline.
