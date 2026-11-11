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

`WorkerIdleMode::ResidentAuto` bounds resident polling by the CPU allowance
observed when the pool is constructed. On Linux it reads the creator's allowed CPUs and chooses one
logical CPU per physical core from sysfs. Background workers bind themselves
when entering resident polling and restore their previous affinity on leaving
resident service at the next idle transition. The creator keeps its affinity.
Threads spawned by user callbacks on a pinned worker inherit that worker's mask.
Missing topology or rejected affinity calls allow execution with the OS placement.
Other platforms use the hardware concurrency estimate. CPU bandwidth quotas
require separate accounting. `NumThreads()` still reports the requested pool size. Workers above
the resident limit use ordinary task execution and parking. When the pool exceeds
the CPU allowance, those workers park immediately after unsuccessful searches.
