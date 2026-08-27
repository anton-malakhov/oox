# Vendored Eigen-derived work-stealing scheduler

This directory contains OOX's private scheduler port. Its immediate source is
[`EgorkaZ/pbbsbench`](https://github.com/EgorkaZ/pbbsbench/tree/396a299f03c58dbe9e7604daab38a65781227b75/parlaylib/include/parlay/internal/scheduler_plugins/eigen),
pinned at commit `396a299f03c58dbe9e7604daab38a65781227b75`.
The mailbox changes first appeared there in commit `e857cdd`, although OOX now
adds a guarded overflow queue when the copied bounded publication paths fill.

All implementation symbols live in `oox::detail::eigen_pool`; none are part of
Eigen's namespace or OOX's public API. Workers briefly spin only when requested,
then park with C++20 atomic wait/notify. Queue publication advances a worker
generation without taking a global mutex, and task completion only notifies
registered waiters. Published-task accounting keeps workers alive during
destructor draining and nested waits. Full local deques and mailboxes spill to an
unbounded guarded queue, so nested spawning never falls back to recursive inline
execution.

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
