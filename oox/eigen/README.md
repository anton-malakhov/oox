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

`rapid_start.h` builds a reentrant rapid-region layer on this pool. Workers keep
their pool-lifetime, generation-stamped registrations; loop invocations do not
register or trap workers. Immutable groups name contiguous domains, activation
trees split both workers and iterations proportionally, and TLS region contexts
propagate subdomains into nested loops. A per-worker atomic inbox with a bounded
lock-free overflow is checked with a fairness budget before ordinary work. If
that bounded rapid path fills, the activation's embedded ticket falls back to
the ordinary queue. Descriptors come from a preallocated slab with an
ABA-stamped free-list head. A completion ticket makes the transition to zero
the unique descriptor-recycling claim, and completion follows the activation
tree.
Optional elastic lending leases one balanced topology subtree with one stamped
CAS.

Three parallel-for policies share that activation layer. `ParallelFor` keeps
each proportional range inside Rapid Start for its whole lifetime.
`ParallelForMailbox` uses Rapid Start only to publish one range root per worker,
then executes recursively divisible tasks through the ordinary mailboxes and
deques. `ParallelForLazyStealing` begins each range inside its Rapid domain and
leaves that domain only when its local work is exhausted and a global steal is
required. Both hybrid policies preserve nested calls, exception propagation,
pool cancellation, and caller-supplied grain sizes.

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
