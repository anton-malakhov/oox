# Patent-based work-demand range policy

The Eigen backend now provides `rapid::ParallelForPatent` in
`oox/eigen/patent_parallel_for.h`. It is built on the existing Rapid worker
domain and activation machinery. Ordinary OOX task scheduling and dependency
handling are unchanged.

This operation needs the original iteration range and callback. It controls
partitioning before per-iteration tasks are created; it cannot recover that
information from an arbitrary queued task pointer.

## Algorithm

The technical basis is Malakhov and Kukanov's
[work-demand partitioning patent](https://patents.google.com/patent/WO2013021223A1/en)
(US9262230B2; WO2013021223A1). The implementation uses:

- Initial proportional distribution over the effective worker domain.
- An owner-private, bounded range buffer.
- A signal task that reports demand when another thread executes it.
- FIFO donation of pending ranges, LIFO local work, and a depth increase on demand.
- Direct execution when a range is indivisible.

The default initial relative depth is three, corresponding to an initial
private subdivision density of eight per owner. When demand arrives, the owner
can stop between iteration callbacks and subdivide the unexecuted remainder.
An indivisible remainder always executes, which prevents a stream of signals
from stopping progress. The caller's grain is the range's split threshold.

The range buffer is adapted from the pinned oneTBB implementation documented in
`tbb_partitioning.h`. The signal protocol and callback-boundary checkpoints
operate on ranges; the earlier ready-task grouping adapter is not used.

## Selection

The existing scheduler-evaluation mode is `RAPID_PATENT_DEMAND`.
The existing direct-loop harness mode is `EIGEN_PATENT_DEMAND_LOOP`.

For direct backend use, create a `rapid::RapidDomainState` for the pool,
a `rapid::RapidStartGroup` describing its domain, and call
`rapid::ParallelForPatent(group, begin, end, callback, grain)`.

The call is synchronous. Iteration callbacks must be safe to execute in
parallel. Exceptions cancel the operation's remaining work and are rethrown to
the caller. Cancellation and demand checkpoints occur between callbacks;
a running callback is not preempted. Nested calls use the current logical
domain. Concurrent roots and calls involving another pool are supported.

The optional template parameters keep block-end checking and feedback-disabled
variants available for controlled comparisons. They are experimental controls,
rather than additional recommended defaults.

## Why the earlier port could not reproduce Rapid's large gains

The original `EIGEN_STEALING` range benchmark can create a scheduling task for
almost every iteration at grain one. The OOX-task benchmark already coarsens
its loop before it reaches the pool. Grouping those existing tasks adds another
allocation and synchronization layer without removing their creation.

This policy instead retains most subdivisions as private range values and only
publishes range work in response to demand. The large speedups are reductions
in scheduling overhead; they are not universal numerical-kernel speedups.

## Controlled local results

Nine closely interleaved repetitions reused the same worker pool for every
policy. Every result was checked against a serial oracle. Each case contains
262,144 iterations; times are median milliseconds.

| Workers | Workload | Grain-one Eigen | Patent default | Coarse Eigen control |
|---:|---|---:|---:|---:|
| 8 | Cheap body | 33.615 | 0.0627 | 0.0514 |
| 12 | Cheap body | 36.838 | 0.0599 | 0.0552 |
| 8 | Uniform cost | 32.428 | 1.830 | 1.889 |
| 12 | Uniform cost | 36.574 | 1.371 | 1.411 |
| 8 | Front-heavy cost | 33.952 | 1.761 | 3.297 |
| 12 | Front-heavy cost | 37.909 | 1.278 | 3.319 |

The cheap case improves roughly 536x/615x over the grain-one baseline.
The front-heavy case also improves roughly 1.87x/2.60x over the coarse control.
Coarse partitioning remains cheaper on some uniform cases, so this is not a
claim of a universal win over tuned partitioning.

The preceding broad, separate-process native run had high dispersion on this
desktop and is retained as diagnostic data. Prefer the same-pool comparisons
for smaller differences. The host is an Apple M4 Max, with other desktop
applications active; these are local results, not a dedicated-machine result.

## Validation

The branch adds deterministic serial-oracle checks for small and large ranges,
range origins near the integer limit, nested loops, concurrent roots,
cross-pool calls, descriptor scarcity, cancellation, and progress with most
workers unavailable. A separate test injects allocation failures and verifies
exception propagation and reuse.

Local checks passed: 121 Release tests, 215 exception-enabled UBSan tests,
and the four existing benchmark entry-point smoke/validator tests for this mode.
The generated prototype audit covered 1,490 cases and over five million items
before integration. A Linux CI run is still required for this new branch.

## Native confirmation

The final same-pool comparison is recorded in [PATENT_DEMAND_RESULTS.md](../../benchmarks/scheduler_eval/PATENT_DEMAND_RESULTS.md). It uses seven interleaved repetitions and includes held-out workloads. At twelve workers, launch improved about 379x, Scan/20 about 259x, grid BFS about 2.03x, and changing worker availability about 6.35x relative to the existing EIGEN_STEALING provider.
