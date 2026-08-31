# Rapid Start versus the normal Eigen backend

## Executive summary

This branch turns the Eigen backend from a conventional recursive
work-stealing pool into a scheduler with a second, composable fast-distribution
path. The normal Eigen policy remains available as the baseline. Three Rapid
policies cover different workload shapes:

- `RAPID_START` assigns proportional ranges through a hierarchical activation
  tree and keeps them static.
- `RAPID_MAILBOX` uses Rapid distribution to publish a bounded set of adaptive
  blocks into ordinary worker mailboxes, then uses unrestricted stealing.
- `RAPID_LAZY_STEALING` begins in proportional Rapid ranges and exposes each
  range only after its owner claims the first block. A worker deregisters from
  its Rapid domain only when it has exhausted local work and a started peer has
  unclaimed blocks.

The result is not one universally best policy. Static Rapid has the lowest
uniform-loop overhead. Lazy Rapid is the best general-purpose compromise in
these measurements: it retains most of the launch advantage while matching
normal Eigen on balanced and irregular SpMV. Mailbox mode is useful when work
must become ordinary pool work immediately.

## What changed relative to `main`

The branch adds a modular Eigen pool backend and a reentrant Rapid scheduler.
Workers register once for the pool lifetime; a parallel invocation no longer
needs to trap and release the team. Rapid activation uses per-worker atomic
inboxes, a bounded overflow path, a preallocated descriptor slab, and
cache-line-isolated activation records. Nested regions inherit proportional
contiguous subdomains instead of creating unrelated root teams.

The implementation also hardens the ordinary pool paths used by both the new
and normal policies. Publication and cancellation have an explicit ordering;
fallback tickets are drained; completion owns descriptor recycling; parked
worker and external-waiter wakeups cannot consume one another's notifications;
queue overflow preserves progress without recursive inline execution; and
ordinary work receives a fairness opportunity during sustained Rapid traffic.

The two hybrid policies originally exposed recursive grain-one task trees.
That recovered imbalance but made a 262K empty loop take roughly 40--43 ms.
The final implementation replaces that frontier with adaptive blocks. Mailbox
mode creates at most a bounded oversubscription of flat tasks. Lazy mode creates
no ordinary tasks: workers claim blocks through cache-line-separated atomic
cursors and make a one-way transition to unrestricted stealing only when it is
useful. Both policies honor the caller's grain as a minimum block size.

## Direct backend loop results

The following medians come from `bench_loops`, which invokes the backend
directly rather than going through OOX. The run used Release mode, 16 workers,
seven repetitions, and a 50 ms minimum sample time on the local Apple Silicon
host. Times are microseconds; speedup is relative to the normal Eigen
work-stealing mode built from the same commit.

| Iterations | Normal Eigen | Static Rapid | Speedup | Mailbox | Speedup | Lazy | Speedup |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 64 | 25.38 | 12.29 | 2.07x | 28.45 | 0.89x | 16.53 | 1.54x |
| 512 | 59.46 | 12.72 | 4.67x | 47.38 | 1.25x | 19.79 | 3.00x |
| 4,096 | 256.81 | 13.77 | 18.65x | 119.18 | 2.15x | 23.88 | 10.75x |
| 32,768 | 1,855.03 | 15.89 | 116.73x | 127.92 | 14.50x | 30.99 | 59.85x |
| 262,144 | 13,787.40 | 24.07 | 572.85x | 451.46 | 30.54x | 71.69 | 192.33x |

The normal Eigen loop recursively splits to grain one, so scheduler work grows
with the iteration count even when the body is almost empty. Static Rapid
publishes a worker-sized activation tree and then runs contiguous ranges.
Lazy Rapid adds only bounded atomic block claims, while mailbox mode pays for
ordinary task allocation and queue publication. That is why mailbox loses the
64-item case but becomes substantially faster as the loop grows.

## Balanced and irregular SpMV

The scheduler suite also measures useful work, where low launch time alone is
not enough. These are matched medians for 131,072-row deterministic CSR
matrices from the same seven-repetition run. Times are milliseconds.

| Distribution | Normal Eigen | Static Rapid | Mailbox | Lazy |
| --- | ---: | ---: | ---: | ---: |
| Balanced | 9.685 | 13.025 | 9.346 | 9.678 |
| Hyperbolic | 5.679 | 28.728 | 5.669 | 5.545 |
| Triangular | 7.298 | 10.983 | 7.343 | 7.321 |

Static Rapid is deliberately static, so the hyperbolic matrix exposes its
failure mode: a few heavy leading rows make it about 5.1x slower than normal
Eigen. Both hybrids remove that cliff. Mailbox is within about one percent of
normal Eigen on all three shapes. Lazy is effectively tied on balanced and
triangular inputs and is about 2.4% faster on the hyperbolic input. Thus the
lazy policy combines the direct-loop advantage with the load balance expected
from the normal work-stealing backend.

These macOS results are development evidence, not cross-machine constants.
Affinity is a no-op on this host, background load and heterogeneous cores add
variance, and absolute times should not be combined with older runs. The
same-commit normal Eigen binary is intentionally used as the policy baseline;
it is more controlled than comparing binaries from different historical
checkouts and also includes the branch's shared pool correctness fixes.

## Correctness and CI evidence

The final implementation passed:

- 114/114 tests in both Clang Debug and Clang Release configurations;
- 23/23 Release scheduler-evaluation oracle, nesting, runner, and smoke tests;
- 20 complete Rapid-suite repetitions under UBSan during tuning, followed by
  five repetitions on the final cache-aligned coordinator;
- 200 repetitions of the no-stall/no-deregistration transition test;
- 100 repetitions of a deterministic test that blocks a range owner and
  verifies another worker steals only later, unclaimed blocks;
- exception propagation and reuse, nested hybrid loops, exact-once visitation,
  concurrent roots, descriptor scarcity, queue saturation, and pool
  cancellation.

AddressSanitizer and ThreadSanitizer were also attempted earlier in the branch
work, but both runtimes fail before test execution on this Apple host. They are
not counted as passing evidence. The GitHub Linux GCC and Clang matrix remains
the authoritative portability check.

## Choosing a policy

Use static `RAPID_START` for uniform loops when the proportional partition is
known to be balanced. Use `RAPID_LAZY_STEALING` as the default candidate when
row or iteration cost may vary: it starts on the fast path and pays the global
stealing transition only when useful peer work exists. Use `RAPID_MAILBOX`
when immediately converting the Rapid partition into ordinary queue work is
valuable, or when interaction with other ordinary tasks is part of the desired
scheduling behavior. Keep normal Eigen available for workloads whose behavior
has not yet been characterized; the benchmark suite now makes that comparison
reproducible rather than implicit.
