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
- `RAPID_LAZY_STEALING` reserves the first block of every proportional range
  for its owner before publishing execution. A worker deregisters from its
  Rapid domain only when it has exhausted local work and a peer has unclaimed
  later blocks.

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

Lazy mode reserves all owner blocks before publishing its single execution
tree. This closes a delayed-owner race: a fast peer can finish before another
owner is scheduled, but it can still safely claim later blocks because the
delayed owner's first block is already protected. Forced descriptor scarcity
may group several ranges into one activation without changing that invariant.

Mailbox tasks remain ordinary, globally stealable work, but now carry their
logical proportional domain separately from the Rapid region context. This
prevents an already-parallel outer task from expanding every nested loop back
across the whole pool. The context is scoped to its owning pool, so a callback
may still invoke a different pool independently. Rapid region contexts carry
the same ownership tag, preventing static or lazy nested calls from inheriting
a foreign pool's domain. Mailbox and lazy modes also run a one-worker effective
domain directly, without task or coordinator allocation, and check
cancellation between serial iterations. Finally, mailbox mode publishes half
as many tasks when there are at most 64 iterations per worker; larger and
irregular loops retain the finer balance-preserving density.

## Direct backend loop results

The following medians come from `bench_loops`, which invokes the backend
directly rather than going through OOX. The final run used Release mode, 16
workers, nine repetitions, and a 100 ms minimum sample time on the local Apple
Silicon host. Times are microseconds; speedup is relative to the normal Eigen
work-stealing mode built from the same commit.

| Iterations | Normal Eigen | Static Rapid | Speedup | Mailbox | Speedup | Lazy | Speedup |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 64 | 14.80 | 7.45 | 1.99x | 14.90 | 0.99x | 9.86 | 1.50x |
| 512 | 53.70 | 6.65 | 8.08x | 30.90 | 1.74x | 16.50 | 3.25x |
| 4,096 | 249.00 | 7.17 | 34.73x | 126.00 | 1.98x | 33.40 | 7.46x |
| 32,768 | 1,802.00 | 11.80 | 152.71x | 135.00 | 13.35x | 40.10 | 44.94x |
| 262,144 | 14,230.00 | 43.50 | 327.13x | 467.00 | 30.47x | 106.00 | 134.25x |

The normal Eigen loop recursively splits to grain one, so scheduler work grows
with the iteration count even when the body is almost empty. Static Rapid
publishes a worker-sized activation tree and then runs contiguous ranges.
Lazy Rapid adds only bounded atomic block claims, while mailbox mode pays for
ordinary task allocation and queue publication. That is why mailbox loses the
most ground at 64 items, where the short-loop density tuning brings it to an
effective tie with normal Eigen.

## Balanced and irregular SpMV

The scheduler suite also measures useful work, where low launch time alone is
not enough. These are matched medians for 131,072-row deterministic CSR
matrices from the same final run. Times are milliseconds.

| Distribution | Normal Eigen | Static Rapid | Mailbox | Lazy |
| --- | ---: | ---: | ---: | ---: |
| Balanced | 9.721 | 13.158 | 9.545 | 9.228 |
| Hyperbolic | 5.488 | 29.832 | 5.982 | 5.289 |
| Triangular | 7.455 | 11.467 | 7.247 | 7.063 |

Static Rapid is deliberately static, so the hyperbolic matrix exposes its
failure mode: a few heavy leading rows make it about 5.44x slower than normal
Eigen. Both hybrids remove that cliff. Mailbox ranges from 2.8% faster on the
triangular input to 9.0% slower on the hyperbolic input. Lazy is 5.1% faster on
balanced, 3.6% faster on hyperbolic, and 5.3% faster on triangular input. Thus
the lazy policy combines the direct-loop advantage with the load balance
expected from the normal work-stealing backend.

## Nested matrix work

Nested loops exposed a separate mailbox failure mode before the final tuning.
Ordinary mailbox tasks correctly had no Rapid region context, but that made
each inner loop look like a new whole-pool root. Preserving the task's logical
domain removes this fan-out without restricting ordinary stealing. Final
matched medians are:

| Workload | Normal Eigen | Static Rapid | Mailbox | Lazy |
| --- | ---: | ---: | ---: | ---: |
| Matrix multiply | 1.408 ms | 0.321 ms | 0.229 ms | 0.218 ms |
| Matrix transpose | 55.71 us | 15.36 us | 20.49 us | 17.96 us |

Mailbox is now 6.15x faster than normal Eigen on nested matrix multiply and
2.72x faster on transpose. Relative to the untuned mailbox path measured in
this pass, multiply fell from 3.60 ms to 0.22 ms and transpose from 298 us to
20.1 us. With one worker, both hybrid modes now collapse to the same direct
serial path as static Rapid; a 262K direct loop measured about 58 us instead of
124 us for the previous lazy coordinator path.

These macOS results are development evidence, not cross-machine constants.
Affinity is a no-op on this host, background load and heterogeneous cores add
variance, and absolute times should not be combined with older runs. The
same-commit normal Eigen binary is intentionally used as the policy baseline;
it is more controlled than comparing binaries from different historical
checkouts and also includes the branch's shared pool correctness fixes.

## Correctness and CI evidence

The final local implementation passed:

- 122/122 tests in both Clang Debug and Clang Release configurations;
- 45/45 Release scheduler-evaluation oracle, nesting, runner, and smoke tests;
- 20 complete Rapid-suite repetitions under UBSan on the final code;
- 200 repetitions of the mailbox nesting, cross-pool, one-worker, and
  cancellation edge cases;
- 500 repetitions of the delayed-owner and forced descriptor-scarcity lazy
  stealing cases;
- 200 repetitions of the no-stall/no-deregistration transition test;
- exception propagation and reuse, nested hybrid loops, exact-once visitation,
  concurrent roots, descriptor scarcity, queue saturation, and pool
  cancellation;
- `clang-tidy` using the exact Release compilation database, with no findings.

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
