# Rapid Start versus the normal Eigen backend

## Executive summary

This branch turns the Eigen backend from a conventional recursive
work-stealing pool into a scheduler with a second, composable fast-distribution
path. The normal Eigen policy remains available as the baseline. Four Rapid
policies cover different workload shapes:

- `RAPID_START` assigns proportional ranges through a hierarchical activation
  tree and keeps them static.
- `RAPID_MAILBOX` uses Rapid distribution to publish a bounded set of adaptive
  blocks into ordinary worker mailboxes, then uses unrestricted stealing.
- `RAPID_LAZY_STEALING` reserves the first block of every proportional range
  for its owner before publishing execution. A worker deregisters from its
  Rapid domain only when it has exhausted local work and a peer has unclaimed
  later blocks.
- `RAPID_TIMESPAN_LAZY_STEALING` keeps that lazy transition but continuously
  sizes owner blocks from their measured useful-work duration. Thieves use the
  latest estimate without feeding their migration cost back into it.

The result is not one universally best policy. Static Rapid has the lowest
uniform-loop overhead. Fixed lazy Rapid minimizes the extra bookkeeping on
short calls. Timespan-lazy Rapid is the best general-purpose compromise for
longer variable-cost work in these measurements: it retains most of the launch
advantage while improving balanced and irregular SpMV. Mailbox mode is useful
when work must become ordinary pool work immediately.

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

The hybrid policies originally exposed recursive grain-one task trees.
That recovered imbalance but made a 262K empty loop take roughly 40--43 ms.
The final implementation replaces that frontier with adaptive blocks. Mailbox
mode creates at most a bounded oversubscription of flat tasks. Lazy mode creates
no ordinary tasks: workers claim blocks through cache-line-separated atomic
cursors and make a one-way transition to unrestricted stealing only when it is
useful. All hybrid policies honor the caller's grain as a minimum block size. Large
slices now stop at 64 blocks per worker instead of 128. Timespan-lazy mode
starts from those blocks, times only the proportional owner's work, and adapts
toward a 75 microsecond target. A 512-iteration-per-worker cutoff keeps clock
reads out of short loops. Alternated old/new runs
showed 1.75--1.79x faster 262K mailbox launch and 1.24--1.27x faster lazy launch,
while retaining 1,024 independently stealable blocks at 16 workers.

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
as many tasks through 512 iterations per worker; larger and irregular loops
retain the finer balance-preserving density. Isolated alternating runs moved
the 4K direct loop from 115.8 us to 62.9 us in the final suite. A wider
1,024-iteration threshold was rejected: although it reduced 16K launch time,
it made hyperbolic SpMV 12--16% slower by crossing that benchmark's 529-row-
per-worker balance point. Quarter-density and tiered variants were likewise
rejected when reductions or unaffected large loops regressed. The retained
rule is the smallest policy that produced a repeatable win without moving
those controls.

## Direct backend loop results

The following medians come from `bench_loops`, which invokes the backend
directly rather than going through OOX. The final run used Release mode, 16
workers, seven repetitions, and a 100 ms minimum sample time on the local Apple
Silicon host. Times are microseconds; speedup is relative to the normal Eigen
work-stealing mode built from the same commit.

| Iterations | Normal Eigen | Static Rapid | Mailbox | Fixed lazy | Timespan lazy | Timespan speedup |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 64 | 21.20 | 13.30 | 21.60 | 24.40 | 24.40 | 0.87x |
| 512 | 55.70 | 13.20 | 36.20 | 24.60 | 25.70 | 2.17x |
| 4,096 | 274.00 | 13.80 | 69.30 | 28.00 | 27.80 | 9.86x |
| 32,768 | 1,932.00 | 13.10 | 123.00 | 32.70 | 31.90 | 60.56x |
| 262,144 | 14,598.00 | 20.30 | 254.00 | 60.50 | 47.20 | 309.28x |

The normal Eigen loop recursively splits to grain one, so scheduler work grows
with the iteration count even when the body is almost empty. Static Rapid
publishes a worker-sized activation tree and then runs contiguous ranges.
Lazy Rapid adds only bounded atomic block claims, while mailbox mode pays for
ordinary task allocation and queue publication. Mailbox therefore has the
smallest advantage at 64 items, while timespan adaptation removes 22.0% of the
fixed-lazy 262K time. The short-loop bypass intentionally makes its scheduling
path identical to fixed lazy below the cutoff; the 64-item difference from
normal Eigen is the fixed Rapid-coordinator cost.

## Other backend-only scheduler families

The same final run exercises scheduler behavior beyond empty loops. The table
uses representative medians; units are shown per row.

| Workload | Normal Eigen | Static Rapid | Mailbox | Lazy |
| --- | ---: | ---: | ---: | ---: |
| 262K launch (us) | 14,398.11 | 22.30 | 222.64 | 50.30 |
| 1M one-relax iterations (us) | 56,435.13 | 94.31 | 279.73 | 97.29 |
| Small blocked reduce (us) | 509.60 | 583.27 | 438.28 | 394.88 |
| Exclusive scan, 2^22 items (ms) | 424.96 | 1.95 | 4.89 | 2.32 |
| 1,024 repeated 16-item calls (ms) | 15.51 | 14.93 | 21.99 | 15.02 |

Rapid's bounded activation tree is especially effective for scan, whose many
phases amplify recursive grain-one scheduler overhead. Lazy is the fastest
hybrid on the small reduction and stays close to static Rapid on the million
item loop. The repeated-call row also records the boundary honestly: static
Rapid ties normal Eigen, lazy is close, and mailbox pays ordinary allocation
and publication on every tiny call.

## Timespan-lazy follow-up

The new policy was tuned independently at 25, 75, and 250 microsecond targets.
The short target generated too many claims, most visibly on triangular SpMV;
the long target gave up balancing and regressed Scan and hyperbolic SpMV. The
75 microsecond target was the best mixed choice. A separate size sweep showed
that timing did not repay its clock overhead below roughly 512 iterations per
effective worker, which became the fixed-lazy bypass threshold.

The final same-load follow-up used seven repetitions and 100 ms minimum sample
time. Lower is better:

| Workload | Fixed lazy | Timespan lazy | Change |
| --- | ---: | ---: | ---: |
| 262K launch (us) | 52.1 | 40.3 | -22.6% |
| Exclusive scan, 2^22 items (us) | 2,386 | 2,365 | -0.9% |
| Balanced SpMV (us) | 9,688 | 9,338 | -3.6% |
| Hyperbolic SpMV (us) | 5,425 | 5,333 | -1.7% |
| Triangular SpMV (us) | 7,740 | 7,063 | -8.7% |

This is the intended merger of the two ideas: Rapid Start supplies immediate
proportional ownership, the owner measures useful-work time while it has local
work, and timespan sizing reduces claim traffic without removing the later
escape to ordinary stealing.

## Balanced and irregular SpMV

The scheduler suite also measures useful work, where low launch time alone is
not enough. These are matched medians for deterministic CSR matrices with
8,455 rows and a 131,139-column input at 16 workers; the benchmark argument is
the nominal column count, not the row count. Times are milliseconds.

| Distribution | Normal Eigen | Static Rapid | Mailbox | Lazy |
| --- | ---: | ---: | ---: | ---: |
| Balanced | 9.638 | 12.615 | 9.666 | 9.660 |
| Hyperbolic | 5.519 | 29.575 | 5.957 | 5.247 |
| Triangular | 7.409 | 11.731 | 7.496 | 7.007 |

Static Rapid is deliberately static, so the hyperbolic matrix exposes its
failure mode: a few heavy leading rows make it about 5.36x slower than normal
Eigen. Both hybrids remove that cliff. Mailbox is within 1.2% of normal on
balanced and triangular input and 7.9% slower on the hyperbolic input. Lazy is
within 0.3% on balanced input, 4.9% faster on hyperbolic input, and 5.4% faster
on triangular input. Thus the lazy policy combines the direct-loop advantage
with the load balance expected from the normal work-stealing backend.

## Nested matrix work

Nested loops exposed a separate mailbox failure mode before the final tuning.
Ordinary mailbox tasks correctly had no Rapid region context, but that made
each inner loop look like a new whole-pool root. Preserving the task's logical
domain removes this fan-out without restricting ordinary stealing. Final
matched medians are:

| Workload | Normal Eigen | Static Rapid | Mailbox | Lazy |
| --- | ---: | ---: | ---: | ---: |
| Matrix multiply | 1.389 ms | 0.323 ms | 0.243 ms | 0.216 ms |
| Matrix transpose | 65.91 us | 20.50 us | 20.40 us | 22.71 us |

Mailbox is now 5.72x faster than normal Eigen on nested matrix multiply and
3.23x faster on transpose. Relative to the untuned mailbox path measured in
this branch work, multiply fell from 3.60 ms to 0.24 ms and transpose from
298 us to 20.4 us. With one worker, both hybrid modes now collapse to the same
direct serial path as static Rapid; a 262K direct loop measured about 58 us
instead of 124 us for the previous lazy coordinator path.

These macOS results are development evidence, not cross-machine constants.
Affinity is a no-op on this host, background load and heterogeneous cores add
variance, and absolute times should not be combined with older runs. The
same-commit normal Eigen binary is intentionally used as the policy baseline;
it is more controlled than comparing binaries from different historical
checkouts and also includes the branch's shared pool correctness fixes.

## Correctness and CI evidence

The final local implementation passed:

- 151/151 tests in the current Clang Debug configuration, including every new
  timespan-lazy backend, trace, oracle, and nesting target;
- 26/26 tests in the focused Release scheduler-evaluation matrix;
- one complete 122/122 suite under UBSan on the final code;
- a longer UBSan repetition exposed a pre-existing mailbox cancellation hang;
  isolated old- and new-density binaries both reproduce it, so the task-density
  change is not causal and the broader pool cancellation fix remains separate;
- 200 repetitions of the mailbox nesting, cross-pool, one-worker, and
  cancellation edge cases;
- 500 repetitions of the delayed-owner and forced descriptor-scarcity lazy
  stealing cases;
- 200 repetitions of the no-stall/no-deregistration transition test;
- exception propagation and reuse, nested hybrid loops, exact-once visitation,
  concurrent roots, descriptor scarcity, queue saturation, and pool
  cancellation;
- `clang-tidy` using the exact Release compilation databases; the changed
  Rapid path had no findings, while the benchmark translation unit retained
  existing analyzer warnings in the baseline Eigen partitioner and benchmark
  loop idiom.

AddressSanitizer and ThreadSanitizer were also attempted earlier in the branch
work, but both runtimes fail before test execution on this Apple host. They are
not counted as passing evidence. The GitHub Linux GCC and Clang matrix remains
the authoritative portability check.

## Choosing a policy

Use static `RAPID_START` for uniform loops when the proportional partition is
known to be balanced. Use `RAPID_LAZY_STEALING` for short or extremely cheap
variable work. Use `RAPID_TIMESPAN_LAZY_STEALING` as the default candidate for
longer variable-cost loops: it starts on the fast path, adapts claim density to
measured work, and pays the global stealing transition only when useful peer
work exists. Use `RAPID_MAILBOX`
when immediately converting the Rapid partition into ordinary queue work is
valuable, or when interaction with other ordinary tasks is part of the desired
scheduling behavior. Keep normal Eigen available for workloads whose behavior
has not yet been characterized; the benchmark suite now makes that comparison
reproducible rather than implicit.
