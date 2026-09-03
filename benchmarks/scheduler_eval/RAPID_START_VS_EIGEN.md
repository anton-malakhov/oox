# Rapid Start for Eigen: fast launch without giving up work stealing

OOX's Eigen backend began as a conventional recursive work-stealing scheduler.
That design is robust when iteration costs are unknown, but it can spend far
more time constructing and distributing work than executing a cheap loop. This
branch adds Rapid Start as a second, composable distribution path and then
builds progressively more adaptive policies on top of it.

The central result is that distribution no longer has to choose between a fast
static launch and eventual load balance. Static Rapid Start remains the best
choice for known-uniform work. Lazy and timespan-lazy policies start with the
same proportional launch, keep their work locally while useful work is
available, and enter ordinary stealing only when imbalance actually appears.

## Results at a glance

These are representative same-commit, 16-worker Release medians from the local
Apple Silicon host. Each speedup uses the normal Eigen policy from the same
benchmark family as its baseline.

| Workload | Best relevant Rapid policy | Normal Eigen | Rapid | Speedup |
| --- | --- | ---: | ---: | ---: |
| 262K backend-only loop | Static Rapid | 14,263 us | 25.5 us | 559x |
| 262K backend-only adaptive loop | Timespan lazy | 14,263 us | 51.8 us | 275x |
| 1M one-relax iterations | Static Rapid | 56,435 us | 94.3 us | 598x |
| Exclusive scan, 2^22 items | Static Rapid | 425 ms | 1.95 ms | 218x |
| Nested matrix multiply | Lazy Rapid | 1.389 ms | 0.216 ms | 6.43x |
| Nested matrix transpose | Mailbox Rapid | 65.9 us | 20.4 us | 3.23x |
| Hyperbolic SpMV | Lazy Rapid | 5.519 ms | 5.247 ms | 1.05x |
| Triangular SpMV | Lazy Rapid | 7.409 ms | 7.007 ms | 1.06x |

The hundreds-fold figures are scheduler-overhead results, not claims that
useful numerical kernels universally become hundreds of times faster. The SpMV
and nested-matrix rows show the more important end of the spectrum: Rapid can
retain the launch advantage without losing the balancing behavior required by
real, nonuniform work.

## The four designs

The normal Eigen policy remains available throughout as a controlled baseline.
Four Rapid policies cover different workload shapes:

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

There is deliberately no claim that one policy wins everywhere. Static Rapid
has the lowest uniform-loop overhead. Fixed lazy Rapid minimizes adaptive
bookkeeping on short calls. Timespan-lazy Rapid is the portable adaptive
candidate for longer or variable-cost work. Mailbox mode is useful when Rapid
distribution should turn into ordinary pool work immediately.

## Why the normal Eigen path became expensive

Normal Eigen recursively divides a loop into tasks and relies on workers to
steal those tasks. That is a reasonable default for expensive or unpredictable
iterations. For a nearly empty body, however, the number of scheduling
operations grows with the iteration count. The direct loop results make this
visible: normal Eigen rises from 28.6 us at 64 iterations to 14.3 ms at 262K,
while static Rapid stays between 13 and 26 us.

Rapid Start changes the unit of distribution. It activates a proportional tree
over the worker domain, so the launch structure grows with the number of
workers rather than with the number of loop iterations. Each worker receives a
contiguous data range immediately. The hybrid policies then recover imbalance
without reconstructing the grain-one task frontier.

## Building a production-quality Rapid path

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
bounded queue saturation preserves progress through post-admission inline
backpressure; and
ordinary work receives a fairness opportunity during sustained Rapid traffic.

### From recursive tasks to bounded blocks

The hybrid policies originally exposed recursive grain-one task trees.
That recovered imbalance but made a 262K empty loop take roughly 40--43 ms.
The final implementation replaces that frontier with adaptive blocks. Mailbox
mode creates at most a bounded oversubscription of flat tasks. Lazy mode creates
no ordinary tasks: workers claim blocks through cache-line-separated atomic
cursors and make a one-way transition to unrestricted stealing only when it is
useful. All hybrid policies honor the caller's grain as a minimum block size.
Large slices now stop at 64 blocks per worker instead of 128. Timespan-lazy mode
starts from those blocks and times only the proportional owner's work. Its
default target is now computed from a one-time platform overhead calibration,
effective domain size, projected owner-range time, and live steal pressure; it
contains no fixed duration or iteration-count cutoff. Alternated old/new runs
showed 1.75--1.79x faster 262K mailbox launch and 1.24--1.27x faster lazy launch,
while retaining 1,024 independently stealable blocks at 16 workers.

### Protecting delayed owners

Lazy mode reserves all owner blocks before publishing its single execution
tree. This closes a delayed-owner race: a fast peer can finish before another
owner is scheduled, but it can still safely claim later blocks because the
delayed owner's first block is already protected. Forced descriptor scarcity
may group several ranges into one activation without changing that invariant.

### Preserving domains through nesting

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
| 64 | 28.60 | 13.20 | 22.00 | 21.50 | 22.90 | 1.25x |
| 512 | 61.20 | 12.90 | 34.70 | 23.80 | 26.10 | 2.34x |
| 4,096 | 256.00 | 14.20 | 67.90 | 28.10 | 28.60 | 8.95x |
| 32,768 | 1,778.00 | 15.80 | 123.00 | 34.00 | 30.90 | 57.54x |
| 262,144 | 14,263.00 | 25.50 | 258.00 | 60.50 | 51.80 | 275.35x |

The normal Eigen loop recursively splits to grain one, so scheduler work grows
with the iteration count even when the body is almost empty. Static Rapid
publishes a worker-sized activation tree and then runs contiguous ranges.
Lazy Rapid adds only bounded atomic block claims, while mailbox mode pays for
ordinary task allocation and queue publication. Mailbox therefore has the
smallest advantage at 64 items, while automatic timespan adaptation removes
14.4% of the fixed-lazy 262K time. Unlike the former fixed-target version, the
automatic policy also times short loops; the runtime formula grows or shrinks
their blocks from observed cost instead of branching on an iteration threshold.

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

## Timespan-lazy calibration follow-up

The first version was tuned independently at 25, 75, and 250 microsecond targets.
The short target generated too many claims, most visibly on triangular SpMV;
the long target gave up balancing and regressed Scan and hyperbolic SpMV. The
75 microsecond target was the best fixed mixed choice. It remains available as
an explicit experimental override, but it is no longer the default.

Automatic mode measures the current CPU's clock and atomic scheduling sequence
once; call that local overhead $h$. For a domain of $P$ workers, projected full
owner-range time $R$, and $s$ owners already looking for work, the scheduler
models the cost of target duration $\tau$ as

$$
C(\tau)=\frac{(Ph)R}{\tau}+(1+s/P)\tau.
$$

The first term estimates scheduling and coherence work: smaller blocks require
more claims. The second estimates exposed tail time: larger blocks take longer
to make useful work available to an idle worker. Minimizing this expression
gives

$$
\tau=\sqrt{\frac{(Ph)R}{1+s/P}}.
$$

This host measured $h=49$ ns, but 49 ns is evidence rather than policy. A
different CPU, clock implementation, worker count, or loop body changes the
target automatically. The measured block change is bounded to one quarter
through eight times the previous block, smoothed after the first sample, and
capped so that at least four later steal opportunities remain. The caller's
grain remains a hard lower bound.

The matched local follow-ups used seven repetitions and 100 ms minimum sample
time. Lower is better:

| Workload | Fixed lazy | Fixed 75 us | Automatic | Automatic vs lazy |
| --- | ---: | ---: | ---: | ---: |
| 262K launch (us) | 52.1 | 41.1 | 38.9 | -25.3% |
| Exclusive scan, 2^22 items (us) | 2,386 | 2,303 | 2,273 | -4.7% |
| Balanced SpMV (us) | 9,688 | 9,412 | 9,640 | -0.5% |
| Hyperbolic SpMV (us) | 5,425 | 5,230 | 5,594 | +3.1% |
| Triangular SpMV (us) | 7,740 | 6,953 | 7,484 | -3.3% |

The automatic 262K direct loop measured 128, 108, 58.4, 40.4, and 49.2 us at
1, 2, 4, 8, and 16 workers. The reversal after eight workers reflects this
host's 12 performance and 4 efficiency cores plus unavailable affinity, and is
why the calibration uses measured costs instead of assuming a CPU-frequency or
worker-count lookup table.

The automatic result is 1.3--5.2% faster than fixed 75 us on launch and Scan,
and 2.4--7.6% slower on the three SpMV shapes, while removing its
machine-specific duration. Run-to-run variation is material on this host,
where worker affinity is unavailable, so small differences should not be read
as universal rankings. This is the intended merger of the two
ideas: Rapid Start supplies immediate proportional ownership, the owner
measures useful-work time while it has local work, and timespan sizing reduces
claim traffic without removing the later escape to ordinary stealing.

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
Eigen. Even the structurally balanced matrix is not guaranteed to take equal
wall time on every worker: this host mixes performance and efficiency cores,
and cache misses, memory service, and operating-system interruptions vary.
Static Rapid gives every worker equal row work and then waits for the slowest;
it cannot redistribute that physical-time imbalance. Both hybrids remove that
cliff. Mailbox is within 1.2% of normal on
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

The final pushed commit passed:

- 152/152 tests in the complete local Debug build;
- 42/42 tests in the Release scheduler matrix;
- 129/129 tests in each local CI-shaped Clang Debug and Release build;
- 26/26 Rapid tests under UBSan;
- all four jobs in the [GitHub Linux CI run](https://github.com/anton-malakhov/oox/actions/runs/33430895687):
  GCC and Clang 19, each in Debug and Release.

The suite covers exact-once visitation, exception propagation and reuse,
publication/cancellation ordering, concurrent roots, nested hybrid loops,
cross-pool calls, single-worker domains, descriptor scarcity, delayed owners,
queue saturation, fairness to ordinary work, and pool destruction. Focused
stress campaigns also ran the mailbox edge cases 200 times, the delayed-owner
and descriptor-scarcity cases 500 times, and the no-stall/no-deregistration
transition 200 times.

AddressSanitizer and ThreadSanitizer were attempted but their runtimes abort
before test execution on this Apple host, so they are not counted as passing
evidence. The installed local GCC 15 toolchain also fails in its macOS SDK
headers before reaching project code. The successful GitHub Ubuntu GCC matrix
is the portability result that matters. `clang-tidy` with the Release
compilation databases reported no findings in the changed Rapid path.

## Choosing a policy

Use static `RAPID_START` for uniform loops when the proportional partition is
known to be balanced. Use `RAPID_LAZY_STEALING` for short or extremely cheap
variable work. Use `RAPID_TIMESPAN_LAZY_STEALING` as the default candidate for
longer variable-cost loops: it starts on the fast path, adapts claim density to
measured work, and pays the global stealing transition only when useful peer
work exists. Use `RAPID_MAILBOX` when immediately converting the Rapid
partition into ordinary queue work is
valuable, or when interaction with other ordinary tasks is part of the desired
scheduling behavior. Keep normal Eigen available for workloads whose behavior
has not yet been characterized; the benchmark suite now makes that comparison
reproducible rather than implicit.

## Conclusion

The work began with a fast static activation mechanism and ended with a family
of composable schedulers. The important steps were not only making Rapid Start
fast, but making it bounded, reentrant, cancellation-safe, nested-domain aware,
and able to hand work to the ordinary Eigen pool without losing progress.

For uniform loops, launch work now scales primarily with workers instead of
iterations, producing up to a 559x measured speedup over normal Eigen. For
workloads that need balance, lazy Rapid preserves the fast start and can still
beat normal Eigen on irregular SpMV. The timespan merger removes the final
machine-specific 75 us assumption: it derives block duration from measured CPU
overhead, observed useful-work rate, domain size, and live steal pressure.

The practical conclusion is not that work stealing should disappear. It is
that work stealing need not be paid for eagerly. Rapid Start establishes cheap
ownership first; the scheduler exposes and steals work only when the execution
shows that doing so is worthwhile.

The benchmark construction and reproduction commands are documented in
[the scheduler evaluation README](README.md). The full event-count and
parameter-selection model is in [the performance model](PERFORMANCE_MODEL.md).
