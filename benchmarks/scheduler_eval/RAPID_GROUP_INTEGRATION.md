# Fast Rapid groups: integration session, 2026-09-21

This session merges upstream main at `e32f8bd` into the Rapid branch
(`7dfa3bc`), then concentrates on fast group activation and scheduler handoff.
It does not introduce another adaptive stealing or timespan policy.

## Integration

- Upstream auto, simple, static and affinity partitioners coexist with all
  previous Rapid policies in the expanded scheduler evaluation suite.
- The historical TBB trapper implementation is named `RAPID_ORIGINAL`.
  `RAPID_START` continues to mean hierarchical pool-backed activation.
- Affinity publication keeps the sender/recipient proxy protocol, but both
  queues are bounded. Saturation executes unclaimed work inline after
  publication admission is released. No global unbounded overflow is restored.
- Native Rapid publication now explicitly releases resident workers into the
  scheduler. A periodic queue probe covers registration/publication races.
  A regression reproduced the previous failure before this fix.
- Existing row-order experiments, mixed ordinary-work cases, and randomized
  fresh-process measurements survive the upstream benchmark reorganization.

## The fast-group entry point

`ParallelForResidentRanges(group, begin, end, callback)` reserves available
helpers and divides the range using the actual captured participant count.
Each participant invokes one callback with its contiguous range. The caller
owns a stack descriptor and waits for every helper before returning or
rethrowing. There is no ordinary task allocation on a warm group launch.

The range callback controls its own inner loop and cancellation safe points.
The earlier per-item API remains available with its per-item cancellation
checks. This distinction matters in almost-empty-loop measurements: removing
those checks from a caller-owned loop measures a different callback contract,
not a free optimization of identical cancellation semantics.

Residents rejoin the group immediately after scheduler work, without the
ordinary pool's additional pre-parking spin. Only queued work requests their
return to the scheduler; already-running tasks do not make idle workers
unavailable. Empty command slots are read before attempting an atomic exchange.
The exchange and its acquire ordering are preserved.

Worker-mask selection takes the entire available mask when it fits the
participant budget, and otherwise extracts set bits in rotated order. A
separate scanning oracle checks 562,688 mask/rotation/capacity combinations.
The completion wait briefly polls before helping ordinary scheduler work;
helping is retained to support callbacks that invoke the demand partitioner.

This remains an explicit `ResidentBusy` experiment. It consumes idle CPU,
captures at most 64 participants per invocation, and performs static initial
distribution. Concurrent roots reserve disjoint helpers. If no helper is
available, the caller runs the range. Same-state nested group calls execute
directly; separate tests exercise nesting through the ordinary demand scheduler.

## Measurement protocol

Apple M4 Max, 12 performance plus 4 efficiency cores, Apple Clang 16, Release,
system allocator. macOS does not provide the per-worker affinity control used
by the Linux experiments. Background load was nonzero. These are exploratory
local results, not a claim of cross-platform superiority.

The final comparison disables Eigen task/steal instrumentation with
`OOX_SCHEDULER_EVAL_STATS=OFF`. Seven independent process rounds use shuffled
mode order and seed 21. The initial release cohort uses 0.1-second samples;
the final `confirmed-8` and `confirmed-16` cohorts use 0.05-second samples.
Times below are medians across processes. The runner records executable
hashes, compiler/build settings, checkout state, and process order.

Raw data and preserved A/B binaries are local under
`results/rapid-integration-20260921/`; that directory is ignored by Git.
Earlier `baseline-8` measurements have instrumentation enabled, so they must
not be combined with the final instrumentation-free cohort.

## Controlled intermediate findings

The seven-round re-entry A/B reduced the one-ordinary-task case from
140.54 to 118.51 microseconds and the four-task case from 147.30 to 133.52
microseconds. The eight-task case was essentially unchanged. Pure launch
times were also close.

The subsequent seven-round completion-wait A/B reduced the one-task mixed
case from 121.46 to 97.66 microseconds. Launch(64) moved from 1.102 to 1.082
microseconds, too small a difference to treat as a strong result by itself.
These measurements isolate successive binaries; they are not additive
percentage improvements.

The important remaining limitation is mixed traffic: fast group capture can
find fewer available helpers while ordinary scheduler work is in flight.
Hierarchical Rapid and ordinary partitioners remain necessary comparison
points. A lower isolated launch latency does not imply better mixed-workload
throughput.

## Full-pool regression and coordinator fix

The initial `release-16` cohort exposed a severe busy-wait interaction:
Launch(64) reached 62.58 microseconds for the group. The coordinator still
called `std::this_thread::yield()` after an unsuccessful scheduler-help attempt.
The final implementation retains that help attempt but uses the processor
pause/yield hint instead of requesting OS rescheduling in this busy mode.

In a three-round alternating A/B with identical sixteen-worker inputs, the
OS-yield binary measured 280.73 microseconds for Launch(64), while the
processor-pause binary measured 2.94 microseconds. This is evidence for the
specific code change, not proof of a particular undocumented macOS scheduling
rule. The large spread in the bad variant is itself a reason to report the
regression instead of selecting only its favorable samples.

## Validation

The full merged Release build completed and CTest listed 648 tests with zero
failures and three existing SharedVar skips. Following the final coordinator
pause refinement, 88 targeted Release pool/Rapid/backend-loop tests and 82
UBSan pool/Rapid tests passed. The preceding broader UBSan run also checked
partitioner range, completion, and lifetime oracles.

The group tests include 1,176 configured ranges (1,152 small and 24 large),
partial and complete worker occupation, 562,688 mask-selection oracle cases,
concurrent roots invoking demand partitioning, nested range callbacks,
exception reuse, and native Rapid publication into a resident pool.

## Final confirmation

Seven fresh processes per mode and worker count; 0.05-second minimum samples,
counters disabled. Values are median microseconds. `RAPID_GROUP` uses the final
processor-pause completion wait. The older `release-*` cohort predates that fix.

### 8 workers

| Policy | Launch 64 | Launch 4,096 | Launch 262,144 |
|---|---:|---:|---:|
| EIGEN_AUTO | 9.163 | 17.396 | 30.160 |
| RAPID_ORIGINAL | 0.684 | 0.850 | 9.185 |
| RAPID_START | 3.527 | 3.577 | 19.006 |
| RAPID_GROUP | 1.056 | 1.311 | 9.435 |

| Policy | Mixed: 1 ordinary task | Mixed: 4 | Mixed: 8 |
|---|---:|---:|---:|
| EIGEN_AUTO | 39.584 | 42.971 | 48.129 |
| RAPID_START | 28.107 | 30.180 | 32.720 |
| RAPID_GROUP | 113.670 | 129.484 | 142.817 |

### 16 workers

| Policy | Launch 64 | Launch 4,096 | Launch 262,144 |
|---|---:|---:|---:|
| EIGEN_AUTO | 13.629 | 39.803 | 59.743 |
| RAPID_ORIGINAL | 8.171 | 8.012 | 15.858 |
| RAPID_START | 7.562 | 8.069 | 23.155 |
| RAPID_GROUP | 3.215 | 3.160 | 11.081 |

| Policy | Mixed: 1 ordinary task | Mixed: 4 | Mixed: 8 |
|---|---:|---:|---:|
| EIGEN_AUTO | 60.627 | 56.770 | 55.302 |
| RAPID_START | 24.782 | 24.909 | 26.152 |
| RAPID_GROUP | 61.954 | 109.463 | 154.789 |

The group improves isolated launch relative to the demand partitioner, but
does not dominate hierarchical Rapid under mixed traffic. These results support
keeping resident groups opt-in and treating scheduler handoff as part of the
design, rather than judging the implementation from empty-loop latency alone.
