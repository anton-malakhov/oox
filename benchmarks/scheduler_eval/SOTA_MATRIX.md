# SOTA policy matrix for Rapid Start

This change turns the lazy-stealing coordinator into a policy matrix so the
open questions in `RAPID_START_CONTEXT_BRIEF.md` (Q3, Q5, Q6, Q8, Tier 3) and
the research report's ranked candidates can be answered by measurement.

## What changed

| Area | File | Change |
| --- | --- | --- |
| Grain laws | `oox/eigen/rapid_start_model.h` | `GrainLaw {Fixed, Sqrt, SqrtCv, Heartbeat, FixedSizeChunk, Factoring, Guided}`; `RunningStats` owner-side Welford estimator; `VictimPolicy {Linear, Hierarchical, MostRemaining}`; `LoopProfile` / `LoopProfileTable` cross-call warm start. Old API unchanged. |
| Coordinator | `oox/eigen/rapid_start.h` | `LazyRangeCoordinator<F, Law, Victims>` + `ParallelForLazyStealingPolicy<Law, Victims>(...)`. `ParallelForLazyStealing` / `ParallelForTimespanLazyStealing` are unchanged wrappers (the latter gains an optional `LoopProfile*`). |
| Pool | `oox/eigen/nonblocking_thread_pool.h` | `PublishOrdinaryBatch` wakes once on first task, then one `EventCount::NotifyN(count)` at batch end. `OOX_EIGEN_BATCHED_WAKE=0` restores per-task wake. `WorkerWakeNotifications()` diagnostic. Linux sandbox A/B: 25,950 -> 450 real wakes for identical work. |
| Workloads | `benchmarks/scheduler_eval/workloads.h`, `bench_scheduler_eval.cpp`, `tests.cpp` | `RowOrder {Sorted, Shuffled, Shifted}` on SpMV; four new benchmark series `SpmvBenchmark<Hyperbolic|Triangle, Shuffled|Shifted>`. Same n/mu/sigma, different contiguous structure. |
| Modes | `benchmarks/eigen/modes.h`, `scheduler_eval/rapid_start_adapter.h`, `scheduler_eval/CMakeLists.txt` | 9 new modes (below). |
| Timer units | `benchmarks/eigen/util.h`, `timespan_partitioner.h`, `scheduler_eval/timespan_tuner.cpp` | `TimerFrequencyHz()`, `TicksToNanoseconds()`, `NanosecondsToTicks()`. `INIT_TIME` is now `NanosecondsToTicks(OOX_EIGEN_INIT_TIME_NS)` (default 75000). Tuner emits `timer_frequency_hz` and `recommended_init_time_ticks`. |
| Runner | `benchmarks/scheduler_eval/run.py` | `--shuffle-modes`, `--fresh-process-repetitions N`, `--seed`; captures core-class topology into `metadata.json` (schema 2). |
| Tests | `tests/policy_matrix_check.cpp`, `tests/wake_check.cpp`, `tests/CMakeLists.txt` | 21-combination exactly-once/exception/nesting/cancel/scarcity matrix; wake A/B counter. |

## New evaluation modes

| Mode | Law | Victims | Notes |
| --- | --- | --- | --- |
| `RAPID_SQRTCV_LAZY_STEALING` | Sqrt / sqrt(1+cv^2) | Linear | adds the sigma/mu term FAC and TAP have; heuristic form |
| `RAPID_HEARTBEAT_LAZY_STEALING` | tau = 20 h | Linear | Acar et al. linear rule |
| `RAPID_FSC_LAZY_STEALING` | Kruskal-Weiss 2/3-power | Linear | falls back to Sqrt until sigma is observed |
| `RAPID_FACTORING_LAZY_STEALING` | FAC2 ceil(R/2P) | Linear | item-based, no timing |
| `RAPID_GUIDED_LAZY_STEALING` | GSS ceil(R/P) | Linear | item-based, no timing |
| `RAPID_TIMESPAN_LAZY_STEALING_PROFILED` | Sqrt | Linear | loop profile warm start keyed by callable type + trip count |
| `RAPID_LAZY_STEALING_HIERARCHICAL` | Fixed | nearest-first | HotSLAW-style locality |
| `RAPID_LAZY_STEALING_PRESSURE` | Fixed | most-remaining | pressure-aware |
| `RAPID_MAILBOX_PERTASK_WAKE` | (mailbox) | - | A/B control with `OOX_EIGEN_BATCHED_WAKE=0` |

## How to read the results

1. **Exponent test (Q6, report section D).** Compare `RAPID_TIMESPAN_LAZY_STEALING` (sqrt), `RAPID_FSC_LAZY_STEALING` (2/3), `RAPID_HEARTBEAT_LAZY_STEALING` (linear) and `RAPID_FACTORING_LAZY_STEALING` (parameter-free) on Launch, Scan and the three SpMV shapes. If FAC2 is within noise of Sqrt everywhere, the sqrt law is not earning its complexity.
2. **cv term.** `RAPID_SQRTCV_LAZY_STEALING` vs `RAPID_TIMESPAN_LAZY_STEALING` on hyperbolic and triangular SpMV. Expect no difference on Balanced.
3. **Spatial vs statistical (Q5).** For each policy, `SpmvBenchmark<Hyperbolic>` vs `<Hyperbolic, Shuffled>` vs `<Hyperbolic, Shifted>`. Static Rapid should move a lot; a policy that is explained by mu/sigma alone should not.
4. **Victim order (Q8).** `RAPID_LAZY_STEALING` vs `_HIERARCHICAL` vs `_PRESSURE`. Differences will be small on 16 workers; they matter more at higher P.
5. **Profile warm start.** `_PROFILED` vs `RAPID_TIMESPAN_LAZY_STEALING` on Scan (2 log N repeated calls of the same shape) and on repeated SpMV.
6. **Batched wake.** `RAPID_MAILBOX` vs `RAPID_MAILBOX_PERTASK_WAKE`. The effect is largest with spinning disabled or when workers park between calls; on the 262K mailbox launch it removes ~N seq_cst fences.

## Protocol

```sh
cmake -S . -B build-eval -G Ninja -DCMAKE_BUILD_TYPE=Release \
  -DOOX_BUILD_TESTS=ON -DOOX_BUILD_TASKBENCH=OFF -DOOX_BUILD_SCHEDULER_EVALS=ON
cmake --build build-eval --target scheduler_eval_all policy_matrix_check wake_check -j
ctest --test-dir build-eval -R "scheduler_eval|PolicyMatrix|BatchedWake" --output-on-failure

# Counterbalanced fresh-process protocol (recommended over --repetitions)
python3 benchmarks/scheduler_eval/run.py --build build-eval --threads 16 \
  --fresh-process-repetitions 5 --shuffle-modes --seed 20260902 \
  --filter 'Launch|Scan|Spmv' --timeout 1800
```

Everything above was compiled and run in a Linux/x86 sandbox before being
written here: all 12 rapid modes pass `tests.cpp`; the 21-combination matrix
passes clean under TSan, ASan and UBSan. No timing numbers from the sandbox are
meaningful (1 core). The known caveats: `SqrtCv` is a heuristic of this branch,
not a published rule; the adapter's profile key (`typeid(F) ^ trip count`)
stands in for a call site and would alias in real code.

On the Apple M4 Max evaluation host, `CNTFRQ_EL0` reports 1 GHz. This is why
the implementation reads the register instead of assuming the 24 MHz value
seen on earlier Apple Silicon generations.

## Current Apple M4 Max results

The targeted ten-policy cohort is retained at
`results/scheduler_eval/20260902_sota_targeted`. It uses five fresh processes,
reshuffled policy order (seed 20260902), and nine cases: two launch endpoints
and seven 131K-row SpMV layouts. Geometric-mean time relative to the per-case
oracle is: most-remaining victim 1.111, square root 1.145, profiled square root
1.173, FSC 1.179, hierarchical victims 1.197, heartbeat 1.206, fixed lazy
1.248, factoring 1.250, square root + CV 1.264, and guided 1.306.

The ranking is provisional. Median fresh-process ranges are 22-45% depending
on policy because macOS cannot pin workers across the M4 Max's 12 performance
and four efficiency cores. Stable directional findings are stronger: the CV
heuristic is 10.4% slower than square root in geometric mean; guided is 14.0%
slower; profiling cuts 262K launch from 71.30 to 38.92 us but loses 14.7-22.9%
on hyperbolic layouts; and most-remaining victims improve fixed lazy by 11.0%
in geometric mean. The wider 49-case four-law run ranks FSC first, but carries
the same placement caveat.

Five sequential wake-check repetitions give medians of 450 batched versus
25,950 per-task notification operations (57.7x fewer, a 98.3% reduction), with
all 3,276,800 visits executed exactly once in every repetition.
