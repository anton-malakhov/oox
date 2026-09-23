# Hardware-calibrated Rapid mailbox: comparison

Measured on 2026-09-23. This report describes local-first handoff with automatic
startup calibration. Both immediate and local-first handoff are implemented;
only local-first is timed in this matrix.

Completed 15 cases × four configurations × three worker counts × five fresh-process repetitions: **900 valid observations**. All executable hashes match the pre-run manifest.

Apple M4 Max, macOS 26.5.2, Release, scheduler task counters disabled. The measured Rapid policy is local-first mailbox handoff; eager handoff is covered by correctness tests but is not timed in this matrix. Background desktop applications remained active. Processes ran sequentially in shuffled order (seed 230918); no builds or correctness tests ran concurrently. Each case used a minimum 0.15-second measurement. These are diagnostic cases, not a new full 615-case sweep.

Speedup is baseline elapsed time / automatic elapsed time. Values above one favor automatic calibration. Each case uses the median of five fresh-process observations; aggregates are equal-case-weight geometric means. Wins/ties/losses use a descriptive 0.95–1.05 ratio band, not a significance test.

| Workers | vs Eigen auto | vs new code, full resident | vs previous adaptive prototype | Wins/ties/losses vs auto |
|---|---:|---:|---:|---:|
| 4 | 1.379× | 1.036× | 1.041× | 7/7/1 |
| 8 | 1.404× | 1.190× | 1.168× | 8/6/1 |
| 16 | 1.367× | 3.425× | 3.000× | 10/1/4 |

## Calibration choices

A limit of one means no background spinner when the caller occupies worker zero. A limit of two means one background spinner. Other pool workers can execute ordinary tasks and steal. Limit zero falls back to the ordinary auto partitioner.

| Workers | Selected (limit, multiplier): count | Startup median / range, ms | Exhausted budgets |
|---|---|---:|---:|
| 4 | (1, 50): 1, (1, 100): 3, (4, 50): 1 | 0.266 / 0.255–0.273 | 0/5 |
| 8 | (2, 50): 4, (4, 50): 1 | 3.060 / 0.765–3.281 | 0/5 |
| 16 | (1, 50): 1, (2, 50): 1, (2, 100): 1, (4, 100): 2 | 7.643 / 6.702–16.072 | 0/5 |

## All case medians

Times are microseconds, lower is better.

| Workers / case | Eigen auto | Previous adaptive | New full resident | Automatic | Auto / automatic |
|---|---:|---:|---:|---:|---:|
| `4t/Bfs<scheduler_eval::GraphKind::PaslPhases50Degree5, BfsPolicy::Fixed>/1024` | 56.192 | 51.199 | 52.335 | 46.132 | 1.218× |
| `4t/ChangingWorkerAvailability/262144` | 11995.294 | 12410.211 | 12363.248 | 12289.015 | 0.976× |
| `4t/CompetingLoops/262144` | 24308.052 | 24648.885 | 24746.917 | 24546.111 | 0.990× |
| `4t/DeepNested/depth:2/fanout:2/work:64` | 947.009 | 959.216 | 959.511 | 959.666 | 0.987× |
| `4t/Launch/262144` | 21.268 | 17.890 | 17.909 | 17.656 | 1.205× |
| `4t/Launch/4096` | 4.799 | 1.058 | 1.048 | 0.999 | 4.802× |
| `4t/Launch/64` | 3.185 | 0.837 | 0.858 | 0.715 | 4.455× |
| `4t/RadixSort<scheduler_eval::KeyKind::Uniform>/1024` | 4.010 | 3.549 | 3.481 | 3.364 | 1.192× |
| `4t/RadixSort<scheduler_eval::KeyKind::Uniform>/4096` | 18.593 | 18.840 | 18.878 | 18.270 | 1.018× |
| `4t/RadixSort<scheduler_eval::KeyKind::Uniform>/65536` | 93.108 | 87.977 | 87.809 | 87.743 | 1.061× |
| `4t/RadixSortPairs<scheduler_eval::KeyKind::Uniform>/4096` | 19.139 | 18.950 | 18.839 | 18.960 | 1.009× |
| `4t/Scan/10` | 48.735 | 16.047 | 14.673 | 13.238 | 3.682× |
| `4t/SpmvBenchmark<SparseKind::Balanced>/4096` | 184.692 | 191.010 | 191.366 | 185.966 | 0.993× |
| `4t/SpmvBenchmark<SparseKind::Hyperbolic>/131072` | 4541.017 | 4659.408 | 4650.143 | 4677.115 | 0.971× |
| `4t/SpmvBenchmark<SparseKind::Hyperbolic>/4096` | 120.106 | 127.290 | 127.420 | 133.565 | 0.899× |
| `8t/Bfs<scheduler_eval::GraphKind::PaslPhases50Degree5, BfsPolicy::Fixed>/1024` | 102.857 | 94.160 | 108.592 | 78.033 | 1.318× |
| `8t/ChangingWorkerAvailability/262144` | 6473.260 | 6594.621 | 6592.824 | 6598.316 | 0.981× |
| `8t/CompetingLoops/262144` | 12292.517 | 12471.184 | 12500.336 | 12724.013 | 0.966× |
| `8t/DeepNested/depth:2/fanout:2/work:64` | 501.313 | 513.458 | 513.688 | 507.476 | 0.988× |
| `8t/Launch/262144` | 29.921 | 10.840 | 10.850 | 12.159 | 2.461× |
| `8t/Launch/4096` | 17.723 | 9.962 | 10.084 | 3.476 | 5.099× |
| `8t/Launch/64` | 8.979 | 5.131 | 5.077 | 2.940 | 3.054× |
| `8t/RadixSort<scheduler_eval::KeyKind::Uniform>/1024` | 4.024 | 3.545 | 3.652 | 3.420 | 1.177× |
| `8t/RadixSort<scheduler_eval::KeyKind::Uniform>/4096` | 20.364 | 24.117 | 27.594 | 20.141 | 1.011× |
| `8t/RadixSort<scheduler_eval::KeyKind::Uniform>/65536` | 121.160 | 90.522 | 91.429 | 100.576 | 1.205× |
| `8t/RadixSortPairs<scheduler_eval::KeyKind::Uniform>/4096` | 21.907 | 25.538 | 23.382 | 20.755 | 1.055× |
| `8t/Scan/10` | 133.616 | 78.584 | 80.365 | 46.148 | 2.895× |
| `8t/SpmvBenchmark<SparseKind::Balanced>/4096` | 200.032 | 193.899 | 195.260 | 198.421 | 1.008× |
| `8t/SpmvBenchmark<SparseKind::Hyperbolic>/131072` | 4191.993 | 4382.736 | 4391.953 | 4369.989 | 0.959× |
| `8t/SpmvBenchmark<SparseKind::Hyperbolic>/4096` | 123.095 | 128.320 | 129.962 | 151.934 | 0.810× |
| `16t/Bfs<scheduler_eval::GraphKind::PaslPhases50Degree5, BfsPolicy::Fixed>/1024` | 118.849 | 181.083 | 230.910 | 76.898 | 1.546× |
| `16t/ChangingWorkerAvailability/262144` | 4876.429 | 5831.609 | 5855.656 | 4615.173 | 1.057× |
| `16t/CompetingLoops/262144` | 7841.510 | 12124.690 | 11470.095 | 8804.677 | 0.891× |
| `16t/DeepNested/depth:2/fanout:2/work:64` | 355.564 | 592.851 | 822.031 | 364.231 | 0.976× |
| `16t/Launch/262144` | 44.554 | 131.244 | 208.267 | 12.464 | 3.574× |
| `16t/Launch/4096` | 30.423 | 239.606 | 351.300 | 6.734 | 4.518× |
| `16t/Launch/64` | 11.287 | 31.570 | 47.619 | 6.466 | 1.746× |
| `16t/RadixSort<scheduler_eval::KeyKind::Uniform>/1024` | 4.015 | 4.121 | 4.572 | 3.461 | 1.160× |
| `16t/RadixSort<scheduler_eval::KeyKind::Uniform>/4096` | 28.676 | 48.348 | 38.391 | 23.131 | 1.240× |
| `16t/RadixSort<scheduler_eval::KeyKind::Uniform>/65536` | 121.181 | 207.585 | 220.132 | 111.975 | 1.082× |
| `16t/RadixSortPairs<scheduler_eval::KeyKind::Uniform>/4096` | 26.710 | 68.161 | 42.628 | 21.573 | 1.238× |
| `16t/Scan/10` | 170.530 | 545.697 | 715.899 | 76.678 | 2.224× |
| `16t/SpmvBenchmark<SparseKind::Balanced>/4096` | 276.548 | 391.249 | 354.778 | 314.004 | 0.881× |
| `16t/SpmvBenchmark<SparseKind::Hyperbolic>/131072` | 5126.067 | 10874.262 | 10346.904 | 5518.246 | 0.929× |
| `16t/SpmvBenchmark<SparseKind::Hyperbolic>/4096` | 164.038 | 978.284 | 1832.297 | 211.945 | 0.774× |

## Interpretation boundaries

- “Eigen auto” is this branch’s Eigen-derived scheduler and integrated auto partitioner, not a pristine upstream Eigen release.
- The automatic/full-resident comparison uses the same executable; startup calibration versus fixed full residency is the controlled difference. The previous-prototype comparison also includes implementation changes and must not be attributed solely to calibration.
- Startup calibration is outside benchmark timing. The table above reports that additional initialization cost separately; short-lived applications must include it in end-to-end decisions.
- The startup probes sample launch, uniform work and skewed work. They cannot guarantee the best setting for each later algorithm, CPU placement, changing load, or other hardware. No CPU affinity was imposed.
- The previous 615-case results describe an earlier fixed-cohort adaptive prototype. Do not transfer its full-suite speedup to this implementation. Raw JSON, process-load snapshots, logs, manifest and per-process samples remain local under `results/rapid-hardware-20260923/`; they are not committed with this report.

## What changed

Startup calibration chooses the eligible resident cohort and a polling-cost
multiplier using launch, uniform-work and skewed-work probes. Workers outside
the cohort park when idle, but remain available for ordinary tasks and stealing.
Each worker measures polling/clock cost; private chunk sizes then adapt to
elapsed callback time. There is no fixed microsecond chunk target or CPU-model
table. Candidate settings and the 5% selection tolerance are still policy
choices, not a guarantee of workload-specific optimality.

Calibration runs automatically in the mailbox benchmark adapters. Applications
using the internal scheduler API explicitly call `rapid::CalibrateMailbox`
from a serialized startup/control context; ordinary pools retain their parking
default. The default 50 ms probe budget is checked between synchronous probes,
so OS preemption can extend total wall-clock time.

The same executable supplies the automatic and new-full-resident columns.
`OOX_RAPID_AUTOCALIBRATE=0` disables startup search and retains full residency;
`OOX_RAPID_RESIDENT_LIMIT=N` fixes the cohort and also disables the search.
The historical prototype uses a separate frozen executable. All configurations
use uniform initial work shares.

## Findings and limitations

The strongest controlled gain is at 16 workers: automatic selection is 3.425×
the same implementation with full residency. Launch/4096 improves from
351.300 µs to 6.734 µs, versus 30.423 µs for Eigen auto. These launch timings
emphasize overhead and must not be generalized to all application work.

Small hyperbolic SpMV remains a regression: 151.934 versus 123.095 µs at eight
workers (23.4% more elapsed time), and 211.945 versus 164.038 µs at sixteen
(29.2% more). Large hyperbolic SpMV takes 4.2% and 7.7% more time than auto at
those worker counts. Balanced SpMV and competing loops also regress at sixteen.
All of these cases remain in the aggregate.

The startup probes may underrepresent short irregular loops, but wakeup cost
is not established as the cause. Isolating cohort size, polling frequency,
CPU placement and donation timing requires further controlled experiments.
This run used one loaded machine, not a quiet multi-platform laboratory.
Neither Eigen static nor fixed-range Rapid was remeasured in this matrix.

## Validation and provenance

The measured source was based on branch `kkalpha-pr37-on-pr35`, HEAD
`ae67dfa7205910edf9c449b2ede2ee6bfa4ff983`, with the mailbox/calibration changes
subsequently committed alongside this report.

- Complete Release build succeeded.
- Full local CTest: 471 passed, 3 skipped, zero failures (474 registered).
- Targeted Debug UndefinedBehaviorSanitizer tests: 63/63 passed.
- Five lifecycle/reconfiguration checks repeated twenty times: 100 passes.
- Standalone calibration-header compilation passed.
- Both handoff policies were covered by correctness tests, including generated
  cases checked against serial oracles, cancellation and concurrent residency
  changes. ASan/TSan success and remote CI success are not claimed here.

Compiler: Apple Clang 16.0.0.16000026, arm64, C++20, Release `-O3 -DNDEBUG`,
using the macOS 15.2 SDK. Benchmark task counters were disabled; calibration
metadata was collected outside the timed loop. The shuffled process order used
seed 230918, five fresh processes per configuration/worker count, and a minimum
0.15-second measurement per case.

Frozen executable SHA-256 identifiers:

| Configuration | SHA-256 |
|---|---|
| Automatic and new full resident | `6746ee51bf1226dd0f2586b2955491b3d84a94fe6f060a7f0f689190fbde8f0e` |
| Eigen auto | `862e7f7249810cb9d371d6d546c1b4503c3a2fc387c2ef9b93469ee4672412a3` |

The full manifest and source snapshots remain in the local results directory.
The next validation step is a full workload-grid run with automatic calibration
and replication on a quiet second machine, retaining short hyperbolic SpMV as
an explicit regression case.
