# Patent-only Eigen results

> Measurements of commit 415dd8f, including experimental controls that are now
> removed. See [PATENT_DEFAULT_COMPARISONS.md](PATENT_DEFAULT_COMPARISONS.md)
> for the current default-only implementation and direct OOX comparisons.

These measurements use ordinary Eigen tasks. The patent header has no Rapid
dependency, and both benchmark modes use the same existing pool implementation.
The existing pool also supports separate Rapid modes; this run creates no
Rapid runtime or activation. Compiler dependency checks confirm that the
patent test and benchmark do not include rapid_start.h or rapid_start_model.h.

Times are medians in milliseconds. Lower is better. Default Eigen is the
existing EIGEN_STEALING range provider at grain one. The coarse control uses
the same provider with min(1024, ceil(N/(8P))) when no grain is supplied.

## Native workloads

Each worker-count run reused one pool. Five policies were shuffled by case
for seven repetitions (980 samples total). Only the range provider changed;
workloads and dimensions were frozen from the preceding experiment.

| Workers | Workload | Default Eigen | Coarse Eigen | Patent only | Default / patent |
|---:|---|---:|---:|---:|---:|
| 8 | Bfs<GraphKind::Rmat, BfsPolicy::Flat>/16384 | 4.56978 | 1.47649 | 1.51351 | 3.02x |
| 8 | Bfs<GraphKind::SquareGrid, BfsPolicy::Flat>/16384 | 6.44473 | 3.90315 | 3.65649 | 1.76x |
| 8 | ChangingWorkerAvailability/262144 | 29.38562 | 6.54969 | 6.40813 | 4.59x |
| 8 | Launch/262144 | 30.57775 | 0.09153 | 0.09760 | 313.29x |
| 8 | MatrixTranspose | 0.04357 | 0.04397 | 0.07417 | 0.59x |
| 8 | SampleSort<KeyKind::ReverseSorted>/65536 | 0.11573 | 0.11420 | 0.12561 | 0.92x |
| 8 | Scan/16 | 15.36831 | 0.39889 | 0.38809 | 39.60x |
| 8 | Scan/20 | 240.64771 | 0.83163 | 0.94771 | 253.93x |
| 8 | SpmvBenchmark<SparseKind::Balanced>/4096 | 0.30428 | 0.11564 | 0.12809 | 2.38x |
| 8 | SpmvBenchmark<SparseKind::Hyperbolic>/4096 | 0.26958 | 0.11588 | 0.09525 | 2.83x |
| 8 | VariableCost<CostKind::Exponential>/262144 | 31.85583 | 0.39343 | 0.41556 | 76.66x |
| 8 | VariableCost<CostKind::Linear>/262144 | 31.34929 | 2.09126 | 2.07817 | 15.09x |
| 8 | VariableCost<CostKind::Periodic>/262144 | 31.53637 | 0.94196 | 0.95798 | 32.92x |
| 8 | VariableCost<CostKind::Shuffled>/262144 | 31.32654 | 2.15759 | 2.17491 | 14.40x |
| 12 | Bfs<GraphKind::Rmat, BfsPolicy::Flat>/16384 | 4.99885 | 1.58825 | 1.64074 | 3.05x |
| 12 | Bfs<GraphKind::SquareGrid, BfsPolicy::Flat>/16384 | 8.44340 | 5.94143 | 5.63367 | 1.50x |
| 12 | ChangingWorkerAvailability/262144 | 33.00654 | 4.81725 | 4.80487 | 6.87x |
| 12 | Launch/262144 | 33.57317 | 0.08273 | 0.09773 | 343.52x |
| 12 | MatrixTranspose | 0.05514 | 0.05521 | 0.07869 | 0.70x |
| 12 | SampleSort<KeyKind::ReverseSorted>/65536 | 0.11201 | 0.11437 | 0.12822 | 0.87x |
| 12 | Scan/16 | 17.25335 | 0.70448 | 0.63887 | 27.01x |
| 12 | Scan/20 | 270.97546 | 1.19808 | 1.29066 | 209.95x |
| 12 | SpmvBenchmark<SparseKind::Balanced>/4096 | 0.29983 | 0.10253 | 0.11945 | 2.51x |
| 12 | SpmvBenchmark<SparseKind::Hyperbolic>/4096 | 0.28548 | 0.08664 | 0.09006 | 3.17x |
| 12 | VariableCost<CostKind::Exponential>/262144 | 33.96842 | 0.31579 | 0.36237 | 93.74x |
| 12 | VariableCost<CostKind::Linear>/262144 | 34.11675 | 1.54715 | 1.55394 | 21.96x |
| 12 | VariableCost<CostKind::Periodic>/262144 | 33.52937 | 0.69919 | 0.73745 | 45.47x |
| 12 | VariableCost<CostKind::Shuffled>/262144 | 33.29908 | 1.58220 | 1.61543 | 20.61x |

The patent policy has a lower median than default Eigen in 24 of 28 native
case/worker combinations. Transpose and reverse-sorted sample sort regress
at both worker counts. Coarse Eigen is competitive or faster on many cases.
Large ratios against grain-one Eigen mainly reflect avoided task creation.
They do not imply equivalent speedups over tuned granularity.

## Synthetic controls

Nine shuffled repetitions reused one pool; a serial oracle checked every
sample (450 samples total, 262,144 items per case). Feedback-disabled and
private-block-boundary variants share the same ordinary initial task tree.

| Workers | Workload | Default Eigen | Coarse Eigen | Patent only | No feedback | Block-end feedback |
|---:|---|---:|---:|---:|---:|---:|
| 8 | cheap | 34.90560 | 0.04613 | 0.06863 | 0.02340 | 0.03388 |
| 8 | uniform | 32.91220 | 1.77032 | 1.82624 | 1.82076 | 1.94334 |
| 8 | clustered | 34.04780 | 6.14958 | 6.15307 | 6.23552 | 6.22715 |
| 8 | phase | 32.15550 | 5.74613 | 5.74397 | 11.14570 | 6.33805 |
| 8 | front_heavy | 33.76900 | 3.25731 | 1.72277 | 11.16360 | 11.15050 |
| 12 | cheap | 35.99330 | 0.05504 | 0.07412 | 0.02356 | 0.04450 |
| 12 | uniform | 35.03870 | 1.24286 | 1.30168 | 1.35879 | 1.35858 |
| 12 | clustered | 36.37770 | 4.23713 | 4.22171 | 4.64431 | 4.31957 |
| 12 | phase | 35.69970 | 4.00339 | 3.90088 | 7.47892 | 4.37127 |
| 12 | front_heavy | 36.04900 | 3.11671 | 1.21518 | 11.24110 | 11.24090 |

The front-heavy case improves about 1.89x at eight workers and 2.56x at twelve
relative to coarse Eigen. With the same depth-three initial subdivision,
moving feedback checks from block boundaries to callback boundaries reduces
the twelve-worker median from 11.241 ms to 1.215 ms. This controlled change
makes demand visible while an expensive block still has an unexecuted remainder.
The cheap-body case remains slower than coarse Eigen. Its feedback-disabled
control is cheaper with the same initial task tree; this run does not separate
signal-task cost from callback-boundary polling cost.

## Reproduction and limits

- Host: Apple M4 Max, 12 performance and 4 efficiency cores; no pinning.
- Compiler: LLVM 19, C++20, -O3, NDEBUG; system task allocation.
- Date: 2026-09-15. Other desktop processes were active; their snapshot is logged.
- Synthetic seed: 2026091501; native seed: 2026091503.
- Native Google Benchmark minimum time: 0.02 seconds per sample.
- Scan dimensions: 65,536 and 1,048,576 values; BFS: 16,384 vertices.
- SpMV: 2,119 rows by 4,115 columns; transpose: 68 by 68.
- The availability case occupies half the pool and releases it during work.
- The synthetic block-end control uses depth three; the native block-end
  control retains the historical depth-five setting. Neither is the default.
- The benchmark fixture and generated range tests include numerical checks.
- Patent-only native IQR/median was at most 7.8%; small differences should be interpreted cautiously.
- No ASan, TSan, or Linux CI result is claimed for this revision.

Summary data: [native-patent-only.csv](patent_results/native-patent-only.csv),
[synthetic-patent-only.csv](patent_results/synthetic-patent-only.csv).

Raw samples, frozen fixture sources, compiler commands, the source-header hash,
architecture checks, and runner logs are in build-patent-only-audit in this
worktree. The older CSVs without the patent-only suffix belong to the
[historical Rapid hybrid](PATENT_DEMAND_RESULTS.md).

Implementation and adaptation contracts: [PATENT_ONLY.md](../../oox/eigen/PATENT_ONLY.md).
