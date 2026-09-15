# Default Eigen vs current patent implementation

The branch now retains one patent policy: worker-budget initial subdivision,
private ranges, stolen-signal demand feedback, and callback-boundary checks.
Initial relative depth is fixed at three. Rapid activation is not used.
The measured patent implementation is commit a6182b5.

## Which implementations are being compared?

| Label | Execution path | Default splitting |
|---|---|---|
| Default Eigen | Existing EIGEN_STEALING range provider | Grain one |
| Current patent | ParallelForPatent on ordinary Eigen tasks | Worker budget plus demand feedback |

## Native range comparisons

Median milliseconds; lower is better. Ratios above one favour the patent
policy. The two reported implementations contribute 392 samples: seven
shuffled repetitions across fourteen workloads and two worker counts.
Each worker-count run reused one pool, with identical workload code and inputs.

| Workers | Workload | Default Eigen ms | Current patent ms | Default / patent |
|---:|---|---:|---:|---:|
| 8 | Launch/262144 | 29.80325 | 0.09625 | 309.65x |
| 8 | Scan/20 | 234.83013 | 0.93788 | 250.38x |
| 8 | Scan/16 | 14.33296 | 0.38805 | 36.94x |
| 8 | Bfs<GraphKind::SquareGrid, BfsPolicy::Flat>/16384 | 6.20247 | 3.61105 | 1.72x |
| 8 | Bfs<GraphKind::Rmat, BfsPolicy::Flat>/16384 | 4.37644 | 1.53040 | 2.86x |
| 8 | VariableCost<CostKind::Exponential>/262144 | 30.01263 | 0.40651 | 73.83x |
| 8 | VariableCost<CostKind::Linear>/262144 | 28.14304 | 2.03825 | 13.81x |
| 8 | VariableCost<CostKind::Periodic>/262144 | 31.67367 | 0.94677 | 33.45x |
| 8 | VariableCost<CostKind::Shuffled>/262144 | 28.84988 | 2.16935 | 13.30x |
| 8 | SpmvBenchmark<SparseKind::Hyperbolic>/4096 | 0.25980 | 0.09392 | 2.77x |
| 8 | SpmvBenchmark<SparseKind::Balanced>/4096 | 0.28483 | 0.12652 | 2.25x |
| 8 | ChangingWorkerAvailability/262144 | 28.42117 | 6.32242 | 4.50x |
| 8 | SampleSort<KeyKind::ReverseSorted>/65536 | 0.11157 | 0.12353 | 0.90x |
| 8 | MatrixTranspose | 0.04181 | 0.07453 | 0.56x |
| 12 | Launch/262144 | 31.12892 | 0.09402 | 331.08x |
| 12 | Scan/20 | 252.15254 | 1.29618 | 194.53x |
| 12 | Scan/16 | 16.18683 | 0.65430 | 24.74x |
| 12 | Bfs<GraphKind::SquareGrid, BfsPolicy::Flat>/16384 | 8.40176 | 5.69534 | 1.48x |
| 12 | Bfs<GraphKind::Rmat, BfsPolicy::Flat>/16384 | 4.65000 | 1.54998 | 3.00x |
| 12 | VariableCost<CostKind::Exponential>/262144 | 31.84842 | 0.34362 | 92.68x |
| 12 | VariableCost<CostKind::Linear>/262144 | 31.97200 | 1.47503 | 21.68x |
| 12 | VariableCost<CostKind::Periodic>/262144 | 32.38600 | 0.71215 | 45.48x |
| 12 | VariableCost<CostKind::Shuffled>/262144 | 31.70554 | 1.53643 | 20.64x |
| 12 | SpmvBenchmark<SparseKind::Hyperbolic>/4096 | 0.27937 | 0.08671 | 3.22x |
| 12 | SpmvBenchmark<SparseKind::Balanced>/4096 | 0.28256 | 0.11206 | 2.52x |
| 12 | ChangingWorkerAvailability/262144 | 31.61750 | 4.68335 | 6.75x |
| 12 | SampleSort<KeyKind::ReverseSorted>/65536 | 0.10902 | 0.11979 | 0.91x |
| 12 | MatrixTranspose | 0.05462 | 0.07725 | 0.71x |

The patent default has a lower median than default Eigen in 24/28 native
comparisons. Small transpose and reverse-sort cases remain regressions.
The large ratios against grain-one Eigen mainly reflect avoiding excessive
task creation; they are not universal numerical-kernel speedups.

## Uneven-work comparisons

Median milliseconds for 262,144 iterations. Nine shuffled repetitions with
a serial result oracle provide 180 samples for the two reported
implementations: default Eigen and the current patent policy.

| Workers | Workload | Default Eigen ms | Current patent ms | Default / patent |
|---:|---|---:|---:|---:|
| 8 | cheap | 34.19300 | 0.07389 | 462.73x |
| 8 | uniform | 32.84310 | 1.85942 | 17.66x |
| 8 | clustered | 32.89570 | 6.19778 | 5.31x |
| 8 | phase | 33.61320 | 5.80777 | 5.79x |
| 8 | front_heavy | 33.73630 | 1.75347 | 19.24x |
| 12 | cheap | 39.20780 | 0.08299 | 472.43x |
| 12 | uniform | 35.71990 | 1.38303 | 25.83x |
| 12 | clustered | 36.31170 | 4.45263 | 8.16x |
| 12 | phase | 36.59320 | 4.09738 | 8.93x |
| 12 | front_heavy | 36.44960 | 1.33311 | 27.34x |

On the front-heavy workload, the current patent policy is 19.24x faster
at eight workers and 27.34x faster at twelve than default Eigen.
These are observed advantages on this distribution, not universal guarantees.

## Validation and reproducibility

- LLVM 19, C++20, -O3, NDEBUG, system allocation; Apple M4 Max, 12P + 4E.
- Local desktop run on 2026-09-15, no core pinning; other applications active.
- Tests passed: 121 Release, 215 exception-enabled UBSan, four benchmark checks.
- Range test includes serial oracles, nesting, concurrent roots, cross-pool
  calls, cancellation, queue saturation, allocation failure, and reuse.
- Synthetic seed: 2026091501; native shuffle seed: 2026091503.
- Google Benchmark minimum time: 0.02 seconds per sample.
- Scan: 65,536 and 1,048,576 elements; BFS: 16,384 vertices;
  SpMV: 2,119 by 4,115; small transpose: 68 by 68.
- Both reported range providers use a participating caller and P-1 background workers.
- Per-mode dispersion and sample counts are in the CSVs. Small timing
  differences should not be overinterpreted on this host.
- Raw data, exact compiler commands, retained baseline sources, and source
  hashes are in build-patent-default-audit.

Data: [native-default.csv](patent_results/native-default.csv),
[synthetic-default.csv](patent_results/synthetic-default.csv).
The tables above select the fine and patent rows from these run archives.

Code: [patent_parallel_for.h](../../oox/eigen/patent_parallel_for.h),
[adaptation contracts](../../oox/eigen/PATENT_ONLY.md).
