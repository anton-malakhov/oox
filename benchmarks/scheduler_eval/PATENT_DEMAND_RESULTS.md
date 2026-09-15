# Patent demand range results

The implementation is described in [PATENT.md](../../oox/eigen/PATENT.md).

This comparison uses the same pool instance for every policy within each worker-count run. Five policies were interleaved by case for seven repetitions. The original workload code and inputs were shared; only the range provider changed.

Times below are median milliseconds. The Eigen column is the existing EIGEN_STEALING range provider, which uses grain one when the caller does not supply a grain. Coarse Eigen uses the same provider with the prior OOX adapter's 8P/1024 grain rule. The two baselines must not be conflated.


| Workers | Workload | Eigen | Coarse Eigen | Patent demand | Eigen / Patent |
|---:|---|---:|---:|---:|---:|
| 8 | Launch/262144 | 29.95944 | 0.09373 | 0.08263 | 362.56x |
| 8 | Scan/20 | 238.97779 | 0.90687 | 0.78425 | 304.72x |
| 8 | Scan/16 | 15.16293 | 0.43435 | 0.31246 | 48.53x |
| 8 | Bfs<GraphKind::SquareGrid, BfsPolicy::Flat>/16384 | 6.85816 | 4.21170 | 3.35343 | 2.05x |
| 8 | Bfs<GraphKind::Rmat, BfsPolicy::Flat>/16384 | 4.64503 | 1.66323 | 1.71906 | 2.70x |
| 8 | VariableCost<CostKind::Exponential>/262144 | 30.00927 | 0.39544 | 0.39586 | 75.81x |
| 8 | VariableCost<CostKind::Linear>/262144 | 29.21379 | 2.11659 | 2.07812 | 14.06x |
| 8 | VariableCost<CostKind::Periodic>/262144 | 30.03700 | 0.94648 | 0.93044 | 32.28x |
| 8 | VariableCost<CostKind::Shuffled>/262144 | 29.33133 | 2.18092 | 2.16815 | 13.53x |
| 8 | SpmvBenchmark<SparseKind::Hyperbolic>/4096 | 0.25709 | 0.11749 | 0.09450 | 2.72x |
| 8 | SpmvBenchmark<SparseKind::Balanced>/4096 | 0.28708 | 0.11755 | 0.11720 | 2.45x |
| 8 | ChangingWorkerAvailability/262144 | 29.30106 | 6.58655 | 6.43780 | 4.55x |
| 8 | SampleSort<KeyKind::ReverseSorted>/65536 | 0.11694 | 0.11588 | 0.09235 | 1.27x |
| 8 | MatrixTranspose | 0.04437 | 0.04415 | 0.00855 | 5.19x |
| 12 | Launch/262144 | 30.71573 | 0.07983 | 0.08105 | 378.99x |
| 12 | Scan/20 | 248.32038 | 1.17462 | 0.95766 | 259.30x |
| 12 | Scan/16 | 15.81386 | 0.70296 | 0.44075 | 35.88x |
| 12 | Bfs<GraphKind::SquareGrid, BfsPolicy::Flat>/16384 | 8.18884 | 5.85677 | 4.03461 | 2.03x |
| 12 | Bfs<GraphKind::Rmat, BfsPolicy::Flat>/16384 | 4.80979 | 1.63198 | 1.65657 | 2.90x |
| 12 | VariableCost<CostKind::Exponential>/262144 | 31.85923 | 0.31757 | 0.33630 | 94.74x |
| 12 | VariableCost<CostKind::Linear>/262144 | 31.79594 | 1.54016 | 1.53734 | 20.68x |
| 12 | VariableCost<CostKind::Periodic>/262144 | 32.17277 | 0.71602 | 0.74917 | 42.94x |
| 12 | VariableCost<CostKind::Shuffled>/262144 | 31.38963 | 1.67716 | 1.60090 | 19.61x |
| 12 | SpmvBenchmark<SparseKind::Hyperbolic>/4096 | 0.27545 | 0.08533 | 0.08603 | 3.20x |
| 12 | SpmvBenchmark<SparseKind::Balanced>/4096 | 0.28504 | 0.09532 | 0.10235 | 2.79x |
| 12 | ChangingWorkerAvailability/262144 | 30.98140 | 4.79608 | 4.87694 | 6.35x |
| 12 | SampleSort<KeyKind::ReverseSorted>/65536 | 0.10565 | 0.10556 | 0.09386 | 1.13x |
| 12 | MatrixTranspose | 0.05349 | 0.05365 | 0.00867 | 6.17x |

The patent mode has a lower median than the existing Eigen provider in all 28 case/worker combinations in this run. The coarse control is already competitive on several cases. There is no claim of a universal win over tuned partitioning.

The useful-work workloads impose normal hardware limits: most of the hundreds-fold gain comes from removing scheduling work that grows with iteration count. The fixed-body synthetic experiment separately demonstrates an adaptive benefit: at twelve workers, front-heavy work takes 1.278 ms with the chosen policy versus 3.319 ms with coarse Eigen.


## Checkpoint experiment
At fixed initial depth five, checking demand only after a whole private block took 4.997 ms on the twelve-worker front-heavy synthetic case. Checking between callbacks reduced that to 1.315 ms. The chosen default uses initial depth three and the earlier checkpoint, which measured 1.278 ms. This comparison isolates the timing of feedback; it does not rely only on a stack trace or a changed caller.

The same synthetic comparisons used nine shuffled repetitions, one persistent pool, and a serial result oracle after every sample. The cheap-body result was 36.838 ms for grain-one Eigen and 0.0599 ms for the chosen policy at twelve workers. The analogous eight-worker result was 33.615 ms versus 0.0627 ms.


## Inputs and reproducibility
- CPU: Apple M4 Max, 12 performance and 4 efficiency cores; no core pinning.
- Compiler: LLVM 19, C++20, -O3, NDEBUG, system task allocation.
- Native policy modes: fine, coarse, block_end_k5, patent (early checks, depth three), static_k3 (feedback off).
- Scan/20 and Scan/16 contain 1,048,576 and 65,536 values. BFS uses 16,384 vertices.
- SpMV inputs are fixed at 2,119 rows and 4,115 columns; transpose is 68 by 68.
- The availability case occupies half the pool, then releases those workers during the computation.
- The native corpus includes distributions and algorithms not used in the five-case synthetic checkpoint experiment.
- Numerical checks, generated range oracles, nesting, concurrent roots, cancellation, unavailable-worker progress, and allocation-failure recovery passed.
- Local validation: 121 Release tests, 215 exception-enabled UBSan tests, and four existing benchmark-entry smoke/validator tests.
- Raw benchmark files, commands, seeds, prototypes, and logs are retained locally in the demand worktree under build-demand/patent-range-20260914T202201Z.
- The earlier 1,120-sample separate-process run had high dispersion and is exploratory; its values are not used for the headline comparison here.

Summary data with sample counts and dispersion: [native.csv](patent_results/native.csv), [synthetic.csv](patent_results/synthetic.csv).
