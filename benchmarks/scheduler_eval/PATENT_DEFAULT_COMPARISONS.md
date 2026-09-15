# Default patent-only Eigen: updated comparisons

The branch now retains one patent policy: worker-budget initial subdivision,
private ranges, stolen-signal demand feedback, and callback-boundary checks.
Initial relative depth is fixed at three. Feedback-off, block-end, and
depth-selection variants have been removed from the implementation.
Rapid activation is not used.

**This patent patch does not change normal OOX task execution.** Ordinary
OOX still calls its existing task scheduler; it does not call the new range
partitioner. The before/after OOX checks below use identical machine code
and data sections, verified at both worker counts.

## Which implementations are being compared?

| Label | Execution path | Default splitting |
|---|---|---|
| Default Eigen | Existing EIGEN_STEALING range provider | Grain one |
| Coarse Eigen | The same Eigen range provider | min(1024, ceil(N/(8P))) |
| Patent default | ParallelForPatent on ordinary Eigen tasks | Worker budget plus demand feedback |
| OOX | Actual oox::run tasks, returned variables, and joins | Frozen TaskRange adapter with the coarse grain rule |

**Coarse Eigen is not OOX.** It shares the adapter's grain rule but does not
construct OOX task/result dependencies. Likewise, using ordinary OOX does not
automatically select the patent partitioner.

## Native range comparisons

Median milliseconds; lower is better. Ratios above one favour the patent
policy. Seven shuffled repetitions, three policies, fourteen workloads,
and two worker counts produced 588 samples. Each worker-count run reused
one pool, with identical workload code and inputs across range providers.

| Workers | Workload | Default Eigen ms | Coarse Eigen ms | Patent ms | Default / patent | Coarse / patent |
|---:|---|---:|---:|---:|---:|---:|
| 8 | Launch/262144 | 29.80325 | 0.08778 | 0.09625 | 309.65x | 0.91x |
| 8 | Scan/20 | 234.83013 | 0.80617 | 0.93788 | 250.38x | 0.86x |
| 8 | Scan/16 | 14.33296 | 0.38303 | 0.38805 | 36.94x | 0.99x |
| 8 | Bfs<GraphKind::SquareGrid, BfsPolicy::Flat>/16384 | 6.20247 | 3.82893 | 3.61105 | 1.72x | 1.06x |
| 8 | Bfs<GraphKind::Rmat, BfsPolicy::Flat>/16384 | 4.37644 | 1.46938 | 1.53040 | 2.86x | 0.96x |
| 8 | VariableCost<CostKind::Exponential>/262144 | 30.01263 | 0.38856 | 0.40651 | 73.83x | 0.96x |
| 8 | VariableCost<CostKind::Linear>/262144 | 28.14304 | 2.11439 | 2.03825 | 13.81x | 1.04x |
| 8 | VariableCost<CostKind::Periodic>/262144 | 31.67367 | 0.92824 | 0.94677 | 33.45x | 0.98x |
| 8 | VariableCost<CostKind::Shuffled>/262144 | 28.84988 | 2.16600 | 2.16935 | 13.30x | 1.00x |
| 8 | SpmvBenchmark<SparseKind::Hyperbolic>/4096 | 0.25980 | 0.11494 | 0.09392 | 2.77x | 1.22x |
| 8 | SpmvBenchmark<SparseKind::Balanced>/4096 | 0.28483 | 0.11518 | 0.12652 | 2.25x | 0.91x |
| 8 | ChangingWorkerAvailability/262144 | 28.42117 | 6.49375 | 6.32242 | 4.50x | 1.03x |
| 8 | SampleSort<KeyKind::ReverseSorted>/65536 | 0.11157 | 0.11386 | 0.12353 | 0.90x | 0.92x |
| 8 | MatrixTranspose | 0.04181 | 0.04210 | 0.07453 | 0.56x | 0.56x |
| 12 | Launch/262144 | 31.12892 | 0.07707 | 0.09402 | 331.08x | 0.82x |
| 12 | Scan/20 | 252.15254 | 1.19487 | 1.29618 | 194.53x | 0.92x |
| 12 | Scan/16 | 16.18683 | 0.71871 | 0.65430 | 24.74x | 1.10x |
| 12 | Bfs<GraphKind::SquareGrid, BfsPolicy::Flat>/16384 | 8.40176 | 6.03424 | 5.69534 | 1.48x | 1.06x |
| 12 | Bfs<GraphKind::Rmat, BfsPolicy::Flat>/16384 | 4.65000 | 1.48770 | 1.54998 | 3.00x | 0.96x |
| 12 | VariableCost<CostKind::Exponential>/262144 | 31.84842 | 0.29818 | 0.34362 | 92.68x | 0.87x |
| 12 | VariableCost<CostKind::Linear>/262144 | 31.97200 | 1.49207 | 1.47503 | 21.68x | 1.01x |
| 12 | VariableCost<CostKind::Periodic>/262144 | 32.38600 | 0.67253 | 0.71215 | 45.48x | 0.94x |
| 12 | VariableCost<CostKind::Shuffled>/262144 | 31.70554 | 1.55594 | 1.53643 | 20.64x | 1.01x |
| 12 | SpmvBenchmark<SparseKind::Hyperbolic>/4096 | 0.27937 | 0.08486 | 0.08671 | 3.22x | 0.98x |
| 12 | SpmvBenchmark<SparseKind::Balanced>/4096 | 0.28256 | 0.09478 | 0.11206 | 2.52x | 0.85x |
| 12 | ChangingWorkerAvailability/262144 | 31.61750 | 4.82111 | 4.68335 | 6.75x | 1.03x |
| 12 | SampleSort<KeyKind::ReverseSorted>/65536 | 0.10902 | 0.10918 | 0.11979 | 0.91x | 0.91x |
| 12 | MatrixTranspose | 0.05462 | 0.05471 | 0.07725 | 0.71x | 0.71x |

The patent default has a lower median than default Eigen in 24/28 native
comparisons. Small transpose and reverse-sort cases remain regressions.
Coarse Eigen is competitive or faster on several workloads. The large
ratios against grain-one Eigen mainly reflect avoiding excessive task creation;
they are not equivalent gains over already-coarsened code.

## Uneven-work controls

Median milliseconds for 262,144 iterations. Nine shuffled repetitions with
a serial result oracle produced 270 samples. Only the retained default
patent policy and the two Eigen baselines were measured.

| Workers | Workload | Default Eigen ms | Coarse Eigen ms | Patent ms | Coarse / patent |
|---:|---|---:|---:|---:|---:|
| 8 | cheap | 34.19300 | 0.05279 | 0.07389 | 0.71x |
| 8 | uniform | 32.84310 | 1.82631 | 1.85942 | 0.98x |
| 8 | clustered | 32.89570 | 6.19527 | 6.19778 | 1.00x |
| 8 | phase | 33.61320 | 5.87133 | 5.80777 | 1.01x |
| 8 | front_heavy | 33.73630 | 3.26349 | 1.75347 | 1.86x |
| 12 | cheap | 39.20780 | 0.05500 | 0.08299 | 0.66x |
| 12 | uniform | 35.71990 | 1.35644 | 1.38303 | 0.98x |
| 12 | clustered | 36.31170 | 4.43404 | 4.45263 | 1.00x |
| 12 | phase | 36.59320 | 4.20406 | 4.09738 | 1.03x |
| 12 | front_heavy | 36.44960 | 3.25309 | 1.33311 | 2.44x |

The front-heavy case is the clearest benefit beyond coarse partitioning:
1.86x at eight workers and 2.44x at twelve.
This is an observed advantage on that distribution, not a universal
guarantee. The default's signal tasks and demand checks still add work on
short or uniformly cheap loops.

## Does normal OOX become slower with this patch?

**The patch does not change the code executed by ordinary OOX.** The baseline
is this branch's pre-port commit 93b8b5e. The relevant before/after chain is:

1. oox::run allocates a functional task and removes its satisfied prerequisites.
2. task_node::remove_prerequisite calls task::spawn when the task is ready.
3. The Eigen backend calls pool.Schedule(MakeTask(...)).
4. OOX completion uses its existing task state and wait/notification path.

The OOX header, backend selector, Eigen task wrapper, and pool implementation
have no diff against that baseline. Compiling the same frozen OOX benchmark
against each revision produced identical contents, virtual addresses, and
sizes for every Mach-O section, including machine code and stored data, at
both eight and twelve workers. Whole-file hashes differ, so this is section
equivalence rather than a claim of byte-identical executable containers.

Six actual OOX workloads were also run before and after, using seven shuffled
paired repetitions at each worker count (168 samples). Positive change means
a higher observed time. These timings execute identical code; differences
are run-to-run variation, not evidence of a changed OOX implementation.

| Workers | OOX workload | Before ms | Current ms | Observed change |
|---:|---|---:|---:|---:|
| 8 | Launch/262144 | 0.10717 | 0.10718 | +0.0% |
| 8 | Scan/20 | 1.87652 | 1.88070 | +0.2% |
| 8 | Bfs<GraphKind::SquareGrid, BfsPolicy::Flat>/16384 | 8.98774 | 8.96657 | -0.2% |
| 8 | ChangingWorkerAvailability/262144 | 6.54179 | 6.57155 | +0.5% |
| 8 | SampleSort<KeyKind::ReverseSorted>/65536 | 0.16189 | 0.16287 | +0.6% |
| 8 | MatrixTranspose | 0.16781 | 0.16869 | +0.5% |
| 12 | Launch/262144 | 0.11981 | 0.12008 | +0.2% |
| 12 | Scan/20 | 2.73439 | 2.75179 | +0.6% |
| 12 | Bfs<GraphKind::SquareGrid, BfsPolicy::Flat>/16384 | 12.62100 | 12.53292 | -0.7% |
| 12 | ChangingWorkerAvailability/262144 | 4.83003 | 4.79315 | -0.8% |
| 12 | SampleSort<KeyKind::ReverseSorted>/65536 | 0.17148 | 0.17074 | -0.4% |
| 12 | MatrixTranspose | 0.17702 | 0.17634 | -0.4% |

This answers whether the patent patch regresses existing OOX on this branch.
It does not measure a future OOX adapter that invokes ParallelForPatent.
That integration belongs to the later branch. OOX task graphs can have
additional allocation, result-state, and join work compared with a direct
range loop, so those two interfaces are not interchangeable benchmark labels.

## Validation and reproducibility

- LLVM 19, C++20, -O3, NDEBUG, system allocation; Apple M4 Max, 12P + 4E.
- Local desktop run on 2026-09-15, no core pinning; other applications active.
- Tests passed: 121 Release, 215 exception-enabled UBSan, four benchmark checks.
- Range test includes serial oracles, nesting, concurrent roots, cross-pool
  calls, cancellation, queue saturation, allocation failure, and reuse.
- Synthetic seed: 2026091501; native shuffle seed: 2026091503;
  OOX before/after shuffle seed: 2026091505.
- Google Benchmark minimum time: 0.02 seconds per sample.
- Scan: 65,536 and 1,048,576 elements; BFS: 16,384 vertices;
  SpMV: 2,119 by 4,115; small transpose: 68 by 68.
- Range tests use a participating caller and P-1 background workers.
  OOX tests use P background workers and a waiting external caller.
  Before/after OOX comparisons keep that arrangement identical.
- Per-mode dispersion and sample counts are in the CSVs. Small timing
  differences should not be overinterpreted on this host.
- Raw data, exact compiler commands, retained baseline sources, source hashes,
  and section-equivalence evidence are in build-patent-default-audit.

Data: [native-default.csv](patent_results/native-default.csv),
[synthetic-default.csv](patent_results/synthetic-default.csv),
[oox-default.csv](patent_results/oox-default.csv).

Code: [patent_parallel_for.h](../../oox/eigen/patent_parallel_for.h),
[OOX Eigen wrapper](../../oox/backends/eigen/backend.h),
[adaptation contracts](../../oox/eigen/PATENT_ONLY.md).
The preceding [control experiment](PATENT_ONLY_RESULTS.md) is retained
separately; its alternative policies are no longer part of the header.
