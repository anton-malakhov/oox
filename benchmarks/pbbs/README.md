# PBBS eigen-mailbox reproduction

OOX tracks the original `EgorkaZ/pbbsbench` `eigen-mailbox` experiment as the
`thirdparty/pbbsbench` submodule, pinned at commit `396a299`. `run.py` uses that
explicit checkout and never clones PBBS itself. It can run either the untouched
historical Eigen plugin (`reference`) or the integrated OOX Eigen port (`oox`).
This preserves the original mailbox implementation as an experimental baseline
without duplicating its source tree inside OOX. On
non-Linux hosts, the driver only disables the reference plugin's Linux-specific
thread-affinity calls; its scheduling and mailbox code remains unchanged.

The `oox` backend does **not** obtain Eigen from the downloaded repository. It
combines the runtime mailbox scheduler in `oox/eigen` with the experimental
parallel-for layer in `benchmarks/eigen`. The external clone provides the PBBS
applications, inputs, validators, and—only for the explicit `reference`
backend—the untouched historical Eigen snapshot.

Initialize the dependency after cloning OOX, then run one small workload:

```sh
git submodule update --init thirdparty/pbbsbench
python3 benchmarks/pbbs/run.py --prepare-only
python3 benchmarks/pbbs/run.py --compile-only --threads 2
python3 benchmarks/pbbs/run.py --backend oox --threads 8 \
  --mode EIGEN_SHARING_STEALING \
  --benchmark integerSort/parallelRadixSort
```

Without an explicit backend, the driver runs OOX. Select both backends explicitly
to compare them. Their mode names differ: the reference uses `EIGEN_SIMPLE`, `EIGEN_TIMESPAN`,
`EIGEN_TIMESPAN_GRAINSIZE`, `EIGEN_STATIC`, and `EIGEN_RAPID`; OOX uses
`EIGEN_STEALING`, `EIGEN_SHARING`, `EIGEN_STEALING_GRAINSIZE`, and
`EIGEN_SHARING_STEALING`. Therefore `--mode` requires one `--backend`.

The historical reference targets Linux. It builds on macOS with the affinity
shim, but its original scheduler can stall there; use Linux for reference data.

The default suite covers every PBBS application requested by the OOX porting
plan: flat BFS, convex hull, remove-duplicates, radix and sample sort, suffix
array, nearest neighbors, Delaunay triangulation/refinement, ray casting,
minimum spanning forest, and spanning forest. Each workload runs PBBS's own
checker. The driver also corrects the pinned octree benchmark's small-input
underflow and selects its static build/query path, matching current canonical
PBBS behavior. Two stale success-return defects in the pinned deduplication
driver and suffix-array checker are corrected while the checkout is active;
their actual PBBS validators still decide correctness.

Add `--full` for paper-scale inputs. Full runs generate or download large
datasets and can take hours; use an otherwise idle Linux machine with fixed
affinity. Use `--all-benchmarks --full` for every benchmark in the pinned PBBS
tree, beyond the porting-plan set.

Raw logs are written below `cmake-build-pbbs/results` with the backend in each
filename. They retain PBBS's
per-input repetitions and geometric-mean summaries, allowing comparison with
the logs committed to the reference branch.

The driver temporarily adapts files in the submodule checkout and restores the
pinned versions when it exits, including after a benchmark error. PBBS leaves
some generated inputs and build products untracked in its checkout; the parent
repository ignores those while still reporting modifications to tracked files.

This split is intentional: scheduler microbenchmarks and the flat-versus-nested
BFS graph families are native CMake targets under `benchmarks/scheduler_eval`,
while PBBS remains the pinned upstream application suite so its algorithms,
generators, validators, and run protocol are not silently changed.
