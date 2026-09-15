# PBBS eigen-mailbox reproduction

The required original benchmark sources now live in-tree under `vendor/`.
See [vendoring and license policy](VENDORING.md) and `vendor_manifest.json`.
No research submodule or upstream checkout is needed to build the suite.

`--backend serial --mode SERIAL` selects the original serial BFS, hull, hash
deduplication, radix/comparison sort, divsufsort, MST and spanning-forest
implementations. It uses the pinned sequential Parlay plugin and one execution
thread regardless of `--threads`. The selected compiler is also applied to
serial makefiles, with C++20 and platform-specific portability flags.
`--compile-only` builds all eight; `--ci-smoke` runs the six portable serial
sort/dedup/suffix/forest checker suites after compiling all eight. Explicit
`--benchmark` can select any registered serial implementation. An unknown
benchmark or failed compiler now terminates the driver rather than producing
an apparently successful empty or failed build.

The additional `--backend oox-tasks --mode OOX_TASKS` adapter evaluates actual
`oox::run`/`oox::var` task graphs for the same application suite. It compiles the
OOX worker count from `--threads` and uses `--task-grain 1024` whenever PBBS does
not supply a grain. Explicit PBBS grains are respected. These choices are
visible in the retained compiler commands. OOX's pool has the requested worker
threads; PBBS's external main thread receives one additional scratch-storage
slot and may execute the inline branch of a fork/join. Account for that caller
when comparing resource budgets against Eigen modes that include their main
thread in the configured worker count.

All application families remain available under this adapter. Compilation
coverage is distinct from runtime checker coverage; the portable CI runtime
set still excludes the pinned BFS and hull cases described below.

OOX vendors the selected sources from the original `EgorkaZ/pbbsbench`
`eigen-mailbox` experiment at commit `396a299`. `run.py` verifies their hashes
and builds in a separate working copy. It can run either the untouched
historical Eigen plugin (`reference`) or the integrated OOX Eigen port (`oox`).
This preserves the original mailbox implementation as an experimental baseline
without depending on a separately maintained repository. On
non-Linux hosts, the driver only disables the reference plugin's Linux-specific
thread-affinity calls; its scheduling and mailbox code remains unchanged.

The `oox` backend does **not** obtain Eigen from the historical snapshot. It
combines the runtime mailbox scheduler in `oox/eigen` with the experimental
parallel-for layer in `benchmarks/eigen`. The vendored source provides the PBBS
applications, inputs, validators, and—only for the explicit `reference`
backend—the untouched historical Eigen snapshot.

Verify the snapshot, acquire originals separately, then run the suite:

```sh
python3 benchmarks/pbbs/run.py --prepare-only
python3 benchmarks/scheduler_eval/tools/datasets.py --archives-only --bundled
python3 benchmarks/pbbs/run.py --compile-only --threads 2
python3 benchmarks/pbbs/run.py --ci-smoke --timeout 600 --threads 2
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

`--ci-smoke` runs the small deduplication, radix-sort, sample-sort, suffix-array,
nearest-neighbor, Delaunay triangulation/refinement, ray-casting, minimum-
spanning-forest, and spanning-forest suites, including PBBS's real output
checkers. Flat BFS and convex hull remain compile-covered but are excluded from
this portable runtime profile: their pinned small suites fail under both OOX
and the untouched reference scheduler on macOS. `--timeout` bounds each
backend/mode suite invocation and reports the retained log when the limit is
exceeded.

Add `--full` for paper-scale inputs after preparing all required original data
archives (`datasets.py --bundled --archives-only`). Runs generate large inputs
and can take hours. Use `--archives <directory>` to select the archive cache.
`--all-benchmarks` selects all vendored implementations for the chosen backend;
unrelated upstream applications are intentionally not included.

Raw logs are written below `cmake-build-pbbs/results` with the backend in each
filename. They retain PBBS's
per-input repetitions and geometric-mean summaries, allowing comparison with
the logs committed to the reference branch.

Each run gets a unique `pbbs-build-*/source` directory below its output directory.
Only this copy is adapted. Its build products and generated inputs remain there
for inspection, including after an error; checked-in sources are never rewritten.
The original archive cache defaults to `results/pbbs-archives`; archives are
hash-checked before copying into a build. Missing archives fail the upstream
input recipe rather than initiating a code checkout or substituting data.

On the supported POSIX hosts, benchmark commands have independent process
groups. Timeouts and keyboard interruption terminate the group, including
ordinary make/compiler/benchmark descendants, while retaining logs and the
isolated build directory. Descendants that explicitly create their own sessions
are outside this cleanup guarantee.

This split is intentional: scheduler microbenchmarks and the flat-versus-nested
BFS graph families are native CMake targets under `benchmarks/scheduler_eval`,
while PBBS remains a pinned, attributed in-tree source snapshot so its algorithms,
generators, validators, and run protocol are not silently changed.
