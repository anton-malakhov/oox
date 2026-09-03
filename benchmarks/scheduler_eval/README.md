# Native scheduler evaluation

This directory is OOX's offline, reproducible implementation of the evaluation
families studied in *Fast work distribution for composable task scheduling
engines*. It compares ordinary work stealing, proactive mailbox publication,
adaptive timespan splitting, their combined Eigen policy, oneTBB partitioners,
and available OpenMP schedules through one shared workload layer.

The historical thesis repository is pinned as optional reference material at
`thirdparty/composable-parallel-scheduler-thesis`. The runner never clones or
downloads it. See `PROVENANCE.md` for the clean-room implementation policy and
the intentional differences from that source.

## Build and verify

Use a Release build for measurements:

```sh
cmake -S . -B build-eval -G Ninja \
  -DCMAKE_BUILD_TYPE=Release \
  -DOOX_BUILD_TESTS=OFF \
  -DOOX_BUILD_TASKBENCH=OFF \
  -DOOX_BUILD_SCHEDULER_EVALS=ON
cmake --build build-eval --target scheduler_eval_all -j
ctest --test-dir build-eval -R scheduler_eval --output-on-failure
```

CMake includes only installed/enabled backends. Eigen contributes
`EIGEN_STEALING`, `EIGEN_SHARING`, `EIGEN_STEALING_GRAINSIZE`, and
`EIGEN_SHARING_STEALING`. TBB contributes simple, automatic, and affinity
partitioners, plus the historical bitmask `RAPID_START` prototype. OpenMP
contributes static, dynamic-nonmonotonic, and guided-nonmonotonic schedules.

`RAPID_START` waits for every requested trapper task to register before its
warm-up publication. It supports at most 64 workers. Because the prototype has
one global publication descriptor and is not reentrant, its target omits nested
matrix multiplication and transpose; normalized reports use only cases present
in every selected mode. Nested workloads and registrations live in separate
sources that CMake attaches only to reentrant modes.

The `test_scheduler_eval_*` executables validate scan, reduction, all three
sparse distributions, convex hull, remove-duplicates, radix sort, and sample
sort for every built policy. Reentrant policies additionally validate nested
multiplication, transpose, and BFS.
The standard CI configuration keeps `OOX_BUILD_BENCHMARKS=OFF`; this research
suite and its JSON-to-report smoke test run only in an explicit opt-in build.

## Workload coverage

| Family | What it exposes |
| --- | --- |
| `Launch` | Flat task publication overhead across task counts |
| Four `Spin` payloads | Scheduler overhead under relax, shared atomic, isolated distributed-read, and thread-local-read work |
| `Reduce` | Flat blocks with non-power-of-two tails |
| `Scan` | Repeated synchronization across an up-sweep/down-sweep tree |
| Balanced SpMV | Equal row work; isolates distribution overhead |
| Hyperbolic SpMV | A few very heavy rows and a long light tail |
| Triangular SpMV | Gradually changing row width and work |
| Convex hull | Parallel block sorting and merging followed by a hull scan over square, disk, circle, and Kuzmin point distributions |
| Remove duplicates | Concurrent open-addressing over uniform, exponential, duplicate-heavy, and almost-sorted keys |
| Stable radix sort | Four parallel histogram/scatter passes over 32-bit keys |
| Sample sort | Deterministic sampling, parallel bucket distribution, and parallel bucket sorting |
| Flat/fixed/adaptive BFS | High-arity trees, parallel chains, dense/sparse phases, trunk-first, RMat, square/cube grids, and a small-world control |
| Variable-cost loops | Constant, uniform, exponential, Pareto, linear, clustered, periodic, shuffled, and phase-changing costs |
| Competing loops | Two simultaneous OOX loops contending for one worker pool |
| First touch | Serial versus parallel page initialization before parallel reads |
| Matrix multiply | Nested parallel regions with substantial inner work |
| Matrix transpose | Nested tiled regions with short inner tasks |

Inputs are deterministic and construction and validation stay outside measured
regions. A full SpMV run intentionally has the same large scale as the research
workload and can require several gigabytes; use `--smoke` before a full run.
Adaptive BFS uses SPTL's κ/α estimator rule with its 20 µs and 1.8 defaults;
see `THIRDPARTY.md` for the retained MIT notice.

Eigen benchmark JSON includes scheduled/executed tasks, successful steals,
failed steal rounds, worker sleeps, and observed sleeping time. BFS additionally
records nested launches, sequentialized inner loops, and the learned sequential
complexity limit. The counters are compiled only for scheduler-evaluation
targets.

## Startup and publication probes

The tools deliberately separate two costs that are often mixed together:

- `initialization_ns` measures `InitParallel`, including worker creation,
  pinning setup, and the initial warm-up.
- `scheduling_dist` timestamps each task's first instruction after a
  `ParallelFor` publication. It reports every task-to-worker assignment plus
  median and p99 arrival/spread statistics for `spin`, `barrier`, and
  `multitask` scenarios.
- `timespan_tuner_EIGEN_STEALING` uses one blocking task per worker after ten
  warm-ups. Its p99 maximum arrival is a candidate publication gate for the
  Eigen timespan policy; it is not a Rapid Start parameter or a hard guarantee.
  It is intentionally restricted to the non-adaptive baseline: an adaptive
  policy cannot split its first range after that range has blocked.
- `trace_spin` emits Chrome Trace Event JSON with task, iteration, worker,
  start, and duration. It visualizes externally observable execution; it does
  not invent internal mailbox/deque events that the public `ParallelFor` API
  cannot observe.

For the same reason, the full runner omits the blocking `barrier` scenario for
`EIGEN_STEALING_GRAINSIZE`. Spin and multitask measurements remain valid for
that mode.

## Run and inspect results

The runner discovers built policies, sets one worker count for all backends,
validates every JSON file, applies a subprocess timeout, and records the exact
OOX/PBBS/thesis revisions and build environment:

```sh
python3 benchmarks/scheduler_eval/run.py \
  --build build-eval --threads 16 --repetitions 5 --smoke

python3 benchmarks/scheduler_eval/run.py \
  --build build-eval --threads 16 --repetitions 5 --timeout 1800

python3 benchmarks/scheduler_eval/run.py \
  --build build-eval --threads 16 --cpu-node 0 --memory-node 1 --perf
```

Use `--filter REGEX` for a workload family and `--benchmark-min-time 9s` for
long throughput-quality samples. The default is 0.5 seconds per benchmark
case. A result's `complete` metadata field becomes true only after all selected
commands and report generation succeed.

On Linux, `--cpu-node` and `--memory-node` create explicit local or remote NUMA
placements; `--interleave-memory` selects interleaved allocation. `--perf`
records process-level cycles, instructions, cache misses, and CPU migrations,
with `--perf-events` available for another event list. Placement and event names
are retained in metadata. These options fail early when `numactl` or `perf` is
unavailable rather than silently running an uncontrolled experiment.

Each timestamped directory under `results/scheduler_eval` contains:

- `metadata.json`: schema, commits, host, compiler, flags, allocator, affinity,
  modes, thread count, and run profile;
- `raw/`: Google Benchmark data, scheduling distributions, and tuner data;
- `traces/`: Chrome-compatible task timelines;
- `summaries/`: raw CSV tables and a Markdown comparison;
- `plots/normalized.svg`: median-per-case geometric-mean comparison;
- `plots/time_vs_*.svg`: article-style absolute median time versus workload
  parameter, with one curve per scheduler and logarithmic axes;
- `summaries/absolute_time_sweeps.csv`: the effective X values and median
  microsecond values used by those plots;
- `plots/worker_initialization.svg`: cold worker creation/setup comparison;
- `plots/publication_spread.svg`: historical filename for the p99 task-start
  spread (last minus first observed callback); it is not publication-to-full-team
  activation unless required worker coverage is also established.

For publishable Linux measurements, fix CPU frequency policy, NUMA placement,
allocator, compiler, affinity, and background load outside the runner (for
example with `taskset`/`numactl`), then preserve those choices with the result.
Do not combine numbers from different metadata files without checking them.

PBBS application benchmarks remain in `benchmarks/pbbs`; they answer the
end-to-end application question, while this suite isolates scheduler mechanics.

## Fit the explanatory model

For a complete full result containing Rapid Start, `Launch`, and all three SpMV
families, fit the warm task-publication, useful-work, and residual-load-imbalance
terms, and report separately observed cold initialization with:

```sh
python3 benchmarks/scheduler_eval/model.py results/scheduler_eval/<result>
```

The command adds model parameters, per-case predictions, initialization
amortization, and an observed-versus-predicted SpMV plot to that result. The
research lineage, publication-time estimator, parameter-selection procedure,
published foundations, limitations, and next measurements are in
[*Estimating Rapid Start and choosing scheduler parameters*](PERFORMANCE_MODEL.md).
