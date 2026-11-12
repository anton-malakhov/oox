# Native scheduler evaluation

Start with the [MR review guide](docs/REVIEW_GUIDE.md) for scope and review order.

## Directory layout

- `bench/`: benchmark registrations and timed entry points.
- `workloads/`: workload algorithms and granularity control.
- `runtime/`: scheduler adapters and shared execution helpers.
- `metrics/`: scheduler statistics and optional PAPI counters.
- `probes/`: scheduling distribution, spin tracing, and timespan tuning.
- `tests/`: correctness tests, fixtures, and the isolated fake-PAPI harness.
- `tools/`: dataset acquisition, analysis, plotting, and CI helpers.
- `data/`: the original-dataset catalog (downloaded inputs stay outside the source tree).
- `docs/`: design, provenance, usage guides, and plan status.

`run.py` remains the main entry point. CMake target names and executable output
paths are unchanged; invoke helper scripts from `tools/`.

All native evaluation backends leave per-worker CPU pinning disabled. The runner
disables OpenMP-specific repinning as well, so every mode inherits the same CPU
set. Use `--cpu-node` on Linux to restrict that set uniformly; this is not a
promise of identical worker-to-core assignments. Direct executable invocations
must likewise avoid externally configured OpenMP affinity.

Result metadata records every measured executable's SHA-256 and size, and checks
that those files did not change during the run. `checkout` records the current
revision, dirty status, and tracked-diff hash. The legacy `oox_commit` is the
checkout revision only: it does not identify the binary's source revision.
`binary_source_revision` remains null because the runner cannot prove how an
existing build was produced. Keep the build and its source snapshot together.

See [the porting-plan status](docs/PLAN_STATUS.md) for the remaining gaps versus the
original full reproduction plan.
The current remaining scope is [original datasets and PAPI](docs/DATASETS_AND_PAPI.md).
Hardware measurement campaigns and legacy-runtime builds are not required.

`SERIAL_ELISION` runs the same algorithms with serial loop execution. It keeps
the requested thread count for workload-size formulas, while metadata and
execution traces report one execution thread. This is an algorithm-elision
control, not a replacement for independent serial implementations. Concurrent
caller experiments still create their explicitly requested caller threads.
The PBBS driver separately provides eight original implementations with
`--backend serial --mode SERIAL`; those always execute with one worker.

Use `tools/input_graphs.py --kind rmat24 --smoke --output <directory>` to build the
pinned PBBS generator and produce a validated small graph. Omit `--smoke` for
the full RMat24/RMat27 recipe; random-local and cube-grid recipes support
`--size small|large`. These are PBBS recipes, not assertions of byte identity
with historical PASL datasets. Metadata records source revision, generator
hash, parameters, actual graph dimensions, and graph SHA-256.

On Linux, `--likwid-group MEM --likwid-cpus 0-3` wraps the benchmark process in
`likwid-perfctr` and retains its CSV output. Use a group supported by the CPU.
LIKWID collects across the selected CPUs, including unrelated work on them;
reserve those CPUs for the experiment. Counts include process initialization
and the entire selected benchmark suite. They are not individual kernel
counters. LIKWID and `--perf` are mutually exclusive; LIKWID pinning cannot be
combined with `--cpu-node`, though explicit memory placement is supported.
An unavailable collector or missing output fails the run without marking it
complete. Optional PAPI callback instrumentation is available with
`-DOOX_SCHEDULER_EVAL_PAPI=ON` and `--papi-events`; useful-work utilization is
still unmeasured. See the scope and limitations in `docs/DATASETS_AND_PAPI.md`.
The wrapper follows the [official LIKWID interface](https://github.com/RRZE-HPC/likwid/blob/master/doc/likwid-perfctr.1).
See [historical inputs and baseline runners](docs/HISTORICAL_BASELINES.md) for pinned
PASL commands and revision-checked historical executable integration.

Native sample sort now includes string and 64-bit record cases (records use
lexicographic key/value ordering). `RadixPassWidth` compares 4-, 8- and 11-bit
digits while retaining full-width keys. `Pasl*` BFS cases implement wrap-around
grids, rejoining chains and cyclic phased topologies without vertex permutation.
File BFS accepts `--source-vertex`; all three policies and the oracle use it.
First-touch tests allocate fresh anonymous mappings per repetition, so vector
initialization or allocator reuse cannot pre-touch the input pages.

The suite also builds `OOX_TASKS`, which expresses parallel ranges as recursive
`oox::run` tasks joined through `oox::var`. With `OOX_SCHEDULER_EVAL_STATS=ON`,
it reads counters from OOX's actual pool. Its thread count is the library's build setting `OOX_EIGEN_THREADS`
(zero means detected hardware concurrency); configure that value to match the
runner's `--threads` for cross-mode comparisons.
Automatic OOX task grains target eight ranges per worker, capped at 1,024
iterations per leaf; explicit grain requests are retained. This grain policy
is distinct from the low-level Eigen policies and must be accounted for when
interpreting task counts.

Additional cases include frontier-parallel QuickHull, atomic string deduplication
on synthetic word triples, eight-pass 64-bit radix sort, concurrent callers, and
workers temporarily occupied by controlled tasks. Radix `preflight_pass_*_ns`
counters describe an untimed instrumented invocation; ordinary benchmark timing
does not include that instrumentation. QuickHull uses long-double orientation
arithmetic and reports partition depth, so its floating-point boundary behavior
can differ from PBBS's double-precision implementation.

Use `--graph /path/to/graph.adj_bin --filter BfsFile` to benchmark a supplied
PASL binary or PBBS `AdjacencyGraph` text file. Loading and serial validation
occur outside timing; the runner records the graph path and SHA-256. Add
`--paper-scale` to register 100-million-element primary cases explicitly.

This directory is OOX's offline, reproducible implementation of the evaluation
families studied in *Fast work distribution for composable task scheduling
engines*. It compares ordinary work stealing, proactive mailbox publication,
adaptive timespan splitting, their combined Eigen policy, oneTBB partitioners,
and available OpenMP schedules through one shared workload layer.

The historical thesis is cited at its original revision without a repository
checkout dependency. The runner never clones or downloads its code.
See `docs/PROVENANCE.md` for the clean-room implementation policy and
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
ctest --test-dir build-eval -L scheduler-eval --output-on-failure
```

CMake includes only installed/enabled backends. Eigen contributes
`EIGEN_STEALING`, `EIGEN_SHARING`, `EIGEN_STEALING_GRAINSIZE`, and
`EIGEN_SHARING_STEALING`, plus the oneTBB-derived `EIGEN_AUTO`,
`EIGEN_SIMPLE`, `EIGEN_STATIC`, and `EIGEN_AFFINITY` range policies.
See [the partitioner API and placement semantics](../../oox/eigen/PARTITIONERS.md).

With `OOX_SCHEDULER_TRACE_TOOLS=ON`, opt-in paired probes additionally build
`measure_scheduler_RAPID_NATIVE_{AUTO,SIMPLE,STATIC,AFFINITY}` and
`measure_scheduler_NATIVE_{AUTO,SIMPLE,STATIC,AFFINITY}_RESIDENT`. They reuse the
calibration suite (10 launch/balanced/tail/cluster/single-heavy cases), fixed
grain 2 and identical resident-capable pools. Only initial Rapid activation
differs within each pair. They are not part of the full `run.py` mode matrix.
For example, after configuring a stats/PAPI/sanitizer-free Release build:

```sh
cmake --build build-eval --target measure_scheduler_RAPID_NATIVE_AUTO measure_scheduler_NATIVE_AUTO_RESIDENT -j
BENCH_NUM_THREADS=8 OOX_RAPID_RESIDENT_LIMIT=8 ./build-eval/benchmarks/scheduler_eval/measure_scheduler_NATIVE_AUTO_RESIDENT --kernel arithmetic_v1 --min-ms 40
BENCH_NUM_THREADS=8 OOX_RAPID_RESIDENT_LIMIT=8 ./build-eval/benchmarks/scheduler_eval/measure_scheduler_RAPID_NATIVE_AUTO --kernel arithmetic_v1 --min-ms 40
```

Use repeated fresh processes, alternating the pair order; a single invocation
is only a smoke check. `RAPID_NATIVE_AUTO` means the explicit native Auto policy,
not the optimized lazy `RAPID_AUTO`, and neither means upstream Eigen's original
recursive loop. Do not combine measurements from different kernels.

TBB contributes simple, automatic, static, and affinity partitioners, plus the historical bitmask `RAPID_START` prototype. OpenMP
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
| Remove duplicates | Concurrent open-addressing over uniform, exponential, duplicate-heavy, almost-sorted, and reverse-sorted keys |
| Stable radix sort | Four parallel histogram/scatter passes over 32-bit scalar keys and key-value records |
| Sample sort | Deterministic sampling, parallel bucket distribution, and parallel bucket sorting, including reverse-sorted input |
| Flat/fixed/adaptive BFS | High-arity trees, parallel chains, dense/sparse phases, trunk-first, RMat, square/cube grids, and a small-world control |
| Variable-cost loops | Constant, uniform, exponential, Pareto, linear, clustered, periodic, shuffled, and phase-changing costs |
| Competing loops | Two simultaneous OOX loops contending for one worker pool |
| First touch | Serial versus parallel page initialization before parallel reads |
| Matrix multiply | Nested parallel regions with substantial inner work |
| Matrix transpose | Nested tiled regions with short inner tasks |
| Deep nested loops | Fixed 65,536 elements through 2/4/8/16 nested fork/join levels |

DeepNested fixes the element count independently of depth and worker count.
Its arguments are depth, fanout, and work per element: fanout 4 with one mixing
step at depths 2/4/8, and fanout 2 with 64 steps at depths 2/4/8/16. Outer levels
branch by fanout; the innermost loop spans all remaining elements. Every level
calls and joins the selected backend with grain 1. Compare depths within one
fanout/work profile to keep
useful work fixed. The existing nested validator checks exact visitation and
values against a flat serial oracle, including empty ranges, uneven splits,
large origins, and the benchmark tree shapes.

Inputs are deterministic and construction and validation stay outside measured
regions. A full SpMV run intentionally has the same large scale as the research
workload and can require several gigabytes; use `--smoke` before a full run.
Adaptive BFS uses SPTL's κ/α estimator rule with its 20 µs and 1.8 defaults;
see `docs/THIRDPARTY.md` for the retained MIT notice.

Eigen benchmark JSON includes scheduled/executed tasks, successful steals,
failed steal rounds, worker sleeps, and observed sleeping time. BFS additionally
records nested launches, sequentialized inner loops, and the learned sequential
complexity limit. Native primary workloads record hull vertices and merge
passes, deduplication probe rate and table load, radix passes, and sample-sort
bucket count, maximum bucket size, and imbalance. The counters are compiled
only for scheduler-evaluation targets. Workload descriptors are collected in
an untimed preflight invocation so probe accounting does not perturb the timed
kernel or its scheduler counters.

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

### Internal scheduler timeline (opt-in)

Enable `-DOOX_SCHEDULER_TRACE_TOOLS=ON` in a scheduler-eval build, then build
`trace_scheduler_RAPID_AUTO`, `trace_scheduler_RAPID_START`,
`trace_scheduler_EIGEN_AUTO`, and `trace_scheduler_EIGEN_STATIC`.
The original binary-mask `RAPID_START` target requires TBB. Additional targets
cover `RAPID_GROUP`, `EIGEN_AUTO_RESIDENT`, `EIGEN_SIMPLE`, and `EIGEN_AFFINITY`.
`cmake --build build-scheduler-trace --target scheduler_trace_all` builds all
available trace modes and the recorder test without building the benchmark suite.

```sh
python3 benchmarks/scheduler_eval/tracing/record.py --build build-scheduler-trace \
  --threads 4 --tasks 256 --iterations 3 --work 200 --shape tail \
  --modes RAPID_AUTO RAPID_START EIGEN_AUTO EIGEN_STATIC
```

Open the printed `comparison.html` directly in a browser; it is self-contained,
offline, and does not upload data. The runner creates a fresh result directory
with raw JSON, process logs, binary hashes, and the HTML. Existing output
directories are never overwritten. `viewer.html` can also load multiple trace
JSON files through its file chooser. `--callbacks` adds exact per-iteration
execution intervals, at substantial extra recording cost.
To package existing records with the current viewer, use
`python3 benchmarks/scheduler_eval/tracing/view.py trace1.json trace2.json --output compare.html`.
Mode checkboxes hide unwanted records and refit the shared scale, so a slow
outlier does not compress the modes you are currently comparing.

The timeline plots time on X and iteration indices on Y. A rectangle spans its
executed interval `[a, b)` vertically and its execution time horizontally; color
identifies a native thread (`T`, with its worker number `W` in the legend).
All modes share a time scale relative to the selected launch. Click a rectangle
or transfer for details; wheel zooms and dragging pans all modes together.
Stable task IDs remain in details. Thread colors are identities within a
recording, not a guarantee of matching CPU placement across modes.

New recordings include `chunk` scopes for actual locally executed blocks,
after any split/reservation has fixed their bounds. `range` still describes an
initial executor range and can contain a subsequently donated tail, so it is
not used as an execution rectangle when chunks or callbacks are available.
Fixed `RAPID_START`/`RAPID_GROUP` ranges are directly executed and can be plotted
as such. With callbacks enabled, individual executed iterations are shown.
Older records without chunks or callbacks show faint initial assignments and
are labeled accordingly. Instantaneous theft is a vertical marker covering
the transferred indices; its arrowhead has the receiving thread's color.
Queue and activation events resolve their iteration coordinates from the
associated task/range, never from worker or slot numbers stored in event args.
Use `view.py ... --output viewer.html --replace` to refresh a generated HTML
with the current viewer while preserving its input trace files.

Events distinguish activation, attempted queue placement, acquisition from an
actual deque/mailbox/deferred slot, successful cross-worker theft, inline
fallback, and parent/child splits. A queue delivery arrow connects an offer
only to an observed acquisition; an offer alone does not prove enqueue success.
Range theft is recorded on the thief with the observed victim worker and donor
range ID. It is not reported as a donor-side operation. The original bitmask
algorithm records mask activations and range execution, not invented steals.
Its `mask_task` IDs identify commands/ranges, not heap-allocated task objects.
Gaps mean no recorded execution, not proof of parking or OS descheduling.

This is **not a performance measurement mode**. Recording adds timestamps,
atomic operations, trace-only task fields, and can change allocation sizes and
scheduling decisions. Normal builds compile out hooks and trace-only fields.
The runner fixes the resident limit to the thread count and disables automatic
calibration for reproducible policy selection; it does not pin CPU affinity.
`balanced`, `hyperbolic`, and `tail` are synthetic spin workloads shared by all
recorders; the hyperbolic shape is not the SpMV benchmark.
`--grain` controls Eigen partitioners; the fixed-range `RAPID_START` and
`RAPID_GROUP` adapters ignore it, as in their normal benchmark implementation.

Buffers are bounded per native thread and allocated before recording. No file
I/O or event-buffer growth occurs on the recorded path. Increase
`--events-per-thread` if the viewer reports drops; incomplete traces never
silently invent missing connections. A recorder supports one session per
process. All translation units using the instrumented scheduler must agree on
`OOX_SCHEDULER_TRACE` and link `tracing/recorder.cpp`; use the provided isolated
targets rather than mixing traced and normal scheduler objects.

`python3 benchmarks/scheduler_eval/tracing/audit.py trace1.json trace2.json`
emits a machine-readable validation report. It checks scope nesting, every
iteration in every launch against a simple integer coverage oracle, callback
containment, and donation bounds/identity. Use `--callbacks` when recording to
enable all checks; absent observations are reported as unverified. Exit codes
are 0 (pass), 1 (failed check), and 2 (insufficient/unsupported evidence).

Rapid traces also record setup/capture/publication/join phase boundaries and
per-native-thread counters for range-slot locks, contention, rejected thefts,
and owner wait spins. Counters cover the entire recording, not individual
launches. They add recording overhead and establish observed work counts, not
the magnitude of those costs in an uninstrumented run. Completion of an outer
task envelope may trail the loop's return even though its callbacks and range
tree have finished; the audit distinguishes this cleanup from late work.
Stopping recording closes admission for new scopes and drains already accepted
scopes before disabling events. The drain is bounded at 250 ms; timeout,
pending/truncated scopes and rejected new scopes are explicit JSON metadata.
The recorder never fabricates end events to make an incomplete trace pass.

Tests (Python standard library; Node for the viewer model):

```sh
cmake --build build-scheduler-trace --target test_scheduler_trace_recorder
python3 benchmarks/scheduler_eval/tracing/test_trace.py build-scheduler-trace
node benchmarks/scheduler_eval/tracing/test_model.js
```

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
python3 benchmarks/scheduler_eval/tools/model.py results/scheduler_eval/<result>
```

The command adds model parameters, per-case predictions, initialization
amortization, and an observed-versus-predicted SpMV plot to that result. The
research lineage, publication-time estimator, parameter-selection procedure,
published foundations, limitations, and next measurements are in
[*Estimating Rapid Start and choosing scheduler parameters*](docs/PERFORMANCE_MODEL.md).

## Fast-group comparison

`RAPID_GROUP` runs the Eigen pool's resident range groups on the existing
workload suite, including nested and worker-availability checks. `RAPID_START`
retains upstream's historical TBB prototype. Compare with `EIGEN_AUTO` and
`EIGEN_STATIC`; resident mode is opt-in and consumes idle CPU.
`RAPID_AUTO` combines fast resident activation with the existing AutoPartitioner.
`EIGEN_AUTO_RESIDENT` is the ordinary Auto control on the same idle policy.
The Rapid+Auto adapter calibrates its resident limit at startup; set
`OOX_RAPID_RESIDENT_LIMIT=N` to use a fixed cohort instead.

Configure `OOX_SCHEDULER_EVAL_STATS=OFF` for timings without Eigen's diagnostic
counter updates (default ON). The runner records this setting. For direct
backend loops, use `bench_loops_EIGEN.EIGEN_AUTO_LOOP` and
`bench_loops_EIGEN.EIGEN_RAPID_GROUP_LOOP`, or
`bench_loops_EIGEN.EIGEN_RAPID_AUTO_LOOP` for the adaptive implementation.

Direct-loop CTest smoke cases use the `backend-loops` label and the registered
size 64. An unmatched benchmark filter is a test failure, not an empty success.
The scheduler-eval CI job with external backends disabled builds and runs all
three Eigen loop modes once, with two threads.
