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
`EIGEN_SHARING_STEALING`, plus the pool-backed `RAPID_START`, `RAPID_MAILBOX`,
`RAPID_LAZY_STEALING`, and `RAPID_TIMESPAN_LAZY_STEALING` experiments. TBB
contributes simple, automatic, and affinity partitioners. OpenMP
contributes static, dynamic-nonmonotonic, and guided-nonmonotonic schedules.

`RAPID_START` uses the Eigen pool's lifetime worker registrations. Each
invocation creates an independent stack region and publishes a hierarchical
activation tree through per-worker inboxes, falling back to the ordinary queue
when a bounded rapid inbox fills. Nested calls inherit contiguous worker
domains, so nested matrix multiplication and transpose are enabled. The
implementation supports the pool's full 65535-worker limit rather than the
historical 64-bit mask.

`RAPID_MAILBOX` ends Rapid participation after bounded adaptive range blocks
have been placed in targeted ordinary mailboxes; the blocks then use
unrestricted work stealing. A mailbox block retains its logical proportional
domain for nested loops, avoiding whole-pool fan-out while remaining ordinary
stealable work. `RAPID_LAZY_STEALING` reserves one first block for every
proportional owner before publishing execution. An idle worker leaves its
Rapid domain once before claiming later blocks from peer ranges. Both modes run
one-worker effective domains directly. These modes trade some uniform-loop
launch cost for recovery from irregular static partitions without creating a
task per item.

`RAPID_TIMESPAN_LAZY_STEALING` retains the same protected first blocks and
one-way Rapid-domain exit, but each proportional owner times its own blocks and
smoothly adjusts toward 75 microseconds of useful work. Changes are bounded to
one quarter through eight times the previous block and leave at least four
later steal opportunities. Thieves use the owner's latest published block size
without feeding migration or contention time back into the estimate. Calls at
or below 512 iterations per effective worker use fixed lazy blocks because the
clock cost is larger than the available adaptation benefit.

The `test_scheduler_eval_*` executables validate scan, reduction, and all three
sparse distributions for every built policy. Reentrant policies additionally
validate nested multiplication and transpose.
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
| Matrix multiply | Nested parallel regions with substantial inner work |
| Matrix transpose | Nested tiled regions with short inner tasks |

Inputs are deterministic and construction and validation stay outside measured
regions. The current SpMV input is reused across Google Benchmark calibration
and repetition entries so setup does not repeatedly rebuild a multi-gigabyte
matrix. A full SpMV run intentionally has the same large scale as the research
workload and can still require several gigabytes; use `--smoke` before a full
run.

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
```

Use `--filter REGEX` for a workload family and `--benchmark-min-time 9s` for
long throughput-quality samples. The default is 0.5 seconds per benchmark
case. A result's `complete` metadata field becomes true only after all selected
commands and report generation succeed.

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

For an implementation overview and a direct comparison with the normal Eigen
backend, see [Rapid Start versus the normal Eigen backend](RAPID_START_VS_EIGEN.md).

## Fit the explanatory model

For a complete result containing Rapid Start, `Launch`, and all three SpMV
families, fit the policy-specific scheduling-event, useful-work, and
residual-load-imbalance terms, and report separately observed cold initialization
with:

```sh
python3 benchmarks/scheduler_eval/model.py results/scheduler_eval/<result>
```

The command adds fitted parameters, structural launch-event counts, per-case
predictions, deterministic size-holdout errors, policy-selection regret,
initialization amortization, and an observed-versus-predicted SpMV plot to that
result. The
research lineage, publication-time estimator, parameter-selection procedure,
published foundations, limitations, and next measurements are in
[*Estimating Rapid Start and choosing scheduler parameters*](PERFORMANCE_MODEL.md).
