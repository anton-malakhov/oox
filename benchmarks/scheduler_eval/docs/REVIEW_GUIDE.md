# Scheduler benchmarks: review guide

This MR adds an opt-in scheduler-evaluation suite and PBBS application driver,
plus scheduler fixes needed by the benchmarks. It is not a claim of reproduced
paper results or production readiness for every experimental scheduler policy.

The selected original PBBS implementations are now vendored under
`benchmarks/pbbs/vendor/` with licenses and a checksum manifest, not replaced
with native approximations. Builds use isolated copies. The two research
submodules have been removed; dataset acquisition remains a separate operation.

## Suggested review order

1. Runtime changes in `oox/eigen/nonblocking_thread_pool.h` and
   `oox/backends/eigen/backend.h`: task ownership, completion, parking, and cleanup.
   Review their regression coverage in `tests/eigen_pool_test.cpp`.
2. `benchmarks/eigen/` and `scheduler_eval/runtime/`: loop adapters, partitioning,
   and thread-placement policy. `OOX_TASKS` measures the public OOX task API;
   `EIGEN_*` measures the lower-level scheduler adapter.
3. `scheduler_eval/workloads/` and `scheduler_eval/tests/`: algorithm correctness
   and independent reference checks before looking at timings.
4. `scheduler_eval/bench/`, `metrics/`, `probes/`, and `run.py`: timed regions,
   counter scope, experiment orchestration, and artifact identity.
5. `benchmarks/pbbs/run.py`, dataset tools, and CI: original application adapters,
   checker-backed smoke tests, optional dependencies, and backend coverage.

Paths beginning with `scheduler_eval/` above are relative to `benchmarks/`.

## Scope and limitations

- Native kernels cover primary, graph, nested, and synthetic workload families;
  the PBBS driver covers the original application families separately.
- Scheduler CI builds with TBB/OpenMP both disabled and enabled. The enabled
  configuration explicitly requires TBB, Rapid Start, and OpenMP targets.
- Native evaluation binaries do not pin individual workers. The runner disables
  OpenMP repinning and supports a shared Linux CPU-node restriction.
- Metadata identifies the actual executables by hash and records dirty-checkout
  state. It does not falsely equate an existing binary with checkout HEAD.
- PAPI is optional. CI checks real-library compilation and fake-API lifecycle
  accounting without collecting hardware measurements. Counter scope includes
  callback regions during paused/setup portions; it is not whole-kernel work.
- Four original graph downloads remain externally unavailable after the recorded
  retrieval attempts. No generated substitutes are presented as those originals.
- Hardware measurement campaigns and historical-runtime builds are out of scope.

See [plan status](PLAN_STATUS.md), [dataset/PAPI scope](DATASETS_AND_PAPI.md), and
[provenance](PROVENANCE.md) for the detailed coverage and qualifications.

## Owner review follow-up (2026-09-15)

- LLVM 19 provisioning: the hypothesized apt failure did not reproduce.
  [All ten checks for the vendoring commit passed](https://github.com/anton-malakhov/oox/actions/runs/34893889162),
  including the configuration that installs and uses libomp-19-dev. No compiler
  downgrade or additional package repository is needed for the observed CI run.
- Escaped task exceptions: `spawn -> Schedule -> ExecuteTask -> execute` formerly
  swallowed an exception before task completion could be published. With a
  non-throwing task policy, that could leave `wait_and_get` waiting indefinitely.
  The pool now terminates on an escaped exception: it cannot repair an arbitrary
  task's result state. Exception-enabled OOX tasks still catch inside `execute`,
  store the exception, and notify dependents. Regressions cover injected
  `std::bad_alloc` propagation and fail-fast behavior; this is not an actual
  memory-exhaustion campaign. CI also exercises the exception-enabled policy.
- PBBS timeouts: the driver and graph-generator commands run in fresh POSIX
  sessions. Timeout or interruption kills the complete process group, including
  make/compiler/benchmark descendants. A test checks a grandchild that ignores
  SIGTERM. Processes deliberately escaping into a different session are outside
  this group-based guarantee.
- Nesting: worker waits execute queued work through `Wait/TryExecuteOne`; they
  are not passive OS waits. Queue-order independence is not asserted. Adapter
  comments require rerunning nesting tests after scheduler changes, and the
  pool regression explicitly saturates both workers before launching children.
- Input handling: malformed, missing, overflowing, or out-of-range numeric
  options produce diagnostics and exit status 2. JSON escaping covers every
  control byte. Degenerate model inputs raise explicit validation errors instead
  of division-by-zero failures.
- Branch scripts: M1 runs use detached worktrees, never checkout/reset the main
  tree. A fake-Git/fake-runner test checks isolation without running benchmarks.
  Comparison generation failures now propagate instead of being ignored.
- Research-repository dependencies were removed in the preceding vendoring
  commit; original attributed benchmark sources remain isolated in-tree.
