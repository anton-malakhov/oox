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
