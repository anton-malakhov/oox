# OOX Twist Tests

This subtree contains deterministic and randomized concurrency tests for OOX's
dependency/lifetime state machine. These tests must compile OOX with
`HAVE_TWIST=1`; every executable has a compile-time guard for `OOX_USING_TWIST`.

Typical quick run:

```bash
cmake --preset twist-sim-dfs
cmake --build --preset twist-sim-dfs
ctest --test-dir build/twist-sim-dfs -L twist --output-on-failure
```

Without presets:

```bash
cmake -S . -B build/twist-sim-dfs \
  -DOOX_BUILD_TESTS=ON \
  -DOOX_BUILD_TWIST_TESTS=ON \
  -DOOX_BUILD_BENCHMARKS=OFF \
  -DOOX_ENABLE_TBB=OFF \
  -DOOX_ENABLE_TF=OFF \
  -DOOX_ENABLE_FOLLY=OFF \
  -DOOX_ENABLE_OMP=OFF \
  -DTWIST_SIM=ON \
  -DTWIST_SIM_ISOLATION=ON \
  -DTWIST_FAULTY=ON
cmake --build build/twist-sim-dfs
ctest --test-dir build/twist-sim-dfs -L twist --output-on-failure
```

The first tests run under randomized simulation and cover:

- `SimpleChain`;
- `DeferredDiamond`;
- `ConsumerAddedWhileProducerCompletes`;
- `DependencyPublicationTree`;
- forwarding fan-out and nested forwarding;
- reader/writer ordering around repeated writes.

TSAN publication check:

```bash
cmake --preset twist-fault-tsan
cmake --build --preset twist-fault-tsan
ctest --preset twist-fault-tsan
```

Add new tests as small scenario functions with strong oracles. Use
`RunRandomSeeds` for regular PR coverage, and add `RunDfs` only for scenarios
that have been measured to keep the explored state space tiny.
