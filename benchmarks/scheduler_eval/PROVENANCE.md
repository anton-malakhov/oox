# Scheduler evaluation provenance

## Historical reference

The research implementation is tracked at
`thirdparty/composable-parallel-scheduler-thesis`.

- Repository: `https://github.com/lejabque/composable-parallel-scheduler-thesis`
- Pinned commit: `f58844b4fb6f0a1394968771c1a932c8cf2c75d4`
- Submodule path: `thirdparty/composable-parallel-scheduler-thesis`

No explicit license file is present at the pinned revision. Its files are
therefore reference material, not vendored OOX source. The files in this
directory are independently implemented under Apache-2.0 and use OOX's public
backend abstraction. No runner clones the reference at execution time.

## Implemented coverage

| Historical component | Native OOX implementation | Validation |
| --- | --- | --- |
| `bench_reduce.cpp` | Blocked reduction with tail handling | Exact scalar result |
| `bench_scan.cpp` | Work-efficient exclusive tree scan | Exact prefix values |
| Three `bench_spmv_*` files | Deterministic balanced, harmonic/hyperbolic, and triangular CSR generators | Each compared with a serial CSR oracle |
| `bench_mmul.cpp` | Nested dense matrix multiplication | Compared elementwise with serial multiplication |
| `bench_mtranspose.cpp` | Nested blocked transpose | Exact element mapping |
| `bench_spin.cpp` | Relax, atomic, isolated distributed-read, and thread-local-read payload matrix | CMake smoke run per backend mode |
| `scheduling_dist` | Spin, barrier, and multitask arrival/worker distributions | JSON smoke run per backend mode |
| `timespan_tuner` | Warm baseline p99 publication-timespan estimator | Baseline JSON smoke test |
| `trace_spin` | Chrome task-span trace across repeated publications | JSON smoke run per backend mode |
| Bitmask `RapidStart` | Benchmark-only adapter with bounded worker registration and warm publication | Flat correctness, distribution, trace, and runner smoke tests |
| Original runner/plotting intent | Offline metadata capture, schema checks, CSV, Markdown, and SVG reports | End-to-end CTest runner smoke |

## Intentional differences

1. OOX uses deterministic inputs so backend comparisons see identical data.
   The historical implementation creates random values and positions.
2. The sparse generators preserve the intended average density and balanced,
   harmonic, or triangular work shape without copying their implementation.
3. One parameterized Google Benchmark binary replaces separate latency and
   nine-second throughput binaries. Run duration and repetitions are controlled
   by the runner/Google Benchmark arguments and are recorded in JSON.
4. The spin benchmark excludes internal tracing overhead and isolates
   distributed values by the platform interference size. Non-relax payload
   counts retain the historical 32x instruction scaling.
5. `initialization_ns` is explicit. It includes worker creation and setup;
   arrival timestamps start afterward and therefore represent warm task
   publication separately.
6. `timespan_tuner` is built only for `EIGEN_STEALING`. Blocking one first task
   per worker is incompatible with a policy that decides whether to split only
   after observing that first task's elapsed time.
7. The pure adaptive-grainsize mode skips the same blocking distribution case.
   This prevents a methodological deadlock rather than masking it with a
   timeout; spin and multitask results still cover that policy.
8. Trace JSON contains only events observable around the public `ParallelFor`
   callback. Internal enqueue, mailbox publication, and steal-source events
   require a separate instrumentation API and are not inferred.
9. The report generator is Python-standard-library-only and emits SVG rather
   than depending on a plotting installation. Raw Google Benchmark and trace
   JSON remain available for external analysis.
10. The Rapid Start prototype is limited to 64 workers and is not reentrant.
    Nested matrix workloads are omitted for that mode, and aggregate scores use
    only benchmark cases shared by every selected mode.

## Evaluation rules

1. Keep both research submodules optional for normal OOX builds and pinned by
   gitlink. Benchmark execution must remain network-free.
2. Construct inputs and validate outputs outside timed regions.
3. Compare modes from one build/result directory with identical thread count,
   affinity, allocator, compiler, power policy, and machine state.
4. Treat smoke results only as pipeline validation, never as evidence.
5. Preserve `metadata.json`, raw JSON, and the exact OOX revision with every
   paper figure or table.
6. Record future semantic or measurement changes in this file before comparing
   them with earlier data.
