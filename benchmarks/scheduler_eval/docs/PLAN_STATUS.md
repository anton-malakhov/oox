# Benchmark porting plan coverage

The remaining requested scope is original datasets and PAPI support.
Hardware measurement campaigns and historical-runtime builds are excluded by
the user's scope update. A green CI run is not a claim of paper reproduction.
See [datasets and PAPI](DATASETS_AND_PAPI.md) for interfaces and measurement scope.

| Plan item | Executable coverage | Current scope status |
| --- | --- | --- |
| Nested BFS | Flat, fixed-grain, and adaptive traversal; configurable source; pinned generators; original graph CID catalogue; identity/checksum-checked dataset selection | Original LiveJournal, Twitter, Wikipedia and Europe retrieval is externally blocked: gateways and direct IPFS attempts timed out on 2026-09-09 |
| QuickHull | PBBS canonical application plus native frontier-parallel QuickHull, with monotone-chain oracle and partition depth | Implemented; exact floating-point paper-number reproduction is excluded |
| Deduplication | Scalar and string hash tables; canonical PBBS trigram cases through application adapter | Implemented; synthetic native triples remain explicitly distinct from canonical PBBS input |
| Radix sort | Stable 32-bit and 64-bit scalar/pair kernels; 4/8/11-bit digit experiments; untimed per-pass samples; full-output oracle | Implemented; measurement campaigns excluded |
| Sample sort | Native scalar, string and 64-bit record sampling/bucketing; bounded recursive refinement; canonical PBBS generators via adapter | Implemented; measurement campaigns excluded |
| Secondary applications | All 12 PBBS application families and eight original serial implementations vendored in-tree with upstream checkers; original archive acquisition needs no code checkout | Source hashes and isolated builds validated; eight originals materialized and checksum-verified locally; hardware runs excluded |
| Synthetic experiments | Existing workload families plus optional per-thread PAPI callback instrumentation | PAPI lifecycle/accounting verified with an isolated fake API; real-library compilation covered by CI; no hardware measurements requested |
| Metrics | Pool task/steal/sleep counters, descriptors, hashes, PAPI callback counts, comparison export | Useful-work utilization remains null by design; measurement campaigns excluded |
| Historical comparison | Existing revision-checked reference invocation tools remain available | Historical-runtime builds and runs are out of scope |

`OOX_TASKS` evaluates `oox::run` and `oox::var` continuation joins in the native
suite. The `oox-tasks` PBBS adapter uses the same task graph primitives for
parallel loops and fork/join. The older `oox` PBBS backend and `EIGEN_*` native
modes measure the lower-level scheduler adapter and remain separately named.

Native large primary cases are opt-in with `--paper-scale` (100 million
elements). CI runs bounded cases and independent correctness oracles. No
published speedup, work-efficiency ratio, or full-plan completion is claimed.

PASL binary layout was checked against the original
[graphio.hpp](https://github.com/deepsea-inria/pasl/blob/master/graph/include/graphio.hpp):
five 64-bit header fields, followed by n+1 offsets and m neighbors of the stated
32/64-bit width. The loader independently validates counts, monotonic offsets,
vertex bounds, truncation, and trailing data; both byte orders are supported.
