# Historical graph inputs and baseline runners

`paper_graphs.py` translates the parameter formulas in PASL's pinned
`graph/bench/graph.ml` into commands for its original `graphfile.opt2`.
It preserves the upstream generator's random vertex permutation and seed.
Native `Pasl*` graph cases reproduce the underlying topology without that
permutation; their vertex numbering and traversal order are intentionally
distinct from original generated files.

Inspect a command without a checkout or large allocation:

```sh
python3 benchmarks/scheduler_eval/tools/paper_graphs.py --kind trees-524k \
  --size large --pasl /path/to/pasl --plan
```

The supported presets are square/cube grids, 100 parallel chains, both phased
families, trees-524k, and rand-arity-100. Small/medium/large use the original
1M/10M/100M load budgets, not vertex counts. Trunk-first and rmat24/rmat27 use
the original `new-sc15-graph` branch at `d2147d5986866d6060b6dee562fa65df432f90b7`.
For those three kinds the tool requires a checkout at that revision instead.
Its RMat presets derive vertex counts from the load budget: the large preset
requests 13,333,333 vertices and 119,999,997 edges for both labels, with different
quadrant probabilities. The names must not be interpreted as literal 2^24 and
2^27 sizes; `tools/input_graphs.py` provides separately labeled PBBS recipes for those
literal sizes. The exact artifact dataset still needs provenance verification.
Real data remains externally
supplied; this tool does not download it or assert dataset licensing.

To generate a file, build `graphfile.opt2` in the pinned PASL checkout and omit
`--plan`. The tool checks the checkout revision and tracked modifications,
records the executable SHA-256, validates the output header/size, and writes
graph SHA-256 and full command metadata beside the output. Existing outputs
are refused. Generation uses a temporary working directory because PASL's
filename parser expects a simple relative filename.

Use the generated graph with native policies:

```sh
python3 benchmarks/scheduler_eval/run.py --build build-eval --threads 2 \
  --mode EIGEN_STEALING --graph /path/to/graph.adj_bin \
  --source-vertex 0 --filter BfsFile
```

The original PASL harness selects source 12 for Twitter, 123 for Friendster,
and 0 for LiveJournal/Wikipedia. Select the source matching the actual artifact
and retain it in result metadata. Graph formats of other historical tools can
differ; the baseline runner does not silently convert their inputs.

## Running built historical baselines

`baselines.py` verifies the primary checkout revision, records the executable
hash, runs an argument vector without a shell, and retains stdout and numeric
metrics. Repeated `exectime` fields are preserved as separate observations.
Missing timing, non-finite metrics, process failures and timeouts never produce
a completed result.

```sh
python3 benchmarks/scheduler_eval/tools/baselines.py --baseline heartbeat \
  --checkout /path/to/heartbeat --executable /path/to/heartbeat/bench/merge.log \
  --output results/heartbeat-merge -- -algorithm heartbeat -n 10000000 -proc 2

python3 benchmarks/scheduler_eval/tools/baselines.py --baseline pbbs-sptl \
  --checkout /path/to/pbbs-sptl --dependency sptl=/path/to/sptl \
  --executable /path/to/pbbs-sptl/bench/bfs.sptl --output results/sptl-bfs \
  -- -library sptl -check 1 -infile /path/to/sptl-graph -source 0 -proc 2
```

Add `--plan` before `--` to inspect either invocation without running it.
Pins are embedded in the tools: PASL `d3ed948`, Heartbeat `1b2ebc6`, pbbs-sptl
`87c51ef`, and SPTL `911bc7a`. They identify inspected upstream snapshots, not a
claim that every snapshot exactly matches the published artifact release.
Dependencies such as cmdline, chunkedseq, pbbs-include and the runtime/compiler
must still be supplied and versioned by the baseline environment.

These wrappers do not install a legacy toolchain. The inspected Heartbeat
Makefile explicitly targets Linux/x86-64; Cilk variants use `-fcilkplus` and
`libcilkrts`. SPTL's Cilk build also needs its original dependencies. Native
macOS OOX validation and wrapper fixture tests therefore do not constitute a
successful build or run of those historical environments.

PASL's inspected license is Apache-2.0, not the blanket MIT attribution in
the original planning note. Adapted formulas retain its copyright notice;
the repository's Apache-2.0 license applies. Source references:

- [PASL parameters](https://github.com/deepsea-inria/pasl/blob/d3ed9488cea5a8d35b9a86b4408e0f6f9211413b/graph/bench/graph.ml)
- [PASL topology generators](https://github.com/deepsea-inria/pasl/blob/d3ed9488cea5a8d35b9a86b4408e0f6f9211413b/graph/include/graphgenerators.hpp)
- [SC15 trunk-first and RMat parameters](https://github.com/deepsea-inria/pasl/blob/d2147d5986866d6060b6dee562fa65df432f90b7/graph/bench/graph.ml)
- [Heartbeat targets](https://github.com/deepsea-inria/heartbeat/blob/1b2ebc695266b406e26d1565410ddad3da15c935/bench/Makefile)
- [SPTL BFS invocation](https://github.com/deepsea-inria/pbbs-sptl/blob/87c51ef24a458d127072fa056f5e19368d9d729f/bench/bfs.cpp)
