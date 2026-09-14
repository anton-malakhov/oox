# Original datasets and PAPI

Current scope is original dataset availability and PAPI integration. Running
hardware experiments or building historical runtimes is explicitly excluded.

## Original data

[`data/original_datasets.json`](../data/original_datasets.json) records original dataset identities, not substitutes:

- Seven original PBBS text/mesh archives have immutable source URLs and compressed
  SHA-256 values pinned in the catalogue. No PBBS checkout is needed.
  Acquisition caches and verifies each archive in `results/pbbs-archives`,
  decompresses it, and records the extracted file's SHA-256 and size.
- `w3c2` comes from the original corpus author's HTTPS server. Its published
  payload MD5 is checked for historical identity, and modern SHA-256 hashes are
  recorded for both the downloaded archive and extracted file.
- LiveJournal, Twitter, Wikipedia-20070206 and Europe use the exact IPFS CIDs
  published in the [original graph catalogue](http://deepsea.inria.fr/graph/).
  `--ipfs /path/to/ipfs` uses an initialized online node, with DAG validation
  performed by IPFS. HTTPS gateway fallback records the resolved CID and a local
  payload SHA-256, but does not claim local DAG verification.

```sh
python3 benchmarks/scheduler_eval/tools/datasets.py --bundled
python3 benchmarks/scheduler_eval/tools/datasets.py --bundled --archives-only
python3 benchmarks/scheduler_eval/tools/datasets.py --dataset w3c2
python3 benchmarks/scheduler_eval/tools/datasets.py --dataset livejournal --dataset twitter \
  --dataset wikipedia --dataset europe --ipfs /path/to/ipfs
```

Files default to `results/original-datasets`, outside version control. Every
successful file has adjacent identity/hash metadata. Repeating acquisition
verifies existing files rather than replacing them. Failures stay explicitly
incomplete in `acquisition-report.json`; generated, newer, or similarly named
graphs are never substituted. `--list` prints the complete source catalogue.
Graph headers and lengths are validated before publication; native graph loading
additionally validates offsets and vertex IDs. Downloads are byte-bounded and
existing unmatched files are preserved.

Use `run.py --dataset livejournal --dataset-directory <directory>` for an acquired
graph. It checks catalogue identity and the recorded payload hash before running
the benchmark. Dataset source vertices follow the pinned SC15 configuration:
Twitter uses **1** there, unlike **12** in the other PASL snapshot discussed in
`HISTORICAL_BASELINES.md`. `--source-vertex` remains an explicit override.

The source catalogue does not grant new data rights. The code's Apache/MIT
licenses do not automatically apply to graph, corpus or scan contents; original
source terms continue to apply. In particular, these are PBBS's converted scan
files, not newly downloaded or recomputed mesh substitutes.

## PAPI integration

Configure `-DOOX_SCHEDULER_EVAL_PAPI=ON` with PAPI headers and library installed.
The ordinary build keeps this option off and requires no PAPI dependency.

```sh
python3 benchmarks/scheduler_eval/run.py --build build-papi \
  --mode EIGEN_STEALING --threads 2 --filter VariableCost \
  --papi-events PAPI_TOT_CYC,PAPI_TOT_INS
```

This command is provided for later use; hardware measurements are not part of
the current task. PAPI, perf and LIKWID collection are mutually exclusive.
Requesting PAPI against a build without support fails before execution.

PAPI event sets belong to their execution threads. `EvalParallelFor` instruments
callback bodies while a benchmark metrics scope is active. Nested callbacks on
the same thread are counted once; callbacks on other workers contribute their
own counts. Totals appear as `papi_<event>` plus `papi_callback_regions` in Google
Benchmark JSON, and per-iteration counts are retained in the comparison export.

**Scope matters:** these are integer counts for callback regions, not complete
process or whole-kernel counters. They exclude worker scheduling outside callback
bodies and serial code outside those callbacks. They include callbacks invoked
during paused/setup portions of the metrics scope. Instrumented timings include
PAPI overhead and must not be mixed with uninstrumented performance results.
Use integer-valued events; unavailable events or start/stop failures mark the
benchmark as failed instead of publishing successful zero counts.

The lifecycle test uses a guarded, isolated fake API to verify thread ownership,
nested accounting, failures, and disabled operation. CI separately compiles
against real `libpapi-dev`; it does not enable hardware measurements. No fake
API is linked into benchmark targets.
