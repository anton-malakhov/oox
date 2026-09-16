# Vendored PBBS snapshot

`vendor/` is benchmark-only source, imported byte-for-byte from
[`EgorkaZ/pbbsbench`](https://github.com/EgorkaZ/pbbsbench/tree/396a299f03c58dbe9e7604daab38a65781227b75)
at revision `396a299f03c58dbe9e7604daab38a65781227b75`.
It is not a submodule and needs neither Git history nor an upstream checkout
to compile or run. The historical origin remains attributed, not hidden.

The snapshot contains all 12 application families previously selected by the
OOX driver, eight original serial implementations, their checkers, input
generators, common utilities, and the required Parlay source. Unrelated
applications, nested repositories, compiled binaries, timing reports, notebooks,
and large dataset archives are excluded. `--all-benchmarks` means all vendored
implementations for the selected backend, not every application in upstream PBBS.

`vendor_manifest.json` records the upstream revision, application selection,
Git blob identities, SHA-256 hashes, and file/symlink modes. The importer follows
the selected applications' source symlinks; all retained links resolve inside
the snapshot. `tests_snapshot.py` verifies both SHA-256 and original Git blob
identity without consulting Git or the network.

## Licenses and ownership

The upstream PBBS license is retained at `vendor/LICENSE`; Parlay's MIT license
and attribution are retained at `vendor/parlaylib/LICENSE`. Individual source
headers, including embedded libdivsufsort notices and other component notices,
are preserved unchanged. OOX additions outside `vendor/` use this repository's
license. Dataset rights are separate from source-code licenses.

The frozen algorithms remain research benchmark code, not OOX's production
runtime. Vendoring transfers maintenance responsibility to us; it is not a
claim of upstream maintenance, production quality, or exact paper timings.

## Local adaptation policy

Never configure or build directly in `vendor/`. The driver verifies the manifest,
copies the source to a unique `pbbs-build-*/source` below its output directory,
and applies the existing OOX adapters and portability/checker fixes there.
Copies and build logs remain available after success or failure. Concurrent
runs do not edit the same source tree. Graph generation uses the same mechanism.

The adaptations remain visible in `run.py`: compiler/platform flags, scheduler
plugins, small-input octree setup, checker exit-status fixes, and bounded CI
repetitions. They are not silently folded into the upstream snapshot. Preserving
these pre-existing adaptations preserves the previous driver semantics.

Original archives are explicitly acquired into `results/pbbs-archives` by
`scheduler_eval/tools/datasets.py`, then hash-checked and copied into build trees.
Prepare all seven with `--bundled --archives-only`: upstream Makefile rules list
the whole archive group as prerequisites even when a test uses only one member.
Source compilation and benchmark execution do not clone repositories. Dataset
acquisition is a separate network operation; missing archives are not replaced
with generated stand-ins.

For a deliberate future re-import, use `import_snapshot.py <upstream-checkout>`
in a fresh location without an existing snapshot/manifest, review the entire
manifest diff, and run the snapshot tests plus the application checker suites.
The importer requires the exact pinned Git object only during maintenance.
