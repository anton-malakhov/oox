#!/usr/bin/env python3
"""Run revision-checked historical baselines and retain their reported metrics."""
# SPDX-License-Identifier: Apache-2.0

import argparse
import json
import math
import platform
from pathlib import Path
import subprocess

from paper_graphs import digest

REVISIONS = {
    "pasl": "d3ed9488cea5a8d35b9a86b4408e0f6f9211413b",
    "heartbeat": "1b2ebc695266b406e26d1565410ddad3da15c935",
    "pbbs-sptl": "87c51ef24a458d127072fa056f5e19368d9d729f",
    "sptl": "911bc7af7c658020138a08d4923224332b08a27f",
}
FIELDS = {"exectime", "nb_promotions", "nb_steals", "utilization",
          "nb_stacklet_allocations", "nb_stacklet_deallocations", "launch_duration"}


def parse_metrics(output):
    metrics = {}
    for line in output.splitlines():
        columns = line.split()
        if len(columns) != 2 or columns[0] not in FIELDS:
            continue
        value = float(columns[1])
        if not math.isfinite(value) or value < 0:
            raise ValueError(f"invalid baseline metric: {line}")
        metrics.setdefault(columns[0], []).append(value)
    if not metrics.get("exectime"):
        raise ValueError("baseline did not report exectime")
    return metrics


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", choices=("pasl", "heartbeat", "pbbs-sptl"), required=True)
    parser.add_argument("--checkout", type=Path, required=True)
    parser.add_argument("--executable", type=Path, required=True)
    parser.add_argument("--dependency", action="append", default=[], metavar="NAME=PATH")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--timeout", type=int, default=600)
    parser.add_argument("--plan", action="store_true")
    parser.add_argument("arguments", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if args.timeout <= 0:
        parser.error("timeout must be positive")
    arguments = args.arguments[1:] if args.arguments[:1] == ["--"] else args.arguments
    executable = args.executable.resolve()
    checkouts = {args.baseline: args.checkout.resolve()}
    for entry in args.dependency:
        name, path = entry.split("=", 1)
        if name not in REVISIONS or name in checkouts:
            parser.error(f"unknown or duplicate dependency: {name}")
        checkouts[name] = Path(path).resolve()
    if args.baseline == "pbbs-sptl" and "sptl" not in checkouts:
        parser.error("pbbs-sptl requires --dependency sptl=PATH")
    manifest = dict(baseline=args.baseline, complete=False,
                    command=[str(executable), *arguments],
                    checkouts={name: dict(path=str(path), revision=REVISIONS[name])
                               for name, path in checkouts.items()},
                    platform=platform.platform(), timeout_seconds=args.timeout)
    if args.plan:
        print(json.dumps(manifest, indent=2))
        return
    executable.relative_to(args.checkout.resolve())
    for name, path in checkouts.items():
        revision = subprocess.check_output(
            ["git", "-C", str(path), "rev-parse", "HEAD"], text=True).strip()
        if revision != REVISIONS[name]:
            raise RuntimeError(f"{name} is at {revision}, expected {REVISIONS[name]}")
        if subprocess.check_output(["git", "-C", str(path), "diff", "HEAD"], text=True):
            raise RuntimeError(f"{name} has tracked changes")
    manifest["executable_sha256"] = digest(executable)
    args.output.mkdir(parents=True, exist_ok=False)
    metadata = args.output / "metadata.json"
    metadata.write_text(json.dumps(manifest, indent=2) + "\n")
    with (args.output / "stdout.txt").open("w") as stream:
        subprocess.run(manifest["command"], cwd=executable.parent,
                       stdout=stream, stderr=subprocess.STDOUT,
                       timeout=args.timeout, check=True)
    metrics = parse_metrics((args.output / "stdout.txt").read_text())
    (args.output / "metrics.json").write_text(json.dumps(metrics, indent=2) + "\n")
    manifest["complete"] = True
    metadata.write_text(json.dumps(manifest, indent=2) + "\n")
    print(args.output.resolve())


if __name__ == "__main__":
    main()
