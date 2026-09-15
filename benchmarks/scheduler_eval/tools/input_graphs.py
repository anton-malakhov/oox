#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Build pinned PBBS generators and produce validated BFS input files."""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile

from paper_graphs import digest

KINDS = ("rmat24", "rmat27", "random-local", "cube-grid")


def recipe(kind, size="small", smoke=False, seed=1):
    n = (1 << int(kind[4:])) if kind.startswith("rmat") else (
        2000000 if size == "small" else 20000000)
    if smoke:
        n = 1024
    if kind.startswith("rmat"):
        return "rMatGraph", ["-j", "-a", "0.55", "-b", "0.125", "-c", "0.125",
                            "-m", str(12 * n), "-s", str(seed), str(n)]
    if kind == "random-local":
        return "randLocalGraph", ["-j", "-d", "3", "-m", str(10 * n), str(n)]
    if kind == "cube-grid":
        return "gridGraph", ["-j", "-d", "3", str(n)]
    raise ValueError(f"unknown graph recipe: {kind}")


def validate_graph(path):
    with path.open() as stream:
        tokens = (value for line in stream for value in line.split())
        try:
            if next(tokens) != "AdjacencyGraph":
                raise ValueError("expected PBBS AdjacencyGraph output")
            vertices, edges = int(next(tokens)), int(next(tokens))
            if vertices <= 0 or edges < 0 or vertices > 0xffffffff:
                raise ValueError("unsupported graph dimensions")
            previous = 0
            for vertex in range(vertices):
                offset = int(next(tokens))
                if offset < previous or offset > edges or (vertex == 0 and offset):
                    raise ValueError("invalid adjacency offsets")
                previous = offset
            for _ in range(edges):
                neighbor = int(next(tokens))
                if neighbor < 0 or neighbor >= vertices:
                    raise ValueError("invalid vertex ID")
            if next(tokens, None) is not None:
                raise ValueError("trailing graph data")
        except StopIteration as error:
            raise ValueError("truncated graph output") from error
    return vertices, edges


def main():
    root = Path(__file__).resolve().parents[3]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kind", choices=KINDS, required=True)
    parser.add_argument("--size", choices=("small", "large"), default="small")
    parser.add_argument("--smoke", action="store_true")
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--threads", type=int, default=2)
    parser.add_argument("--source", type=Path, default=root / "benchmarks/pbbs/vendor")
    parser.add_argument("--compiler", default=shutil.which("clang++") or "c++")
    parser.add_argument("--output", type=Path, default=root / "results/pbbs-graphs")
    parser.add_argument("--timeout", type=int, default=900)
    parser.add_argument("--plan", action="store_true")
    args = parser.parse_args()
    if args.threads < 1 or args.timeout < 1 or not 0 <= args.seed <= 0xffffffff:
        parser.error("invalid threads, timeout or seed")
    if args.kind not in ("rmat24", "rmat27") and args.seed != 1:
        parser.error("only RMat's pinned generator accepts an explicit seed")
    program, arguments = recipe(args.kind, args.size, args.smoke, args.seed)
    profile = "smoke" if args.smoke else "full" if args.kind.startswith("rmat") else args.size
    manifest = dict(kind=args.kind, profile=profile, seed=args.seed,
                    generator=program, arguments=arguments, complete=False,
                    note="PBBS recipe; not asserted byte-identical to the PASL paper artifact")
    if args.plan:
        print(json.dumps(manifest, indent=2))
        return
    spec = importlib.util.spec_from_file_location("oox_pbbs_driver", root / "benchmarks/pbbs/run.py")
    driver = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(driver)
    source = args.source.resolve()
    driver.validate_source(source)
    args.output.mkdir(parents=True, exist_ok=True)
    target = args.output / f"{args.kind}_{profile}_seed{args.seed}.adj"
    record = target.with_suffix(".json")
    if target.exists() or record.exists():
        raise FileExistsError(f"refusing to replace {target} or its metadata")
    source = driver.snapshot.build_copy(source, args.output)
    generator_dir = source / "testData/graphData"
    env = dict(os.environ, EIGEN="1", EIGEN_MODE="EIGEN_STEALING",
               BENCH_NUM_THREADS=str(args.threads), PARLAY_NUM_THREADS=str(args.threads))
    try:
        driver.configure_checkout(source, root, args.compiler)
        driver.select_backend(source, "oox")
        build = ["make", "-B", "-j", str(args.threads), program]
        with target.with_suffix(".build.log").open("w") as log:
            driver.processes.run(build, cwd=generator_dir, env=env, stdout=log,
                           stderr=subprocess.STDOUT, check=True, timeout=args.timeout)
        executable = generator_dir / program
        manifest.update(pbbs_revision=driver.COMMIT, generator_sha256=digest(executable),
                        compiler=args.compiler, threads=args.threads,
                        generator_backend="oox", generator_mode="EIGEN_STEALING")
        with tempfile.TemporaryDirectory(prefix="pbbs_graph_") as temporary:
            generated = Path(temporary) / "graph.adj"
            command = [str(executable), *arguments, str(generated)]
            with target.with_suffix(".run.log").open("w") as log:
                driver.processes.run(command, env=env, stdout=log, stderr=subprocess.STDOUT,
                               check=True, timeout=args.timeout)
            vertices, edges = validate_graph(generated)
            manifest.update(vertices=vertices, edges=edges, graph_sha256=digest(generated),
                            complete=True)
            with target.open("xb") as destination, generated.open("rb") as origin:
                shutil.copyfileobj(origin, destination)
        with record.open("x") as stream:
            json.dump(manifest, stream, indent=2)
            stream.write("\n")
        print(target.resolve())
    finally:
        driver.validate_source(args.source)


if __name__ == "__main__":
    main()
