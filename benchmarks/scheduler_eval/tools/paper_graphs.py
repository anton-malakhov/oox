#!/usr/bin/env python3
"""Reproduce PASL generator commands without substituting approximate graphs."""

# Parameter formulas adapted from PASL graph/bench/graph.ml.
# Copyright (c) 2014 Umut Acar, Arthur Chargueraud, and Michael Rainey.
# SPDX-License-Identifier: Apache-2.0
# Modified for OOX: Python command planning, revision checks and provenance.

import argparse
import hashlib
import json
import math
from pathlib import Path
import shutil
import struct
import subprocess
import tempfile

PASL_REVISION = "d3ed9488cea5a8d35b9a86b4408e0f6f9211413b"
PASL_SC15_REVISION = "d2147d5986866d6060b6dee562fa65df432f90b7"
SC15_KINDS = ("trunk-first", "rmat24", "rmat27")
KINDS = ("square-grid", "cube-grid", "par-chains-100", "phases-10-d-2",
         "phases-50-d-5", "trees-524k", "rand-arity-100", *SC15_KINDS)
LOADS = {"small": 1000000, "medium": 10000000, "large": 100000000}


def parameters(kind, size):
    load = LOADS[size]
    bits = 64 if size == "large" else 32
    if kind == "trunk-first":
        result = dict(generator="unbalanced_tree", depth_of_trunk=2,
                      depth_of_branches=load // 10, trunk_first=1)
    elif kind in ("rmat24", "rmat27"):
        vertices = load * (2 if size == "large" else 10) // 15
        a, b = (0.5, 0.1) if kind == "rmat24" else (0.57, 0.19)
        bits = 64
        result = dict(generator="rmat", tgt_nb_vertices=vertices,
                      nb_edges=9 * vertices, rmat_seed=3234230, a=a, b=b, c=b)
    elif kind == "square-grid":
        side = math.isqrt(load // 2)
        result = dict(generator="grid_2d", width=side, height=side)
    elif kind == "cube-grid":
        result = dict(generator="cube_grid", nb_on_side=int((load // 3) ** (1 / 3)))
    elif kind == "par-chains-100":
        result = dict(generator="parallel_paths", nb_phases=1,
                      nb_paths_per_phase=100, nb_edges_per_path=load // 200)
    elif kind in ("phases-10-d-2", "phases-50-d-5", "trees-524k"):
        if kind == "phases-10-d-2":
            phases, width, high, low = 10, load // 30, 1, 2
        elif kind == "phases-50-d-5":
            phases, width, high, low = 50, load * (2 if size == "large" else 1) // 250, 0, 5
        else:
            phases, width, high, low = 2 * load // 524288, 524288, 1, 0
        result = dict(generator="phased", nb_phases=phases,
                      nb_vertices_per_phase=width,
                      nb_per_phase_at_max_arity=high,
                      arity_of_vertices_not_at_max_arity=low)
    elif kind == "rand-arity-100":
        bits = 32
        result = dict(generator="random", dim=10, degree=100, num_rows=load // 100)
    else:
        raise ValueError(f"unknown PASL preset: {kind}")
    return dict(result, bits=bits, seed=1, source=0)


def digest(path):
    value = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kind", choices=KINDS, required=True)
    parser.add_argument("--size", choices=LOADS, default="small")
    parser.add_argument("--pasl", type=Path, required=True)
    parser.add_argument("--output", type=Path, default=Path("results/pasl-graphs"))
    parser.add_argument("--threads", type=int, default=1)
    parser.add_argument("--timeout", type=int, default=1800)
    parser.add_argument("--plan", action="store_true")
    args = parser.parse_args()
    if args.threads < 1 or args.timeout < 1:
        parser.error("threads and timeout must be positive")
    source = args.pasl.resolve()
    executable = source / "graph/bench/graphfile.opt2"
    params = parameters(args.kind, args.size)
    command = [str(executable)]
    for name, value in dict(params, generator_proc=args.threads,
                            proc=args.threads, outfile="graph.adj_bin").items():
        command.extend([f"-{name}", str(value)])
    required_revision = PASL_SC15_REVISION if args.kind in SC15_KINDS else PASL_REVISION
    manifest = dict(pasl_revision=required_revision, kind=args.kind, size=args.size,
                    parameters=params, command=command, complete=False)
    if args.plan:
        print(json.dumps(manifest, indent=2))
        return
    actual = subprocess.check_output(
        ["git", "-C", str(source), "rev-parse", "HEAD"], text=True).strip()
    if actual != required_revision:
        raise RuntimeError(f"PASL revision {actual} differs from {required_revision}")
    if subprocess.check_output(["git", "-C", str(source), "diff", "HEAD"], text=True):
        raise RuntimeError("PASL has tracked changes; refusing unrecorded generator changes")
    if not executable.is_file():
        raise RuntimeError(f"build graphfile.opt2 in {source / 'graph/bench'} first")
    args.output.mkdir(parents=True, exist_ok=True)
    target = args.output / f"{args.kind}_{args.size}.adj_bin"
    record = target.with_suffix(".json")
    if target.exists() or record.exists():
        raise FileExistsError(f"refusing to replace {target} or its metadata")
    manifest["generator_sha256"] = digest(executable)
    with tempfile.TemporaryDirectory(prefix="pasl_graph_") as temporary:
        work = Path(temporary)
        result = subprocess.run(command, cwd=work, text=True,
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                timeout=args.timeout, check=False)
        if result.returncode:
            raise RuntimeError(f"PASL generator exited {result.returncode}:\n"
                               f"{result.stdout[-10000:]}")
        generated = work / "graph.adj_bin"
        with generated.open("rb") as stream:
            header = stream.read(40)
        if len(header) != 40:
            raise RuntimeError("generator produced a truncated header")
        endian = "<" if struct.unpack("<Q", header[:8])[0] == 0xdeadbeef else ">"
        magic, bits, vertices, edges, symmetric = struct.unpack(endian + "5Q", header)
        if magic != 0xdeadbeef or bits != params["bits"] or symmetric > 1:
            raise RuntimeError("generator produced an incompatible graph")
        if generated.stat().st_size != 40 + (vertices + 1 + edges) * (bits // 8):
            raise RuntimeError("generator output size disagrees with its header")
        manifest.update(vertices=vertices, edges=edges, graph_sha256=digest(generated),
                        generator_output=result.stdout, complete=True)
        with target.open("xb") as destination, generated.open("rb") as origin:
            shutil.copyfileobj(origin, destination)
    with record.open("x") as stream:
        json.dump(manifest, stream, indent=2)
        stream.write("\n")
    print(target.resolve())


if __name__ == "__main__":
    main()
