#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Maintainer-only, offline import of the selected pinned PBBS source closure."""
import hashlib
import json
from pathlib import Path
import posixpath
import subprocess
import sys

from run import COMMIT, DEFAULT_BENCHMARKS, SERIAL_BENCHMARKS


def main():
    checkout = Path(sys.argv[1]).resolve()
    destination = Path(__file__).with_name("vendor")
    manifest_path = Path(__file__).with_name("vendor_manifest.json")
    if destination.exists() or manifest_path.exists():
        raise FileExistsError("refusing to replace an existing snapshot")
    def git(*args):
        return subprocess.check_output(["git", "-C", str(checkout), *args])
    tree = {}
    for entry in git("ls-tree", "-rz", COMMIT).split(b"\0"):
        if entry:
            metadata, name = entry.split(b"\t", 1)
            mode, kind, oid = metadata.decode().split()
            tree[name.decode()] = (mode, kind, oid)
    prefixes = {"LICENSE", "README.md", "runall", "common", "algorithm", "parlay",
                "parlaylib/LICENSE", "parlaylib/include",
                "testData/sequenceData", "testData/geometryData", "testData/graphData"}
    applications = sorted(set(DEFAULT_BENCHMARKS + SERIAL_BENCHMARKS))
    for application in applications:
        family = "benchmarks/" + application.split("/")[0]
        prefixes.update({"benchmarks/" + application, family + "/bench"})
        prefixes.update(p for p in tree if p.startswith(family + "/")
                        and "/" not in p[len(family) + 1:])
    selected = set()
    while prefixes:
        prefix = prefixes.pop()
        for _ in range(64):
            parts = prefix.split("/")
            ancestor = next(("/".join(parts[:i]) for i in range(1, len(parts))
                             if tree.get("/".join(parts[:i]), (None,))[0] == "120000"), None)
            if ancestor is None:
                break
            target = git("cat-file", "blob", tree[ancestor][2]).decode()
            prefix = posixpath.normpath(posixpath.join(posixpath.dirname(ancestor),
                                                       target, prefix[len(ancestor) + 1:]))
        else:
            raise ValueError("cyclic symlink dependency: " + prefix)
        paths = [p for p in tree if p == prefix or p.startswith(prefix + "/")]
        if not paths:
            raise ValueError("missing symlink dependency: " + prefix)
        for path in paths:
            mode, kind, oid = tree[path]
            if kind != "blob" or path in selected or "/maxFlowGens/" in path:
                continue
            selected.add(path)
            if mode == "120000":
                target = git("cat-file", "blob", oid).decode()
                resolved = posixpath.normpath(posixpath.join(posixpath.dirname(path), target))
                if target.startswith("/") or resolved.startswith("../"):
                    raise ValueError("symlink escapes snapshot: " + path)
                prefixes.add(resolved)
    files = {}
    for path in sorted(selected):
        mode, _, oid = tree[path]
        content = git("cat-file", "blob", oid)
        if b"\0" in content:
            raise ValueError("unexpected binary in source selection: " + path)
        target = destination / path
        target.parent.mkdir(parents=True, exist_ok=True)
        if mode == "120000":
            target.symlink_to(content.decode())
        else:
            target.write_bytes(content)
            target.chmod(0o755 if mode == "100755" else 0o644)
        files[path] = dict(mode=mode, git_blob=oid, sha256=hashlib.sha256(content).hexdigest())
    manifest = dict(schema=1, repository="https://github.com/EgorkaZ/pbbsbench",
                    revision=COMMIT, applications=applications, files=files)
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
    print(f"Imported {len(files)} source files/symlinks for {len(applications)} implementations")


if __name__ == "__main__":
    main()
