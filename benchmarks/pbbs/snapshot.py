# SPDX-License-Identifier: Apache-2.0
"""Integrity and isolated build copies for the in-tree PBBS source snapshot."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import tempfile

VENDOR = Path(__file__).with_name("vendor")
MANIFEST_PATH = Path(__file__).with_name("vendor_manifest.json")


def manifest():
    return json.loads(MANIFEST_PATH.read_text())


def digest(path):
    result = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            result.update(block)
    return result.hexdigest()


def validate(source=VENDOR):
    source = source.resolve()
    files = manifest()["files"]
    actual = {p.relative_to(source).as_posix() for p in source.rglob("*")
              if p.is_symlink() or p.is_file()}
    if actual != set(files):
        raise ValueError(f"PBBS snapshot file set differs: missing={set(files) - actual}, extra={actual - set(files)}")
    for name, entry in files.items():
        path = source / name
        if entry["mode"] == "120000":
            if not path.is_symlink() or not path.resolve().is_relative_to(source) or not path.exists():
                raise ValueError("invalid PBBS source symlink: " + name)
            sha = hashlib.sha256(os.readlink(path).encode()).hexdigest()
        else:
            if path.is_symlink() or bool(path.stat().st_mode & 0o111) != (entry["mode"] == "100755"):
                raise ValueError("PBBS source type/mode mismatch: " + name)
            sha = digest(path)
        if sha != entry["sha256"]:
            raise ValueError("PBBS source checksum mismatch: " + name)


def build_copy(source, output, archives=None, require_archives=False):
    validate(source)
    output = output.resolve()
    if output.is_relative_to(source.resolve()):
        raise ValueError("build output must be outside the source snapshot")
    catalog = json.loads((Path(__file__).parents[1] / "scheduler_eval/data/original_datasets.json").read_text())
    if require_archives:
        missing = [entry["archive"] for entry in catalog["datasets"].values()
                   if "archive" in entry and (archives is None or not (archives / entry["archive"]).is_file())]
        if missing:
            raise FileNotFoundError("Missing original archives: " + ", ".join(missing) +
                                    "; run benchmarks/scheduler_eval/tools/datasets.py --bundled --archives-only")
    output.mkdir(parents=True, exist_ok=True)
    parent = Path(tempfile.mkdtemp(prefix="pbbs-build-", dir=output))
    build = parent / "source"
    shutil.copytree(source, build, symlinks=True)
    data = build / "testData/data"
    data.mkdir(exist_ok=True)
    for entry in catalog["datasets"].values():
        if archives is None or "archive" not in entry:
            continue
        archive = archives / entry["archive"]
        if archive.exists():
            if digest(archive) != entry["sha256"]:
                raise ValueError("original archive checksum mismatch: " + str(archive))
            shutil.copyfile(archive, data / archive.name)
    (parent / "source-manifest.json").write_bytes(MANIFEST_PATH.read_bytes())
    return build
