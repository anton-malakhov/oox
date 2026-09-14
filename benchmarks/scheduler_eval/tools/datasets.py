#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Acquire exact catalogued originals; never substitute generated or newer data."""
import argparse
import bz2
import datetime
import hashlib
import json
import os
from pathlib import Path
import shutil
import struct
import subprocess
import tempfile
import urllib.request

from paper_graphs import digest

CATALOG = json.loads((Path(__file__).resolve().parents[1] / "data" / "original_datasets.json").read_text())


def graph_dimensions(path):
    with path.open("rb") as stream:
        header = stream.read(40)
    if len(header) != 40:
        raise ValueError("truncated PASL header")
    little = struct.unpack("<Q", header[:8])[0] == 0xdeadbeef
    magic, bits, vertices, edges, symmetric = struct.unpack(("<" if little else ">") + "5Q", header)
    if magic != 0xdeadbeef or bits not in (32, 64) or symmetric > 1:
        raise ValueError("not a supported PASL graph")
    if path.stat().st_size != 40 + (vertices + 1 + edges) * (bits // 8):
        raise ValueError("PASL file size differs from its header")
    return dict(vertices=vertices, edges=edges, bits=bits)


def copy_stream(origin, destination, limit):
    size = 0
    while block := origin.read(1024 * 1024):
        size += len(block)
        if size > limit:
            raise ValueError("dataset exceeds the configured byte limit")
        destination.write(block)
    return size


def prepare_archive(entry, archives, timeout, limit):
    archive = archives / entry["archive"]
    # Accept an explicitly supplied old checkout as a data-only cache.
    legacy = archives / "testData/data" / entry["archive"]
    if not archive.exists() and legacy.is_file():
        archive = legacy
    if not archive.exists():
        archives.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix="archive_", dir=archives) as directory:
            temporary = Path(directory) / "payload"
            url = CATALOG["archive_base_url"] + entry["archive"]
            with urllib.request.urlopen(url, timeout=timeout) as origin, temporary.open("wb") as destination:
                copy_stream(origin, destination, limit)
            if digest(temporary) != entry["sha256"]:
                raise ValueError("original archive checksum mismatch: " + url)
            os.link(temporary, archive)
    if digest(archive) != entry["sha256"]:
        raise ValueError(f"original archive checksum mismatch: {archive}")
    return archive


def acquire(name, entry, output, archives, gateways, ipfs, timeout, limit):
    target = output / entry["file"]
    record = output / (entry["file"] + ".metadata.json")
    if record.exists() and target.is_file():
        previous = json.loads(record.read_text())
        if previous.get("complete") and previous.get("identity") == entry and digest(target) == previous.get("sha256"):
            return previous
        raise ValueError(f"existing dataset failed identity/checksum verification: {target}")
    if target.exists() or record.exists():
        raise FileExistsError(f"refusing to replace unmatched dataset or metadata: {target}")
    if entry.get("unavailable"):
        raise RuntimeError(entry["unavailable"])
    metadata = dict(dataset=name, identity=entry, complete=False,
                    acquired_utc=datetime.datetime.now(datetime.timezone.utc).isoformat())
    with tempfile.TemporaryDirectory(prefix="dataset_", dir=output) as directory:
        temporary = Path(directory) / "payload"
        if "archive" in entry:
            archive = prepare_archive(entry, archives, timeout, limit)
            with bz2.open(archive, "rb") as origin, temporary.open("wb") as destination:
                copy_stream(origin, destination, limit)
            metadata.update(transport="checksum-verified PBBS data archive", pbbs_revision=CATALOG["pbbs_revision"],
                            archive_sha256=entry["sha256"])
        elif "url" in entry:
            archive = Path(directory) / "archive.bz2"
            with urllib.request.urlopen(entry["url"], timeout=timeout) as origin, archive.open("wb") as destination:
                copy_stream(origin, destination, limit)
            with bz2.open(archive, "rb") as origin, temporary.open("wb") as destination:
                copy_stream(origin, destination, limit)
            legacy_hash = hashlib.md5()
            with temporary.open("rb") as origin:
                for block in iter(lambda: origin.read(1024 * 1024), b""):
                    legacy_hash.update(block)
            if legacy_hash.hexdigest() != entry["payload_md5"]:
                raise ValueError("original corpus checksum mismatch")
            metadata.update(transport="original corpus HTTPS archive", url=entry["url"],
                            published_md5=entry["payload_md5"], archive_sha256=digest(archive))
        elif ipfs:
            subprocess.run([ipfs, "get", entry["cid"], "-o", str(temporary)],
                           check=True, timeout=timeout)
            if not temporary.is_file() or temporary.stat().st_size > limit:
                raise ValueError("IPFS output is not a bounded regular file")
            metadata.update(transport="IPFS DAG retrieval", cid=entry["cid"])
        else:
            failures = []
            for gateway in gateways:
                url = gateway.rstrip("/") + "/ipfs/" + entry["cid"]
                if not url.startswith("https://"):
                    raise ValueError("dataset gateways must use HTTPS")
                try:
                    request = urllib.request.Request(url, headers={"User-Agent": "OOX-datasets/1"})
                    with urllib.request.urlopen(request, timeout=timeout) as origin, temporary.open("wb") as destination:
                        length = origin.headers.get("Content-Length")
                        if length and int(length) > limit:
                            raise ValueError("dataset exceeds the configured byte limit")
                        copy_stream(origin, destination, limit)
                    graph_dimensions(temporary)
                    metadata.update(transport="HTTPS CID gateway", url=url, cid=entry["cid"],
                                    identity_verification="gateway-resolved CID; local payload SHA-256 recorded, not local DAG verification")
                    break
                except Exception as error:
                    failures.append(f"{url}: {error}")
            else:
                raise RuntimeError("original object unavailable: " + "; ".join(failures))
        if entry["format"] == "pasl-binary":
            metadata.update(graph_dimensions(temporary))
        metadata.update(sha256=digest(temporary), bytes=temporary.stat().st_size, complete=True)
        # Hard-link publication is exclusive and stays on the output filesystem.
        os.link(temporary, target)
        with record.open("x") as stream:
            json.dump(metadata, stream, indent=2)
            stream.write("\n")
    return metadata


def main():
    root = Path(__file__).resolve().parents[3]
    parser = argparse.ArgumentParser(description=__doc__)
    selection = parser.add_mutually_exclusive_group()
    selection.add_argument("--dataset", action="append", choices=CATALOG["datasets"])
    selection.add_argument("--bundled", action="store_true")
    selection.add_argument("--all", action="store_true")
    parser.add_argument("--list", action="store_true")
    parser.add_argument("--output", type=Path, default=root / "results/original-datasets")
    parser.add_argument("--archives", "--pbbs", dest="archives", type=Path,
                        default=root / "results/pbbs-archives")
    parser.add_argument("--archives-only", action="store_true",
                        help="prepare compressed originals for the vendored PBBS runner")
    parser.add_argument("--gateway", action="append")
    parser.add_argument("--ipfs", help="IPFS executable using an initialized online node")
    parser.add_argument("--timeout", type=int, default=60)
    parser.add_argument("--max-gib", type=float, default=64)
    args = parser.parse_args()
    if args.list:
        print(json.dumps(CATALOG, indent=2)); return
    if args.timeout <= 0 or not 0 < args.max_gib <= 512:
        parser.error("timeout and max-gib must be positive; max-gib is capped at 512")
    selected = list(CATALOG["datasets"]) if args.all else (
        [name for name, entry in CATALOG["datasets"].items() if "archive" in entry]
        if args.bundled else args.dataset or [])
    if not selected:
        parser.error("choose --bundled, --all, or --dataset")
    if args.archives_only and any("archive" not in CATALOG["datasets"][name] for name in selected):
        parser.error("--archives-only requires PBBS archive datasets")
    args.output.mkdir(parents=True, exist_ok=True)
    report = args.output / ("archive-acquisition-report.json" if args.archives_only else "acquisition-report.json")
    results = json.loads(report.read_text()) if report.exists() else {}
    for name in selected:
        print(f"Acquiring original {name}", flush=True)
        try:
            free = shutil.disk_usage(args.output).free
            if free < 1024 ** 3:
                raise RuntimeError("less than 1 GiB free; acquisition stopped")
            if args.archives_only:
                archive = prepare_archive(CATALOG["datasets"][name], args.archives, args.timeout,
                                          min(int(args.max_gib * 1024 ** 3), free - 1024 ** 3))
                results[name] = dict(complete=True, archive=str(archive), sha256=digest(archive))
                continue
            results[name] = acquire(name, CATALOG["datasets"][name], args.output, args.archives,
                                    args.gateway or ["https://ipfs.io", "https://dweb.link"],
                                    args.ipfs, args.timeout, min(int(args.max_gib * 1024 ** 3), free - 1024 ** 3))
        except Exception as error:
            results[name] = dict(complete=False, error=str(error))
            print(f"Unavailable: {name}: {error}", flush=True)
    report.write_text(json.dumps(results, indent=2) + "\n")
    print(report.resolve())
    if any(not results[name]["complete"] for name in selected):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
