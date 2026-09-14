# SPDX-License-Identifier: Apache-2.0
import bz2
import hashlib
import io
import json
from pathlib import Path
import sys
import struct
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))
from datasets import acquire, copy_stream, graph_dimensions, prepare_archive



class DatasetTests(unittest.TestCase):
    def test_archive_download_is_bounded_verified_and_reusable(self):
        with tempfile.TemporaryDirectory() as directory:
            cache = Path(directory)
            payload = bz2.compress(b"original fixture")
            entry = dict(archive="fixture.bz2", sha256=hashlib.sha256(payload).hexdigest())
            with patch("datasets.urllib.request.urlopen", return_value=io.BytesIO(payload)) as download:
                archive = prepare_archive(entry, cache, 1, 1024)
                self.assertEqual(archive.read_bytes(), payload)
                self.assertEqual(prepare_archive(entry, cache, 1, 1024), archive)
                self.assertEqual(download.call_count, 1)
            archive.write_bytes(b"corrupt cached data")
            with self.assertRaises(ValueError):
                prepare_archive(entry, cache, 1, 1024)
            self.assertEqual(archive.read_bytes(), b"corrupt cached data")

    def test_failed_archive_download_is_not_published(self):
        with tempfile.TemporaryDirectory() as directory:
            cache = Path(directory)
            entry = dict(archive="fixture.bz2", sha256="0" * 64)
            for limit in (2, 1024):
                with patch("datasets.urllib.request.urlopen", return_value=io.BytesIO(b"wrong data")):
                    with self.assertRaises(ValueError):
                        prepare_archive(entry, cache, 1, limit)
                self.assertFalse((cache / "fixture.bz2").exists())

    def test_original_archive_integrity_and_reuse(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            archive_dir = root / "testData/data"
            archive_dir.mkdir(parents=True)
            output = root / "output"
            output.mkdir()
            payload = b"original corpus\x00bytes\n" * 100
            archive = archive_dir / "fixture.bz2"
            archive.write_bytes(bz2.compress(payload))
            entry = dict(file="fixture", archive="fixture.bz2", format="text",
                         sha256=hashlib.sha256(archive.read_bytes()).hexdigest())
            with patch("datasets.subprocess.check_output", side_effect=AssertionError("must not require Git")):
                record = acquire("fixture", entry, output, root, [], None, 1, 100000)
                self.assertEqual((output / "fixture").read_bytes(), payload)
                self.assertEqual(record["sha256"], hashlib.sha256(payload).hexdigest())
                self.assertEqual(acquire("fixture", entry, output, root, [], None, 1, 100000), record)
                (output / "fixture").write_bytes(b"corrupt")
                with self.assertRaises(ValueError):
                    acquire("fixture", entry, output, root, [], None, 1, 100000)
            self.assertEqual((output / "fixture").read_bytes(), b"corrupt")

    def test_mismatched_archive_never_publishes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "testData/data").mkdir(parents=True)
            (root / "testData/data/a.bz2").write_bytes(bz2.compress(b"wrong"))
            entry = dict(file="a", archive="a.bz2", format="text", sha256="0" * 64)
            with patch("datasets.subprocess.check_output", side_effect=AssertionError("must not require Git")):
                with self.assertRaises(ValueError):
                    acquire("a", entry, root, root, [], None, 1, 1024)
            self.assertFalse((root / "a").exists())
            self.assertFalse((root / "a.metadata.json").exists())

    def test_binary_format_rejects_html_and_truncation(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "g"
            for endian in ("<", ">"):
                path.write_bytes(struct.pack(endian + "5Q", 0xdeadbeef, 32, 2, 1, 0)
                                 + struct.pack(endian + "4I", 0, 1, 1, 1))
                self.assertEqual(graph_dimensions(path), dict(vertices=2, edges=1, bits=32))
            for content in (b"<html>gateway error</html>", path.read_bytes()[:-1]):
                path.write_bytes(content)
                with self.assertRaises(ValueError):
                    graph_dimensions(path)

    def test_download_bound(self):
        with self.assertRaises(ValueError):
            copy_stream(io.BytesIO(b"too large"), io.BytesIO(), 3)


if __name__ == "__main__":
    unittest.main()
