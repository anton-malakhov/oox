# SPDX-License-Identifier: Apache-2.0
import hashlib
import os
from pathlib import Path
import shutil
import tempfile
import unittest

import snapshot
from run import DEFAULT_BENCHMARKS, SERIAL_BENCHMARKS, configure_checkout


class SnapshotTests(unittest.TestCase):
    def test_smoke_requires_complete_archive_cache_before_building(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with self.assertRaisesRegex(FileNotFoundError, "--bundled --archives-only"):
                snapshot.build_copy(snapshot.VENDOR, root / "build", root / "cache",
                                    require_archives=True)
            self.assertFalse((root / "build").exists())

    def test_manifest_and_original_git_blob_identity(self):
        snapshot.validate()
        manifest = snapshot.manifest()
        self.assertEqual(set(manifest["applications"]),
                         set(DEFAULT_BENCHMARKS + SERIAL_BENCHMARKS))
        for name, entry in manifest["files"].items():
            with self.subTest(path=name):
                path = snapshot.VENDOR / name
                data = os.readlink(path).encode() if path.is_symlink() else path.read_bytes()
                header = f"blob {len(data)}\0".encode()
                self.assertEqual(hashlib.sha1(header + data).hexdigest(), entry["git_blob"])
        for app in manifest["applications"]:
            self.assertTrue((snapshot.VENDOR / "benchmarks" / app / "Makefile").is_file())

    def test_build_copy_is_independent_and_needs_no_git(self):
        with tempfile.TemporaryDirectory() as directory:
            build = snapshot.build_copy(snapshot.VENDOR, Path(directory))
            self.assertFalse((build / ".git").exists())
            original = snapshot.VENDOR / "common/parallelDefs"
            before = original.read_bytes()
            (build / "common/parallelDefs").write_text("changed in build only")
            self.assertEqual(original.read_bytes(), before)
            snapshot.validate()
        with self.assertRaises(ValueError):
            configure_checkout(snapshot.VENDOR, Path("."), "c++")
        with self.assertRaises(ValueError):
            snapshot.build_copy(snapshot.VENDOR, snapshot.VENDOR / "build")

    def test_tampered_missing_extra_and_escaping_sources_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "source"
            shutil.copytree(snapshot.VENDOR, source, symlinks=True)
            license_file = source / "LICENSE"
            original = license_file.read_bytes()
            license_file.write_bytes(b"tampered")
            with self.assertRaises(ValueError):
                snapshot.validate(source)
            license_file.unlink()
            with self.assertRaises(ValueError):
                snapshot.validate(source)
            license_file.write_bytes(original)
            extra = source / "unexpected.cpp"
            extra.write_text("unexpected")
            with self.assertRaises(ValueError):
                snapshot.validate(source)
            extra.unlink()
            (source / "parlay").unlink()
            (source / "parlay").symlink_to(directory)
            with self.assertRaises(ValueError):
                snapshot.validate(source)


if __name__ == "__main__":
    unittest.main()
