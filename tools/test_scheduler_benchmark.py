# SPDX-License-Identifier: Apache-2.0
import contextlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import unittest
from unittest.mock import patch

import scheduler_benchmark as runner


class BenchmarkRunnerTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def records(self, rows):
        path = self.root / "result.json"
        path.write_text(json.dumps({"benchmarks": rows}))
        return runner.benchmark_records(path, 8, "default", "RAPID_AUTO")

    def test_units_and_aggregate_rows(self):
        rows = [{"name": unit, "real_time": value, "time_unit": unit}
                for unit, value in [("ns", 1e9), ("us", 1e6), ("ms", 1e3), ("s", 1)]]
        rows.append({"name": "median", "run_type": "aggregate"})
        records = self.records(rows)
        self.assertEqual([row["seconds"] for row in records], [1, 1, 1, 1])
        self.assertTrue(all(row["threads"] == 8 for row in records))

    def test_invalid_measurements_fail(self):
        valid = {"name": "Launch/64", "real_time": 10, "time_unit": "ns"}
        cases = [[], [{"error_occurred": True}], [{"run_type": "aggregate"}],
                 [valid, valid]]
        cases += [[dict(valid, real_time=value)] for value in [0, -1, float("nan"), float("inf")]]
        for index, rows in enumerate(cases):
            with self.subTest(case=index), self.assertRaises(ValueError):
                self.records(rows)

    def test_failed_process_preserves_output(self):
        log = self.root / "failed.log"
        with self.assertRaises(RuntimeError):
            runner.run([sys.executable, "-c", "print('diagnostic'); raise SystemExit(7)"], log)
        self.assertIn("diagnostic", log.read_text())

    def test_timeout_is_failure(self):
        with self.assertRaises(subprocess.TimeoutExpired):
            runner.run([sys.executable, "-c", "import time; time.sleep(30)"],
                       self.root / "timeout.log", timeout=0.1)

    def test_nested_build_preserves_environment_compiler_flags(self):
        configurations = []

        def run(command, *args, **kwargs):
            if command[:2] == ["cmake", "-S"]:
                configurations.append(command)
                if len(configurations) == 2:
                    raise InterruptedError("stop before compilation")

        with patch.object(sys, "argv", ["runner", "--mode", "RAPID_AUTO"]):
            args = runner.arguments()
        with patch.dict(os.environ, {"CXXFLAGS": "-fPIC -O2"}), \
                patch.object(runner, "run", run), \
                patch.object(runner, "capture", return_value="test"), \
                patch.object(runner.shutil, "which", return_value="tool"), \
                patch.object(runner.shutil, "copy2"), \
                self.assertRaises(InterruptedError):
            runner.execute(args, self.root, {})
        self.assertEqual(len(configurations), 2)
        self.assertIn("-DCMAKE_CXX_FLAGS=-fPIC -O2 -DOOX_RAPID_EXPERIMENT_NESTED_LAZY=1",
                      configurations[1])

    def test_archives_success_and_failure_without_build_tree(self):
        def execute(args, directory, metadata):
            (directory / "logs").mkdir()
            (directory / "logs/result.log").write_text("diagnostic")
            (directory / "build-default").mkdir()
            (directory / "build-default/large-object").write_text("build artifact")
            metadata["checks"] = ["8t-default-RAPID_AUTO"]
            if fail:
                raise ValueError("injected failure")

        for fail in [False, True]:
            with self.subTest(fail=fail), patch.object(runner, "execute", execute), \
                    patch.object(sys, "argv", ["runner", "--workdir", str(self.root)]), \
                    contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                self.assertEqual(runner.main(), int(fail))
        directories = [path for path in self.root.iterdir() if path.is_dir()]
        self.assertEqual(len(directories), 2)
        for directory in directories:
            metadata = json.loads((directory / "metadata.json").read_text())
            self.assertEqual(metadata["complete"], "error" not in metadata)
            self.assertEqual(metadata["checks"], ["8t-default-RAPID_AUTO"])
            with tarfile.open(directory.with_suffix(".tar.gz")) as archive:
                names = archive.getnames()
                self.assertIn(directory.name + "/runner.py", names)
                self.assertIn(directory.name + "/logs/result.log", names)
                self.assertFalse(any("build-default" in name for name in names))


if __name__ == "__main__":
    unittest.main()
