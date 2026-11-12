import json
import io
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import calibrate
from calibration_protocol import (build_metadata, check_threads, clean_environment,
                                  hardware, make_plan)
from run import load_profile


class ProtocolTest(unittest.TestCase):
    def test_golden_plan_and_balance(self):
        plan = make_plan([8, 4, 4], 9, 5, 20261014, "arithmetic_v1", 40)
        self.assertEqual(plan["id"], "6175201980065aef77d4d062b8ce62f2562cac113cf1fb7b47528c0273edb2cf")
        self.assertEqual(plan, make_plan([4, 8], 9, 5, 20261014, "arithmetic_v1", 40))
        self.assertEqual(len(plan["jobs"]), 59)
        for workers in (4, 8):
            first = 0
            for rep in range(9):
                order = [j["mode"] for j in plan["jobs"] if j["workers"] == workers and j["repetition"] == rep]
                self.assertEqual(set(order), {"RAPID_AUTO", "RAPID_START", "TBB_AUTO"})
                first += order.index("RAPID_AUTO") < order.index("RAPID_START")
            self.assertIn(first, (4, 5))

    def test_kernel_and_seed_have_distinct_plan_ids(self):
        plans = [make_plan([2], 3, 3, seed, kernel, 5) for seed in (1, 2) for kernel in ("arithmetic_v1", "cpu_relax")]
        self.assertEqual(len({p["id"] for p in plans}), 4)

    def test_limits(self):
        for workers in ([], [0], [65], [True]):
            with self.subTest(workers=workers), self.assertRaises(ValueError):
                make_plan(workers, 9, 5, 1, "arithmetic_v1", 40)
        with self.assertRaises(ValueError):
            check_threads([8], {"available_cpus": 4})
        check_threads([8], {"available_cpus": 4}, True)

    def test_cpu_set(self):
        self.assertEqual(calibrate.cpu_set("0-3,8,10-11"), {0, 1, 2, 3, 8, 10, 11})
        for text in ("", "-1", "3-1", "x", "1-3-5"):
            with self.subTest(text=text), self.assertRaises(Exception):
                calibrate.cpu_set(text)

    def test_environment_is_scoped(self):
        with patch.dict("os.environ", {"OMP_NUM_THREADS": "99", "TBB_TEST": "1", "SAFE": "ok"}, clear=True):
            env = clean_environment()
            self.assertNotIn("OMP_NUM_THREADS", env)
            self.assertNotIn("TBB_TEST", env)
            self.assertEqual(env["SAFE"], "ok")
            self.assertEqual(env["LC_ALL"], "C")
        with patch.dict("os.environ", {"LD_PRELOAD": "unknown.so"}, clear=True), self.assertRaises(ValueError):
            clean_environment()

    def test_build_guard(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "CMakeCache.txt"
            path.write_text("CMAKE_BUILD_TYPE:STRING=Debug\n")
            with self.assertRaises(ValueError):
                build_metadata(directory)
            path.write_text("CMAKE_BUILD_TYPE:STRING=Release\nOOX_SCHEDULER_EVAL_STATS:BOOL=ON\n")
            with self.assertRaises(ValueError):
                build_metadata(directory)
            path.write_text("CMAKE_BUILD_TYPE:STRING=Release\nCMAKE_CONFIGURATION_TYPES:STRING=Debug;Release\n")
            with self.assertRaises(ValueError):
                build_metadata(directory)

    def test_plan_only_has_no_outputs(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "must-not-exist"
            args = ["calibrate.py", "--output", str(output), "--threads", "4", "8", "--plan-only"]
            with patch("sys.argv", args), patch("calibrate.hardware", return_value={"available_cpus": 8}), \
                 patch("sys.stdout", new_callable=io.StringIO) as stdout:
                calibrate.main()
                self.assertEqual(json.loads(stdout.getvalue())["id"], "6175201980065aef77d4d062b8ce62f2562cac113cf1fb7b47528c0273edb2cf")
            self.assertFalse(output.exists())

    def test_failure_keeps_status_and_existing_output(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "run"
            args = ["calibrate.py", "--output", str(output), "--threads", "2", "--no-configure"]
            with patch("sys.argv", args), patch("calibrate.hardware", return_value={"available_cpus": 2}), \
                 patch("calibrate.build_metadata", side_effect=ValueError("synthetic invalid build")):
                with self.assertRaises(ValueError):
                    calibrate.main()
                status = (output / "status.json").read_bytes()
                self.assertEqual(json.loads(status)["state"], "failed")
                self.assertTrue((output / "plan.json").exists())
                with self.assertRaises(FileExistsError):
                    calibrate.main()
                self.assertEqual((output / "status.json").read_bytes(), status)
    def test_linux_affinity_and_quota(self):
        contents = {"/proc/cpuinfo": "model name : Test CPU\n", "/proc/self/cgroup": "0::/group\n",
                    "/sys/fs/cgroup/cpu.max": "max 100000", "/sys/fs/cgroup/group/cpu.max": "200000 100000"}
        with patch("calibration_protocol.platform.system", return_value="Linux"), \
             patch("calibration_protocol.os.cpu_count", return_value=64), \
             patch("calibration_protocol.os.sched_getaffinity", return_value=set(range(8)), create=True), \
             patch("calibration_protocol.read_text", side_effect=lambda p, *a: contents.get(str(p))), \
             patch("calibration_protocol.command_output", return_value=None):
            info = hardware()
            self.assertEqual(info["available_cpus"], 2)
            self.assertEqual(info["cpu"], "Test CPU")

    def test_profile_without_node(self):
        data = {"recorded_at": "2026-10-14T00:00:00Z", "hardware": {}, "protocol": {}, "toolchain": {},
                "binaries": {}, "sources": {}, "measurements": [], "paired": [],
                "serial_cases": {"balanced64_100": {"median": 6500}, "balanced64_1000": {"median": 64100}, "loop64": {"median": 100}},
                "primitives_ns": {key: {"median": 1, "q25": 1, "q75": 1} for key in ("push_front_hot", "pop_front_hot", "pop_back_uncontended_same_thread", "empty_pop_back", "sc_store_acquire_load")}}
        profile = calibrate.compact_profile(data, "test")
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "profile.json"
            path.write_text(json.dumps(profile))
            loaded = load_profile(path)
            self.assertEqual(loaded["body_ns"]["1000"], 1000)
            self.assertEqual(loaded["holdout"], {})


if __name__ == "__main__":
    unittest.main()
