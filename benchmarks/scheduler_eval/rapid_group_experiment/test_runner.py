#!/usr/bin/env python3
from pathlib import Path
import subprocess
import sys
import unittest
from run import plan

BINARY = Path(sys.argv.pop()).resolve() if len(sys.argv) > 1 else None


class Protocol(unittest.TestCase):
    def test_large_pool_is_not_a_128_member_group(self):
        p = plan([128], None, ["lazy", "auto"], 3, 1, None)
        self.assertEqual({j["config"]["group"] for j in p["jobs"]}, {2, 4, 8, 16, 32, 64})
        self.assertTrue(all(j["config"]["workers"] == j["config"]["residents"] == 128 for j in p["jobs"]))
        with self.assertRaises(ValueError): plan([128], [128], ["auto"], 3, 1, None)

    def test_deterministic_adjacent_pairs(self):
        a = plan([4, 8], None, ["auto", "lazy"], 5, 1, None)
        self.assertEqual(a, plan([8, 4], None, ["auto", "lazy"], 5, 1, None))
        for first, second in zip(a["jobs"][::2], a["jobs"][1::2]):
            self.assertEqual(first["contrast"], second["contrast"])
            self.assertEqual(first["repetition"], second["repetition"])
            self.assertEqual({first["side"], second["side"]}, {"a", "b"})
            self.assertEqual(first["config"]["residents"], second["config"]["residents"])

    def test_invalid_limits(self):
        for threads, groups, residents in [([0], None, None), ([8], [9], None), ([8], [4], 2)]:
            with self.assertRaises(ValueError): plan(threads, groups, ["auto"], 3, 1, residents)

    def test_binary_argument_validation(self):
        if BINARY is None: self.skipTest("binary not supplied")
        for args in [("--workers", "0"), ("--workers", "128", "--group", "128"),
                     ("--policy", "lazy", "--placement", "all"),
                     ("--workers", "4", "--group", "4", "--placement", "outsiders"),
                     ("--diagnose",), ("--unknown", "1")]:
            result = subprocess.run([str(BINARY), *args], capture_output=True, text=True, timeout=5)
            self.assertNotEqual(result.returncode, 0, args)


if __name__ == "__main__":
    unittest.main()
