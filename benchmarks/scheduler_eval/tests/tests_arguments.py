# SPDX-License-Identifier: Apache-2.0
import json
import os
import subprocess
import sys
import unittest

BINARY = sys.argv.pop(1)


class ArgumentTests(unittest.TestCase):
    def invoke(self, *args, variable="BENCH_NUM_THREADS", threads="2"):
        env = {k: v for k, v in os.environ.items()
               if k not in ("BENCH_NUM_THREADS", "PARLAY_NUM_THREADS", "OMP_NUM_THREADS")}
        env[variable] = threads
        return subprocess.run([BINARY, *args], env=env, capture_output=True, text=True, timeout=5)

    def test_valid_input_and_every_json_control_character(self):
        result = self.invoke("--value", "0")
        self.assertEqual(result.returncode, 0, result.stderr)
        value = json.loads(result.stdout)
        self.assertEqual(value["value"], 0)
        self.assertEqual(value["threads"], 2)
        self.assertEqual(value["text"], "".join(map(chr, range(32))) + chr(34) + chr(92) + "/UTF-8: é")

    def test_invalid_environment_has_diagnostic_not_abort(self):
        for variable in ("BENCH_NUM_THREADS", "PARLAY_NUM_THREADS", "OMP_NUM_THREADS"):
            for invalid in ("", "0", "-1", "+2", " 2", "2x", "65536", "9" * 40):
                with self.subTest(variable=variable, value=invalid):
                    result = self.invoke(variable=variable, threads=invalid)
                    self.assertEqual(result.returncode, 2)
                    self.assertIn(variable, result.stderr)

    def test_invalid_or_missing_cli_value_has_diagnostic(self):
        for invalid in ([], ["-1"], ["1x"], [""], [" 2"], ["9" * 40]):
            with self.subTest(value=invalid):
                result = self.invoke("--value", *invalid)
                self.assertEqual(result.returncode, 2)
                self.assertIn("--value", result.stderr)


if __name__ == "__main__":
    unittest.main()
