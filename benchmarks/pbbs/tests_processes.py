# SPDX-License-Identifier: Apache-2.0
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest

import processes


@unittest.skipUnless(os.name == "posix", "requires POSIX process groups")
class ProcessTests(unittest.TestCase):
    def test_output_and_failure_status(self):
        result = processes.run([sys.executable, "-c", "print('ok')"],
                               stdout=subprocess.PIPE, text=True, check=True, timeout=5)
        self.assertEqual(result.stdout, "ok\n")
        with self.assertRaises(subprocess.CalledProcessError) as error:
            processes.run([sys.executable, "-c", "raise SystemExit(7)"], check=True)
        self.assertEqual(error.exception.returncode, 7)

    def test_timeout_kills_grandchild_even_when_it_ignores_term(self):
        with tempfile.TemporaryDirectory() as directory:
            ready = Path(directory) / "ready"
            survived = Path(directory) / "survived"
            child = ("import signal,time; from pathlib import Path; "
                     "signal.signal(signal.SIGTERM, signal.SIG_IGN); "
                     f"Path({str(ready)!r}).touch(); time.sleep(3); "
                     f"Path({str(survived)!r}).touch()")
            parent = ("import subprocess,sys,time; "
                      f"subprocess.Popen([sys.executable, '-c', {child!r}]); "
                      "time.sleep(60)")
            with self.assertRaises(subprocess.TimeoutExpired):
                processes.run([sys.executable, "-c", parent], timeout=2,
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            self.assertTrue(ready.exists(), "grandchild must start before timeout")
            time.sleep(3.2)
            self.assertFalse(survived.exists(), "grandchild survived process-group cleanup")


if __name__ == "__main__":
    unittest.main()
