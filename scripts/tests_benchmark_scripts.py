# SPDX-License-Identifier: Apache-2.0
"""Exercise branch-runner isolation with fake Git and benchmark commands."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


class ScriptTests(unittest.TestCase):
    def test_comparison_failure_is_not_reported_as_success(self):
        script = Path(__file__).with_name("run_all_apple_m4_benchmark_branches.sh").read_text()
        marker = 'if [[ -f "${ROOT}/scripts/compare_local_benchmark_branches.py" ]]; then'
        tail = marker + script.split(marker, 1)[1]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "scripts").mkdir()
            compare = root / "scripts/compare_local_benchmark_branches.py"
            compare.write_text("raise SystemExit(9)\n")
            env = dict(os.environ, ROOT=str(root), OUT_ROOT="results", WORKTREE_ROOT="unused")
            result = subprocess.run(["bash", "-c", "set -euo pipefail\n" + tail],
                                    env=env, capture_output=True, text=True, timeout=5)
            self.assertEqual(result.returncode, 9)
            self.assertNotIn("Done.", result.stdout)

    def test_m1max_never_switches_or_resets_main_checkout(self):
        script = Path(__file__).with_name("run_m1max_three_branches.sh")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            for name in ("scripts", "bin", "tmp"):
                (root / name).mkdir()
            shutil.copyfile(script, root / "scripts" / script.name)
            marker = root / "uncommitted.txt"
            marker.write_text("keep my edits")
            calls = root / "calls.jsonl"
            runner = root / "fake_runner"
            runner.write_text("#!" + sys.executable + "\n" +
                "import json,os,sys\n"
                "with open(os.environ['FAKE_CALLS'], 'a') as f:\n"
                "    f.write(json.dumps(['runner', os.getcwd(), sys.argv[1:]]) + '\\n')\n")
            runner.chmod(0o755)
            git = root / "bin/git"
            git.write_text("#!" + sys.executable + "\n" +
                "import json,os,shutil,sys\nfrom pathlib import Path\n"
                "a=sys.argv[1:]\n"
                "with open(os.environ['FAKE_CALLS'], 'a') as f:\n"
                "    f.write(json.dumps(['git', a]) + '\\n')\n"
                "if a == ['fetch', 'origin']: pass\n"
                "elif a[:3] == ['worktree', 'add', '--detach']:\n"
                "    p=Path(a[3]) / 'scripts'; p.mkdir(parents=True)\n"
                "    shutil.copy2(os.environ['FAKE_RUNNER'], p / 'run_apple_m4_benchmarks.sh')\n"
                "else: raise SystemExit('unsafe/unexpected Git command: ' + repr(a))\n")
            git.chmod(0o755)
            env = dict(os.environ, PATH=str(root / "bin") + os.pathsep + os.environ["PATH"],
                       TMPDIR=str(root / "tmp"), FAKE_CALLS=str(calls), FAKE_RUNNER=str(runner))
            subprocess.run(["bash", str(root / "scripts" / script.name)], env=env,
                           check=True, capture_output=True, text=True, timeout=10)
            entries = [json.loads(line) for line in calls.read_text().splitlines()]
            self.assertEqual(marker.read_text(), "keep my edits")
            self.assertEqual(sum(e[0] == "git" for e in entries), 4)
            runs = [e for e in entries if e[0] == "runner"]
            self.assertEqual(len(runs), 3)
            for _, cwd, args in runs:
                self.assertTrue(Path(cwd).is_relative_to(root / "tmp"))
                self.assertTrue(Path(args[args.index("--out-dir") + 1]).is_relative_to(root / "results"))


if __name__ == "__main__":
    unittest.main()
