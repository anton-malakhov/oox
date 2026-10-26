#!/usr/bin/env python3
"""Capture current compiled C++ trace targets, audit, then open the viewer."""
import argparse
from datetime import datetime
import json
from pathlib import Path
import shlex
import subprocess
import sys


HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
TRACING = REPO / "benchmarks/scheduler_eval/tracing"
MODES = ("EIGEN_STEALING", "EIGEN_SHARING", "TBB_AUTO", "RAPID_START", "RAPID_AUTO",
         "RAPID_NATIVE_AUTO", "RAPID_NATIVE_SIMPLE", "RAPID_NATIVE_STATIC", "RAPID_NATIVE_AFFINITY")


def main():
    if sys.version_info < (3, 10):
        raise SystemExit("Trace audit requires Python 3.10+; viewing alone supports Python 3.8+.")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build", type=Path, required=True)
    parser.add_argument("--threads", type=int, choices=(4, 8), default=8)
    parser.add_argument("--shape", choices=("balanced", "tail", "hyperbolic"), default="tail")
    parser.add_argument("--tasks", type=int, default=64)
    parser.add_argument("--work", type=int, default=1000)
    parser.add_argument("--iterations", type=int, default=3)
    parser.add_argument("--grain", type=int, default=2)
    parser.add_argument("--no-serve", action="store_true")
    args = parser.parse_args()
    subprocess.run(["cmake", "--build", str(args.build.resolve()), "--target"] +
                   ["trace_scheduler_" + mode for mode in MODES], check=True)
    output = REPO / "results" / ("viewer-trace-" + datetime.now().strftime("%Y%m%d-%H%M%S-%f"))
    command = [sys.executable, str(TRACING / "record.py"), "--build", str(args.build.resolve()),
               "--output", str(output), "--modes", *MODES, "--callbacks", "--grain", str(args.grain),
               "--threads", str(args.threads), "--shape", args.shape, "--tasks", str(args.tasks),
               "--work", str(args.work), "--iterations", str(args.iterations)]
    subprocess.run(command, check=True)
    report = subprocess.run([sys.executable, str(TRACING / "audit.py"),
                             *[str(output / (mode + ".json")) for mode in MODES]],
                            capture_output=True, text=True, check=True)
    (output / "audit.json").write_text(report.stdout)
    if any(r["status"] != "pass" for r in json.loads(report.stdout)["reports"]):
        raise RuntimeError("Trace audit did not pass")
    serve = [sys.executable, str(HERE / "run.py"), "--traces", str(output)]
    print("Open again: " + shlex.join(serve), flush=True)
    if not args.no_serve:
        subprocess.run(serve, check=True)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        raise SystemExit(str(error))
