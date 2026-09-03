#!/usr/bin/env python3
"""Run CTest while exposing failures as GitHub Actions annotations."""

import os
import subprocess
import sys


def main() -> int:
    result = subprocess.run(
        ["ctest", *sys.argv[1:]],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    print(result.stdout, end="")
    if result.returncode and os.environ.get("GITHUB_ACTIONS") == "true":
        escaped = result.stdout.replace("%", "%25")
        escaped = escaped.replace("\r", "%0D").replace("\n", "%0A")
        print(f"::error title=Scheduler evaluation failure::{escaped}")
    return result.returncode


if __name__ == "__main__":
    sys.exit(main())
