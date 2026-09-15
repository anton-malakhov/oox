# SPDX-License-Identifier: Apache-2.0
"""Identify measured artifacts without claiming checkout HEAD built them."""
import hashlib
from pathlib import Path
import subprocess


def sha256(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def checkout(root):
    def git(*args):
        return subprocess.check_output(["git", "-C", str(root), *args])
    status = git("status", "--porcelain=v1", "--untracked-files=all")
    diff = git("diff", "HEAD", "--binary")
    return dict(commit=git("rev-parse", "HEAD").decode().strip(),
                dirty=bool(status), status=status.decode(),
                tracked_diff_sha256=hashlib.sha256(diff).hexdigest(),
                note="Checkout at run time, not verified build provenance. "
                     "Untracked contents and submodule worktrees are not included in the diff hash.")


def artifacts(paths):
    return {str(path.resolve()): dict(sha256=sha256(path), bytes=path.stat().st_size)
            for path in paths}


def execution_environment(environment):
    result = dict(environment)
    # All modes inherit the same CPU set; disable runtime-specific repinning.
    result.update(OMP_PROC_BIND="false", KMP_AFFINITY="disabled")
    result.pop("GOMP_CPU_AFFINITY", None)
    result.pop("KMP_HW_SUBSET", None)
    return result
