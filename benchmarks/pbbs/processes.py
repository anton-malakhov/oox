# SPDX-License-Identifier: Apache-2.0
"""Run a POSIX benchmark command with bounded process-group lifetime."""
import os
import signal
import subprocess


def run(command, *, timeout=None, check=False, **kwargs):
    if os.name != "posix":
        raise RuntimeError("PBBS process-tree cleanup requires POSIX process groups")
    with subprocess.Popen(command, start_new_session=True, **kwargs) as process:
        try:
            stdout, stderr = process.communicate(timeout=timeout)
        except BaseException:
            # The driver, make, compilers and benchmark inherit this fresh group.
            # Kill the whole group even if the immediate child already exited.
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait()
            raise
        result = subprocess.CompletedProcess(command, process.returncode, stdout, stderr)
        if check:
            result.check_returncode()
        return result
