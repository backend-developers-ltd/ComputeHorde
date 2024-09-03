import os
import pathlib
import subprocess
import sys

from django.conf import settings

from compute_horde_miner.miner.executor_manager._internal.base import BaseExecutorManager

this_dir = pathlib.Path(__file__).parent
executor_dir = this_dir / ".." / ".." / ".." / ".." / ".." / ".." / ".." / "executor"


class DevExecutorManager(BaseExecutorManager):
    async def start_new_executor(self, token, executor_class, timeout):
        return subprocess.Popen(
            [sys.executable, "app/src/manage.py", "run_executor"],
            env={
                "MINER_ADDRESS": f"ws://{settings.ADDRESS_FOR_EXECUTORS}:{settings.PORT_FOR_EXECUTORS}",
                "EXECUTOR_TOKEN": token,
                "PATH": os.environ["PATH"],
            },
            cwd=executor_dir,
        )

    async def kill_executor(self, executor):
        try:
            executor.kill()
        except OSError:
            pass

    async def wait_for_executor(self, executor, timeout):
        try:
            return executor.wait(timeout)
        except subprocess.TimeoutExpired:
            pass

    async def get_manifest(self):
        return {settings.DEFAULT_EXECUTOR_CLASS: 1}
