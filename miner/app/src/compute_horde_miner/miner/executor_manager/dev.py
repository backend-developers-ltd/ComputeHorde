import subprocess

from compute_horde_miner.miner.executor_manager.base import BaseExecutorManager


class DevExecutorManager(BaseExecutorManager):
    def reserve_executor(self, token):
        subprocess.Popen(
            ["python", "app/src/manage.py", "run_executor"],
            env={},
            cwd={}
        )