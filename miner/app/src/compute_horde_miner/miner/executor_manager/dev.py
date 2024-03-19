import os
import pathlib
import subprocess
import sys

from django.conf import settings

from compute_horde_miner.miner.executor_manager.base import BaseExecutorManager

this_dir = pathlib.Path(__file__).parent
executor_dir = this_dir / '..' / '..' / '..' / '..' / '..' / '..' / 'executor'


class DevExecutorManager(BaseExecutorManager):
    def __init__(self):
        self._procs = {}

    async def reserve_executor(self, token):
        self._procs = {t: proc for t, proc in self._procs.values() if proc.poll() is None}
        self._procs[token] = subprocess.Popen(
            [sys.executable, "app/src/manage.py", "run_executor"],
            env={
                'MINER_ADDRESS': f'ws://{settings.ADDRESS_FOR_EXECUTORS}:{settings.PORT_FOR_EXECUTORS}',
                'EXECUTOR_TOKEN': token,
                'PATH': os.environ['PATH'],
            },
            cwd=executor_dir,
        )

    async def destroy_executor(self, token):
        if token in self._procs:
            self._procs.pop(token).kill()
