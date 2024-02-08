import os
import pathlib
import subprocess
import sys

from django.conf import settings

from compute_horde_miner.miner.executor_manager.base import BaseExecutorManager

this_dir = pathlib.Path(__file__).parent
executor_dir = this_dir / '..' / '..' / '..' / '..' / '..' / '..' / 'executor'


class DevExecutorManager(BaseExecutorManager):
    async def reserve_executor(self, token):
        subprocess.Popen(
            [sys.executable, "app/src/manage.py", "run_executor"],
            env={
                'MINER_ADDRESS': f'ws://{settings.ADDRESS_FOR_EXECUTORS}:{settings.PORT_FOR_EXECUTORS}',
                'EXECUTOR_TOKEN': token,
                'PATH': os.environ['PATH'],
            },
            cwd=executor_dir,
        )
