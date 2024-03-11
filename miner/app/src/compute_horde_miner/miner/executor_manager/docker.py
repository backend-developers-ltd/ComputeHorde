import asyncio
import logging
import subprocess

from django.conf import settings

from compute_horde_miner.miner.executor_manager.base import BaseExecutorManager, ExecutorUnavailable

PULLING_TIMEOUT = 300

logger = logging.getLogger(__name__)


class DockerExecutorManager(BaseExecutorManager):
    async def reserve_executor(self, token):
        if settings.ADDRESS_FOR_EXECUTORS:
            address = settings.ADDRESS_FOR_EXECUTORS
        else:
            address = subprocess.check_output([
                'docker',
                'inspect',
                '-f',
                '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}',
                'root_app_1'
            ]).decode().strip()
        process = await asyncio.create_subprocess_exec('docker', 'pull', settings.EXECUTOR_IMAGE)
        try:
            await asyncio.wait_for(process.communicate(), timeout=PULLING_TIMEOUT)
            if process.returncode:
                logger.error(f'Pulling executor container failed with returncode={process.returncode}')
                raise ExecutorUnavailable('Failed to pull executor image')
        except TimeoutError:
            process.kill()
            logger.error('Pulling executor container timed out, pulling it from shell might provide more details')
            raise ExecutorUnavailable('Failed to pull executor image')
        subprocess.Popen([  # noqa: S607
            "docker", "run", "--rm",
            "-e", f"MINER_ADDRESS=ws://{address}:{settings.PORT_FOR_EXECUTORS}",
            "-e", f"EXECUTOR_TOKEN={token}",
            # the executor must be able to spawn images on host
            "-v", "/var/run/docker.sock:/var/run/docker.sock",
            "-v", "/tmp:/tmp",
            settings.EXECUTOR_IMAGE,
            "python", "manage.py", "run_executor",
        ])
