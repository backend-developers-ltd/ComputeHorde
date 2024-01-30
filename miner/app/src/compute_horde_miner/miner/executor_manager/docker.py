import subprocess

from django.conf import settings

from compute_horde_miner.miner.executor_manager.base import BaseExecutorManager


class DockerExecutorManager(BaseExecutorManager):
    def reserve_executor(self, token):
        subprocess.Popen([  # noqa: S607
            "docker", "run", "--rm",
            "-e", f"MINER_ADDRESS={settings.ADDRESS_FOR_EXECUTORS}",
            "-e", f"EXECUTOR_TOKEN={token}",
            # the executor must be able to spawn images on host
            "-v", "/var/run/docker.sock:/var/run/docker.sock",
            "ghcr.io/backend-developers-ltd/computehorde/executor-app:v0-latest",
            "python", "manage.py", "run_executor",
        ])
