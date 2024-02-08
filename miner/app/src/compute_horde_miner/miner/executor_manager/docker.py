import subprocess

from django.conf import settings

from compute_horde_miner.miner.executor_manager.base import BaseExecutorManager


EXECUTOR_IMAGE = "backenddevelopersltd/compute-horde-executor:v0-latest"


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
        subprocess.check_call(['docker', 'pull', EXECUTOR_IMAGE])
        subprocess.Popen([  # noqa: S607
            "docker", "run", "--rm",
            "-e", f"MINER_ADDRESS=ws://{address}:{settings.PORT_FOR_EXECUTORS}",
            "-e", f"EXECUTOR_TOKEN={token}",
            # the executor must be able to spawn images on host
            "-v", "/var/run/docker.sock:/var/run/docker.sock",
            "-v", "/tmp:/tmp",
            EXECUTOR_IMAGE,
            "python", "manage.py", "run_executor",
        ])
