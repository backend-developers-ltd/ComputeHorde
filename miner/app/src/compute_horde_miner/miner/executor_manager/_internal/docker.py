import asyncio
import logging
import os
import subprocess

from compute_horde.certificate import get_docker_container_ip
from django.conf import settings

from compute_horde_miner.miner.executor_manager._internal.base import (
    BaseExecutorManager,
    ExecutorFailed,
)
from compute_horde_miner.miner.executor_manager._internal.selector import (
    HistoricalRandomMinerSelector,
)
from compute_horde_miner.miner.executor_manager.executor_port_dispenser import (
    executor_port_dispenser,
)

PULLING_TIMEOUT = 300
DOCKER_STOP_TIMEOUT = 5

logger = logging.getLogger(__name__)


class DockerExecutor:
    def __init__(self, process_executor, token):
        self.process_executor = process_executor
        self.token = token  # used as container name


class DockerExecutorManager(BaseExecutorManager):
    def __init__(self):
        super().__init__()

        self.selector = HistoricalRandomMinerSelector(
            settings.CLUSTER_SECRET,
        )

    async def start_new_executor(self, token, executor_class, timeout):
        if settings.ADDRESS_FOR_EXECUTORS:
            address = settings.ADDRESS_FOR_EXECUTORS
        else:
            compose_project_name = os.getenv("COMPOSE_PROJECT_NAME", "root")
            container_id = (
                subprocess.check_output(
                    ["docker", "ps", "-q", "--filter", f"name={compose_project_name}[_-]app[_-]1"]
                )
                .decode()
                .strip()
            )
            address = await get_docker_container_ip(container_id)
        if not settings.DEBUG_SKIP_PULLING_EXECUTOR_IMAGE:
            process = await asyncio.create_subprocess_exec(
                "docker", "pull", settings.EXECUTOR_IMAGE
            )
            try:
                await asyncio.wait_for(process.communicate(), timeout=PULLING_TIMEOUT)
                if process.returncode:
                    logger.error(
                        f"Pulling executor container failed with returncode={process.returncode}"
                    )
                    raise ExecutorFailed("Failed to pull executor image")
            except TimeoutError:
                process.kill()
                logger.error(
                    "Pulling executor container timed out, pulling it from shell might provide more details"
                )
                raise ExecutorFailed("Failed to pull executor image")
        hf_args = (
            []
            if settings.HF_ACCESS_TOKEN is None
            else ["-e", f"HF_ACCESS_TOKEN={settings.HF_ACCESS_TOKEN}"]
        )

        nginx_port = executor_port_dispenser.get_port()
        process_executor = await asyncio.create_subprocess_exec(  # noqa: S607
            "docker",
            "run",
            "--rm",
            "-e",
            f"MINER_ADDRESS=ws://{address}:{settings.PORT_FOR_EXECUTORS}",
            "-e",
            f"EXECUTOR_TOKEN={token}",
            "-e",
            f"NGINX_PORT={nginx_port}",
            *hf_args,
            "--name",
            token,
            # the executor must be able to spawn images on host
            "-v",
            "/var/run/docker.sock:/var/run/docker.sock",
            "-v",
            "/tmp:/tmp",
            settings.EXECUTOR_IMAGE,
            "python",
            "manage.py",
            "run_executor",
        )
        return DockerExecutor(process_executor, token)

    async def kill_executor(self, executor):
        # kill executor container first so it would not be able to report anything - job simply timeouts
        process = await asyncio.create_subprocess_exec("docker", "stop", executor.token)
        try:
            await asyncio.wait_for(process.wait(), timeout=DOCKER_STOP_TIMEOUT)
        except TimeoutError:
            pass

        process = await asyncio.create_subprocess_exec("docker", "stop", f"{executor.token}-job")
        try:
            await asyncio.wait_for(process.wait(), timeout=DOCKER_STOP_TIMEOUT)
        except TimeoutError:
            pass

        try:
            executor.process_executor.kill()
        except OSError:
            pass

    async def wait_for_executor(self, executor, timeout):
        try:
            return await asyncio.wait_for(executor.process_executor.wait(), timeout=timeout)
        except TimeoutError:
            pass

    async def get_manifest(self):
        return {settings.DEFAULT_EXECUTOR_CLASS: 1}

    async def is_active(self) -> bool:
        selected = await self.selector.active(
            settings.CLUSTER_HOTKEYS,
        )
        my_address = settings.BITTENSOR_WALLET().hotkey.ss58_address  # type: str

        return selected == my_address

    async def get_executor_public_address(self, executor: DockerExecutor) -> str | None:
        return await get_docker_container_ip(executor.token)
