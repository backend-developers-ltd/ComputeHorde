import logging
import os
import pathlib
import subprocess

from django.conf import settings

from compute_horde_miner.miner.executor_manager._internal.base import (
    BaseExecutorManager,
)
from compute_horde_miner.miner.executor_manager.executor_port_dispenser import (
    executor_port_dispenser,
)

logger = logging.getLogger(__name__)

this_dir = pathlib.Path(__file__).parent
executor_dir = this_dir / ".." / ".." / ".." / ".." / ".." / ".." / ".." / "executor"


class DevExecutorManager(BaseExecutorManager):
    async def start_new_executor(self, token, executor_class, timeout):
        nginx_port = executor_port_dispenser.get_port()

        return subprocess.Popen(
            ["uv", "run", "python", "app/src/manage.py", "run_executor"],
            env={
                "MINER_ADDRESS": f"ws://{settings.ADDRESS_FOR_EXECUTORS}:{settings.PORT_FOR_EXECUTORS}",
                "EXECUTOR_TOKEN": token,
                "PATH": os.environ["PATH"],
                "NGINX_PORT": str(nginx_port),
                # Enable hf_transfer download acceleration package
                # https://huggingface.co/docs/huggingface_hub/package_reference/environment_variables#hfhubenablehftransfer
                "HF_HUB_ENABLE_HF_TRANSFER": "1",
            },
            cwd=executor_dir,
        )

    async def kill_executor(self, executor):
        logger.info("Killing executor process, PID=%s", getattr(executor, "pid", None))
        try:
            executor.kill()
        except OSError as e:
            logger.error("Failed to kill executor: %s", e)

    async def wait_for_executor(self, executor, timeout):
        try:
            return executor.wait(timeout)
        except subprocess.TimeoutExpired:
            pass

    async def get_manifest(self):
        return {settings.DEFAULT_EXECUTOR_CLASS: 1}

    async def get_executor_public_address(self, executor: str) -> str | None:
        return "127.0.0.1"
