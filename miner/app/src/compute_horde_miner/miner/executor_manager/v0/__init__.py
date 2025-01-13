import abc

from compute_horde.executor_class import (
    DEFAULT_EXECUTOR_CLASS,
    DEFAULT_EXECUTOR_TIMEOUT,
    MAX_EXECUTOR_TIMEOUT,  # unused, but re-exported
)

from compute_horde_miner.miner.executor_manager import v1

EXECUTOR_TIMEOUT = DEFAULT_EXECUTOR_TIMEOUT
DockerExecutor = v1.DockerExecutor
PULLING_TIMEOUT = v1.PULLING_TIMEOUT
DOCKER_STOP_TIMEOUT = v1.DOCKER_STOP_TIMEOUT
ExecutorUnavailable = v1.ExecutorUnavailable
ExecutorFailedToStart = v1.ExecutorFailedToStart


class BaseExecutorManager(v1.BaseExecutorManager):
    @abc.abstractmethod
    async def _reserve_executor(self, token):
        """Start spinning up an executor with `token` or raise ExecutorUnavailable if at capacity"""

    @abc.abstractmethod
    async def _kill_executor(self, executor):
        """Kill running executor. It might be platform specific, so leave it to Manager implementation"""

    @abc.abstractmethod
    async def _wait_for_executor(self, executor, timeout):
        """Wait for executor to finish the job for till timeout"""

    async def start_new_executor(self, token, executor_class, timeout):
        return await self._reserve_executor(token)

    async def kill_executor(self, executor):
        return await self._kill_executor(executor)

    async def wait_for_executor(self, executor, timeout):
        return await self._wait_for_executor(executor, timeout)


class DevExecutorManager(v1.DevExecutorManager):
    async def _reserve_executor(self, token):
        return await super().start_new_executor(
            token, DEFAULT_EXECUTOR_CLASS, DEFAULT_EXECUTOR_TIMEOUT
        )

    async def _kill_executor(self, executor):
        return await super().kill_executor(executor)

    async def _wait_for_executor(self, executor, timeout):
        return await super().wait_for_executor(executor, timeout)

    async def start_new_executor(self, token, executor_class, timeout):
        return await self._reserve_executor(token)

    async def kill_executor(self, executor):
        return await self._kill_executor(executor)

    async def wait_for_executor(self, executor, timeout):
        return await self._wait_for_executor(executor, timeout)


class DockerExecutorManager(v1.DockerExecutorManager):
    async def _reserve_executor(self, token):
        return await super().start_new_executor(
            token, DEFAULT_EXECUTOR_CLASS, DEFAULT_EXECUTOR_TIMEOUT
        )

    async def _kill_executor(self, executor):
        return await super().kill_executor(executor)

    async def _wait_for_executor(self, executor, timeout):
        return await super().wait_for_executor(executor, timeout)

    async def start_new_executor(self, token, executor_class, timeout):
        return await self._reserve_executor(token)

    async def kill_executor(self, executor):
        return await self._kill_executor(executor)

    async def wait_for_executor(self, executor, timeout):
        return await self._wait_for_executor(executor, timeout)
