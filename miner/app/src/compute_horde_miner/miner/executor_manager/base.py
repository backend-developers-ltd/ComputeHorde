import abc
import asyncio
import datetime as dt
import logging

logger = logging.getLogger(__name__)


# this leaves around 1 min for synthetic job to complete
EXECUTOR_TIMEOUT = dt.timedelta(minutes=4).total_seconds()


class ExecutorUnavailable(Exception):
    pass


class BaseExecutorManager(metaclass=abc.ABCMeta):
    def __init__(self):
        # supporting multiple executors requires significant refactor
        self._semaphore = asyncio.Semaphore(1)
        self._executor = None
        self._executor_start_timestamp = None

    @abc.abstractmethod
    async def _reserve_executor(self, token):
        """Start spinning up an executor with `token` or raise ExecutorUnavailable if at capacity"""

    @abc.abstractmethod
    async def _kill_executor(self, executor):
        """Kill running executor. It might be platform specific, so leave it to Manager implementation"""

    @abc.abstractmethod
    async def _wait_for_executor(self, executor, timeout):
        """Wait for executor to finish the job for till timout"""

    async def reserve_executor(self, token):
        async with self._semaphore:
            if self._executor is not None:
                wait_time = max(
                    0,
                    EXECUTOR_TIMEOUT
                    - (dt.datetime.now() - self._executor_start_timestamp).total_seconds(),
                )
                if wait_time > 0:
                    logger.debug(f"waiting for {wait_time}s for previous executor to finish")
                    await self._wait_for_executor(self._executor, wait_time)
                logger.debug("killing previous executor if it is still running")
                await self._kill_executor(self._executor)
                self._executor = None
                self._executor_start_timestamp = None
            executor_process = await self._reserve_executor(token)
            self._executor = executor_process
            self._executor_start_timestamp = dt.datetime.now()
