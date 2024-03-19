import abc
import asyncio
import datetime as dt


EXECUTOR_TIMEOUT = dt.timedelta(minutes=4).total_seconds()


class ExecutorUnavailable(Exception):
    pass


class BaseExecutorManager(metaclass=abc.ABCMeta):
    def __init__(self):
        self._semaphore = asyncio.Semaphore(1)  # supporting multiple executors requires significant refactor
        self._executor = None
        self._executor_start_timestamp = None

    @abc.abstractmethod
    async def _reserve_executor(self, token):
        """Start spinning up an executor with `token` or raise ExecutorUnavailable if at capacity"""

    @abc.abstractmethod
    def _kill_executor(self, executor):
        """Kill running executor. It might be platform specific, so leave it to Manager implementation"""

    async def reserve_executor(self, token):
        # basic logic here - only send kill signal to most recently
        async with self._semaphore:
            if self._executor is not None:
                wait_time = max(0, EXECUTOR_TIMEOUT - (dt.datetime.now() - self._executor_start_timestamp).total_seconds())
                if wait_time > 0:
                    await asyncio.sleep(wait_time)
                self._kill_executor(self._executor)
                self._executor = None
                self._executor_start_timestamp = None
            executor_process = await self._reserve_executor(token)
            self._executor = executor_process
            self._executor_start_timestamp = dt.datetime.now()
