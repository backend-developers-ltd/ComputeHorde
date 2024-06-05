import abc
import asyncio
import datetime as dt
import logging
import time

logger = logging.getLogger(__name__)


# this leaves around 1 min for synthetic job to complete
MAX_EXECUTOR_TIMEOUT = dt.timedelta(minutes=4).total_seconds()
DEFAULT_EXECUTOR_CLASS = 0
DEFAULT_EXECUTOR_TIMEOUT = dt.timedelta(minutes=4).total_seconds()


class ExecutorUnavailable(Exception):
    pass


class ReservedExecutor:
    def __init__(self, executor, timeout):
        self.executor = executor
        self.timeout = timeout
        self.start_time = dt.datetime.now()

    def is_expired(self):
        return (dt.datetime.now() - self.start_time).total_seconds() > min(
            MAX_EXECUTOR_TIMEOUT, self.timeout
        )


class ExecutorClassPool:
    def __init__(self, manager, executor_class: int, executor_count: int):
        self.manager = manager
        self.executor_class = executor_class
        self._count = executor_count
        self._executors = []
        self._reservation_lock = asyncio.Lock()
        self._pool_cleanup_task = asyncio.create_task(self._pool_cleanup_loop())

    async def reserve_executor(self, token, timeout):
        start = time.time()
        async with self._reservation_lock:
            while True:
                if self.get_availability() == 0:
                    if time.time() - start < MAX_EXECUTOR_TIMEOUT:
                        await asyncio.sleep(1)
                    else:
                        logger.warning("Error unavailable after timeout")
                        raise ExecutorUnavailable()
                else:
                    try:
                        executor = await self.manager.start_new_executor(
                            token, self.executor_class, timeout
                        )
                    except Exception as exc:
                        logger.error("Error occurred", exc_info=exc)
                        raise ExecutorUnavailable()
                    self._executors.append(ReservedExecutor(executor, timeout))
                    return executor

    def set_count(self, executor_count):
        self._count = executor_count

    def get_availability(self):
        return max(0, self._count - len(self._executors))

    async def _pool_cleanup_loop(self):
        # TODO: this is a basic working logic - pool cleanup should be more robust
        while True:
            try:
                await self._pool_cleanup()
            except Exception as exc:
                logger.error("Error occurred", exc_info=exc)
            await asyncio.sleep(10)

    async def _pool_cleanup(self):
        executors_to_drop = set()
        for reserved_executor in self._executors:
            status = await self.manager.wait_for_executor(reserved_executor.executor, 1)
            if status is not None:
                executors_to_drop.add(reserved_executor)
            else:
                if reserved_executor.is_expired():
                    await self.manager.kill_executor(reserved_executor.executor)
                    executors_to_drop.add(reserved_executor)
        still_running_executors = []
        for reserved_executor in self._executors:
            if reserved_executor not in executors_to_drop:
                still_running_executors.append(reserved_executor)
        self._executors = still_running_executors


class BaseExecutorManager(metaclass=abc.ABCMeta):
    def __init__(self):
        self._executor_class_pools = {}

    @abc.abstractmethod
    async def start_new_executor(self, token, executor_class, timeout):
        """Start spinning up an executor with `token` for given executor_class or raise ExecutorUnavailable if at capacity

        `timeout` is provided so manager does not need to relay on pool cleanup to stop expired executor"""

    @abc.abstractmethod
    async def kill_executor(self, executor):
        """Kill running executor. It might be platform specific, so leave it to Manager implementation"""

    @abc.abstractmethod
    async def wait_for_executor(self, executor, timeout):
        """Wait for executor to finish the job for till timeout.

        Have to return not None status if executor finished. If returned status is None it means that
        executor is still running.
        """

    @abc.abstractmethod
    async def get_manifest(self) -> dict[int, int]:
        """Return executors manifest

        Keys are executor class ids and values are number of supported executors for given executor class.
        """

    async def get_executor_class_pool(self, executor_class):
        manifest = await self.get_manifest()
        for executor_class, executor_count in manifest.items():
            pool = self._executor_class_pools.get(executor_class)
            if pool is None:
                pool = ExecutorClassPool(self, executor_class, executor_count)
                self._executor_class_pools[executor_class] = pool
            else:
                pool.set_count(executor_count)
        return self._executor_class_pools[executor_class]

    async def reserve_executor(self, token):
        # TODO: deprecated - new code should use `reserve_executor_class`
        await self.reserve_executor_class(token, DEFAULT_EXECUTOR_CLASS, DEFAULT_EXECUTOR_TIMEOUT)

    async def reserve_executor_class(self, token, executor_class, timeout):
        pool = await self.get_executor_class_pool(executor_class)
        await pool.reserve_executor(token, timeout)
