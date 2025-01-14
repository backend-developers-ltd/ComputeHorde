import abc
import asyncio
import datetime as dt
import logging
import time
from typing import Any

from compute_horde.executor_class import (
    EXECUTOR_CLASS,
    MAX_EXECUTOR_TIMEOUT,
    ExecutorClass,
)

logger = logging.getLogger(__name__)


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
    RESERVATION_TIMEOUT = MAX_EXECUTOR_TIMEOUT
    POOL_CLEANUP_PERIOD = 10

    def __init__(self, manager, executor_class: ExecutorClass, executor_count: int):
        self.manager = manager
        self.executor_class = executor_class
        self._count = executor_count
        self._executors: list[ReservedExecutor] = []
        self._reservation_lock = asyncio.Lock()
        self._pool_cleanup_task = asyncio.create_task(self._pool_cleanup_loop())

    async def reserve_executor(self, token, timeout):
        start = time.time()
        async with self._reservation_lock:
            while True:
                if self.get_availability() == 0:
                    if time.time() - start < self.RESERVATION_TIMEOUT:
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
            await asyncio.sleep(self.POOL_CLEANUP_PERIOD)

    async def _pool_cleanup(self):
        async def check_executor(reserved_executor):
            status = await self.manager.wait_for_executor(reserved_executor.executor, 1)
            if status is not None:
                return reserved_executor, True
            elif reserved_executor.is_expired():
                await self.manager.kill_executor(reserved_executor.executor)
                return reserved_executor, True
            return reserved_executor, False

        results = await asyncio.gather(
            *[check_executor(reserved_executor) for reserved_executor in self._executors]
        )

        executors_to_drop = set(
            reserved_executor for reserved_executor, should_drop in results if should_drop
        )

        self._executors = [
            reserved_executor
            for reserved_executor in self._executors
            if reserved_executor not in executors_to_drop
        ]


class BaseExecutorManager(metaclass=abc.ABCMeta):
    EXECUTOR_TIMEOUT_LEEWAY = dt.timedelta(seconds=30).total_seconds()

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
    async def get_manifest(self) -> dict[ExecutorClass, int]:
        """Return executors manifest

        Keys are executor class ids and values are number of supported executors for given executor class.
        """

    async def is_active(self) -> bool:
        """Check if the Miner is an active one for configured Cluster"""
        return True

    async def get_executor_public_address(self, executor: Any) -> str | None:
        """To be given to clients to connect to streaming jobs"""
        return None

    async def _sync_pools_with_manifest(self):
        manifest = await self.get_manifest()
        for executor_class, executor_count in manifest.items():
            pool = self._executor_class_pools.get(executor_class)
            if pool is None:
                pool = ExecutorClassPool(self, executor_class, executor_count)
                self._executor_class_pools[executor_class] = pool
            else:
                pool.set_count(executor_count)

    async def get_executor_class_pool(self, executor_class):
        await self._sync_pools_with_manifest()
        return self._executor_class_pools[executor_class]

    async def reserve_executor_class(self, token, executor_class, timeout):
        pool = await self.get_executor_class_pool(executor_class)
        return await pool.reserve_executor(token, self.get_total_timeout(executor_class, timeout))

    def get_total_timeout(self, executor_class, job_timeout):
        spec = EXECUTOR_CLASS.get(executor_class)
        spin_up_time = 0
        if spec is not None and spec.spin_up_time is not None:
            spin_up_time = spec.spin_up_time
        return spin_up_time + job_timeout + self.EXECUTOR_TIMEOUT_LEEWAY
