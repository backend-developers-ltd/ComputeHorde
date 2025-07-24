import abc
import asyncio
import datetime as dt
import logging
from typing import Any

from asgiref.sync import sync_to_async
from compute_horde.executor_class import (
    MAX_EXECUTOR_TIMEOUT,
)
from compute_horde_core.executor_class import ExecutorClass
from django.conf import settings

from compute_horde_miner.miner.dynamic_config import aget_config
from compute_horde_miner.miner.executor_manager._internal.selector import (
    HistoricalRandomMinerSelector,
)

logger = logging.getLogger(__name__)


class AllExecutorsBusy(Exception):
    pass


class ExecutorUnavailable(Exception):
    """
    Thrown when an executor that should be available, but for some reason fails to spin up.
    """

    pass


class ReservedExecutor:
    def __init__(self, executor, timeout, token):
        self.executor = executor
        self.timeout = timeout
        self.start_time = dt.datetime.now()
        self.token = token

    def is_expired(self):
        return (dt.datetime.now() - self.start_time).total_seconds() > min(
            MAX_EXECUTOR_TIMEOUT, self.timeout
        )

    def __str__(self):
        return f"ReservedExecutor(start_time={self.start_time}, timeout={self.timeout}, token={self.token})"


class ExecutorClassPool:
    POOL_CLEANUP_PERIOD = 10

    def __init__(self, manager, executor_class: ExecutorClass, executor_count: int):
        self.manager = manager
        self.executor_class = executor_class
        self._count = executor_count
        self._executors: list[ReservedExecutor] = []
        self._reservation_lock = asyncio.Lock()
        self._pool_cleanup_task = asyncio.create_task(self._pool_cleanup_loop())
        self._reservation_futures: dict[str, asyncio.Future[None]] = {}

    def _reservation_future(self, token: str) -> asyncio.Future[None]:
        if token not in self._reservation_futures:
            self._reservation_futures[token] = asyncio.Future()

            # Clean up eventually.
            async def clear():
                await asyncio.sleep(10)
                del self._reservation_futures[token]

            asyncio.create_task(clear())
        return self._reservation_futures[token]

    async def reserve_executor(self, token: str, timeout: float) -> object:
        async with self._reservation_lock:
            if self.get_availability() == 0:
                logger.debug(
                    "No executor available, current list is:\n %s",
                    "\n".join(str(r) for r in self._executors),
                )
                self._reservation_future(token).set_exception(AllExecutorsBusy())
                raise AllExecutorsBusy()

            # Reservation succeeded - resolve the future to let the listeners know.
            self._reservation_future(token).set_result(None)

            try:
                executor = await self.manager.start_new_executor(
                    token, self.executor_class, timeout
                )
            except Exception as exc:
                logger.error("Error during executor startup", exc_info=exc)
                raise ExecutorUnavailable()

            # This "timeout" is the executor's upper TTL, after which it will be killed by the pool cleanup.
            # TODO: TIMEOUTS - this should depend on various time limits - job, executor spinup etc. with some margin.
            reserved_executor = ReservedExecutor(executor, MAX_EXECUTOR_TIMEOUT, token)
            self._executors.append(reserved_executor)
            logger.debug("Added %s", reserved_executor)
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
                logger.error("Error during pool cleanup", exc_info=exc)
            await asyncio.sleep(self.POOL_CLEANUP_PERIOD)

    async def _pool_cleanup(self):
        async def check_executor(reserved_executor):
            status = await self.manager.wait_for_executor(reserved_executor.executor, 1)
            if status is not None:
                logger.debug("%s finished", reserved_executor)
                return reserved_executor, True
            elif reserved_executor.is_expired():
                logger.debug("%s timed out, killing it.", reserved_executor)
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

    async def wait_for_executor_reservation(self, token: str) -> None:
        await self._reservation_future(token)


class BaseExecutorManager(metaclass=abc.ABCMeta):
    def __init__(self):
        self._executor_class_pools: dict[ExecutorClass, ExecutorClassPool] = {}
        self.selector = HistoricalRandomMinerSelector(settings.CLUSTER_SECRET)

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

    async def get_executor_class_pool(self, executor_class: ExecutorClass) -> ExecutorClassPool:
        await self._sync_pools_with_manifest()
        return self._executor_class_pools[executor_class]

    async def reserve_executor_class(
        self, token: str, executor_class: ExecutorClass, timeout: float
    ) -> object:
        pool = await self.get_executor_class_pool(executor_class)
        return await pool.reserve_executor(token, timeout)

    async def wait_for_executor_reservation(
        self, token: str, executor_class: ExecutorClass
    ) -> None:
        """
        Resolves as soon as the executor is reserved - before it's launched.
        If there are no free executors, raises AllExecutorsBusy.
        """
        pool = await self.get_executor_class_pool(executor_class)
        await pool.wait_for_executor_reservation(token)

    async def get_executor_cmdline_args(self) -> list[str]:
        """
        Arguments passed in to the executor's `manage.py run_executor` command.
        """
        return [
            "--startup-time-limit",
            str(await aget_config("DYNAMIC_EXECUTOR_STARTUP_TIME_LIMIT")),
        ]

    async def is_active(self) -> bool:
        """Check if the Miner is an active one for configured Cluster"""
        selected = await self.selector.active(
            settings.CLUSTER_HOTKEYS,
        )
        my_address = settings.BITTENSOR_WALLET().hotkey.ss58_address  # type: str

        return selected == my_address

    @sync_to_async(thread_sensitive=False)
    def is_peak(self) -> bool:
        import bittensor
        from compute_horde.subtensor import get_peak_cycle

        subtensor = bittensor.Subtensor(network=settings.BITTENSOR_NETWORK)
        current_block = subtensor.get_current_block()
        peak_cycle = get_peak_cycle(current_block, settings.BITTENSOR_NETUID)
        return current_block in peak_cycle

    async def get_split_distribution(self) -> dict[str, float] | None:
        """
        Get the split distribution for decoupled dancing.
        By default, returns the miner's own hotkey with 100% distribution.
        Miners can override this to implement split exposure.
        
        Returns:
            Dictionary mapping hotkeys to percentages, or None if no split
        """
        # Get the miner's own hotkey
        my_hotkey = settings.BITTENSOR_WALLET().hotkey.ss58_address
        return {my_hotkey: 1.0}
