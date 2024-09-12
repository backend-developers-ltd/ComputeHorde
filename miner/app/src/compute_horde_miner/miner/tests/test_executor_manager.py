import asyncio
from contextlib import asynccontextmanager
from unittest.mock import patch

import pytest
import pytest_asyncio
from compute_horde.executor_class import ExecutorClass

from compute_horde_miner.miner.executor_manager._internal.base import (
    BaseExecutorManager,
    ExecutorUnavailable,
)


class DummyExecutor:
    def __init__(self, execution_time):
        self.execution_time = execution_time
        self.task = None

    async def run(self):
        try:
            await asyncio.sleep(self.execution_time)
            return "Completed"
        except asyncio.CancelledError:
            return "Cancelled"


class DummyExecutorManager(BaseExecutorManager):
    def __init__(self, manifest, runtime_offset=0):
        super().__init__()
        self.manifest = manifest
        self.executors = []
        self.runtime_offset = runtime_offset
        self.EXECUTOR_TIMEOUT_LEEWAY = 0

    @asynccontextmanager
    async def set_runtime_offset(self, offset):
        old_offset = self.runtime_offset
        self.runtime_offset = offset
        try:
            yield
        finally:
            self.runtime_offset = old_offset

    async def start_new_executor(self, token, executor_class, timeout):
        executor = DummyExecutor(timeout + self.runtime_offset)
        executor.task = asyncio.create_task(executor.run())
        self.executors.append(executor)
        return executor

    async def kill_executor(self, executor):
        if executor.task and not executor.task.done():
            executor.task.cancel()
        if executor in self.executors:
            self.executors.remove(executor)

    async def wait_for_executor(self, executor, timeout):
        try:
            return await asyncio.wait_for(asyncio.shield(executor.task), timeout)
        except TimeoutError:
            return None

    async def get_manifest(self):
        return self.manifest


@pytest_asyncio.fixture
async def dummy_manager():
    manifest = {
        ExecutorClass.always_on__gpu_24gb: 2,
    }
    manager = DummyExecutorManager(manifest, runtime_offset=-2)
    yield manager
    for pool in manager._executor_class_pools.values():
        pool._pool_cleanup_task.cancel()
        try:
            await pool._pool_cleanup_task
        except asyncio.CancelledError:
            pass


@pytest.mark.asyncio
@patch(
    "compute_horde_miner.miner.executor_manager._internal.base.ExecutorClassPool.RESERVATION_TIMEOUT",
    0,
)
@patch(
    "compute_horde_miner.miner.executor_manager._internal.base.ExecutorClassPool.POOL_CLEANUP_PERIOD",
    0.1,
)
async def test_executor_class_pool(dummy_manager):
    # Test reserving executors
    pool = await dummy_manager.get_executor_class_pool(ExecutorClass.always_on__gpu_24gb)

    executor1 = await pool.reserve_executor("token1", 2)
    assert isinstance(executor1, DummyExecutor)
    assert pool.get_availability() == 1

    executor2 = await pool.reserve_executor("token2", 7)
    assert isinstance(executor2, DummyExecutor)
    assert pool.get_availability() == 0

    # Test ExecutorUnavailable exception
    with pytest.raises(ExecutorUnavailable):
        await pool.reserve_executor("token3", 20)

    # Test executor completion
    status = await dummy_manager.wait_for_executor(executor1, 1)
    assert status == "Completed"
    status = await dummy_manager.wait_for_executor(executor2, 1)
    assert status is None

    # Allow time for pool cleanup
    await asyncio.sleep(1)
    assert pool.get_availability() == 1

    # Test executor completion
    status = await dummy_manager.wait_for_executor(executor2, 7)
    assert status == "Completed"

    # Allow time for pool cleanup
    await asyncio.sleep(1)
    assert pool.get_availability() == 2

    # Test long-running executor
    async with dummy_manager.set_runtime_offset(5):
        long_running_executor = await pool.reserve_executor("token4", 5)

        # Wait a bit, but not long enough for the executor to complete
        status = await dummy_manager.wait_for_executor(long_running_executor, 2)
        assert status is None

        # Wait for the executor to be killed by the cleanup process
        status = await dummy_manager.wait_for_executor(long_running_executor, 10)
        assert status == "Cancelled"

    # Allow time for pool cleanup
    await asyncio.sleep(1)
    assert pool.get_availability() == 2


@pytest.mark.asyncio
@patch(
    "compute_horde_miner.miner.executor_manager._internal.base.ExecutorClassPool.RESERVATION_TIMEOUT",
    0,
)
@patch(
    "compute_horde_miner.miner.executor_manager._internal.base.ExecutorClassPool.POOL_CLEANUP_PERIOD",
    0.1,
)
async def test_manager_reserve_executor_class(dummy_manager):
    await dummy_manager.reserve_executor_class("token1", ExecutorClass.always_on__gpu_24gb, 10)
    assert (
        dummy_manager._executor_class_pools[ExecutorClass.always_on__gpu_24gb].get_availability()
        == 1
    )

    await dummy_manager.reserve_executor_class("token2", ExecutorClass.always_on__gpu_24gb, 10)
    assert (
        dummy_manager._executor_class_pools[ExecutorClass.always_on__gpu_24gb].get_availability()
        == 0
    )

    with pytest.raises(ExecutorUnavailable):
        await dummy_manager.reserve_executor_class("token3", ExecutorClass.always_on__gpu_24gb, 10)


@pytest.mark.asyncio
@patch(
    "compute_horde_miner.miner.executor_manager._internal.base.ExecutorClassPool.RESERVATION_TIMEOUT",
    0,
)
@patch(
    "compute_horde_miner.miner.executor_manager._internal.base.ExecutorClassPool.POOL_CLEANUP_PERIOD",
    0.1,
)
async def test_manifest_update(dummy_manager):
    pool = await dummy_manager.get_executor_class_pool(ExecutorClass.always_on__gpu_24gb)
    assert pool._count == 2

    # Update manifest
    dummy_manager.manifest = {ExecutorClass.always_on__gpu_24gb: 3}

    # Get pool again to trigger update
    pool = await dummy_manager.get_executor_class_pool(ExecutorClass.always_on__gpu_24gb)
    assert pool._count == 3


@pytest.mark.asyncio
@patch(
    "compute_horde_miner.miner.executor_manager._internal.base.ExecutorClassPool.RESERVATION_TIMEOUT",
    0,
)
@patch(
    "compute_horde_miner.miner.executor_manager._internal.base.ExecutorClassPool.POOL_CLEANUP_PERIOD",
    0.1,
)
async def test_concurrent_reservations(dummy_manager):
    async def reserve(i):
        try:
            await dummy_manager.reserve_executor_class(
                f"token{i}", ExecutorClass.always_on__gpu_24gb, 5
            )
            return True
        except ExecutorUnavailable:
            return False

    results = await asyncio.gather(*[reserve(i) for i in range(5)])
    assert results.count(True) == 2
    assert results.count(False) == 3
