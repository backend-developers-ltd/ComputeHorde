import asyncio
from contextlib import asynccontextmanager
from unittest.mock import patch

import pytest
import pytest_asyncio
from compute_horde.subtensor import REFERENCE_BLOCK_IN_PEAK_CYCLE
from compute_horde_core.executor_class import ExecutorClass

from compute_horde_miner.miner.executor_manager._internal.base import (
    AllExecutorsBusy,
    BaseExecutorManager,
)
from compute_horde_miner.miner.executor_manager.base import (
    BaseExecutorManager as BaseBaseExecutorManager,
)
from compute_horde_miner.miner.executor_manager.v0 import (
    BaseExecutorManager as V0BaseExecutorManager,
)
from compute_horde_miner.miner.executor_manager.v0 import (
    DockerExecutorManager as V0DockerExecutorManager,
)
from compute_horde_miner.miner.executor_manager.v1 import (
    BaseExecutorManager as V1BaseExecutorManager,
)
from compute_horde_miner.miner.executor_manager.v1 import (
    DockerExecutorManager as V1DockerExecutorManager,
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
    "compute_horde_miner.miner.executor_manager._internal.base.ExecutorClassPool.POOL_CLEANUP_PERIOD",
    0.1,
)
@patch("compute_horde_miner.miner.executor_manager._internal.base.MAX_EXECUTOR_TIMEOUT", 5)
async def test_executor_class_pool(dummy_manager):
    # Test reserving executors
    pool = await dummy_manager.get_executor_class_pool(ExecutorClass.always_on__gpu_24gb)

    executor1 = await pool.reserve_executor("token1", 2)
    assert isinstance(executor1, DummyExecutor)
    assert pool.get_availability() == 1

    executor2 = await pool.reserve_executor("token2", 7)
    assert isinstance(executor2, DummyExecutor)
    assert pool.get_availability() == 0

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
    "compute_horde_miner.miner.executor_manager._internal.base.ExecutorClassPool.POOL_CLEANUP_PERIOD",
    0.1,
)
async def test_manager_reserve_executor_class(dummy_manager):
    await dummy_manager.reserve_executor_class("token1", ExecutorClass.always_on__gpu_24gb, 10)
    assert (
        await dummy_manager.get_executor_class_pool(ExecutorClass.always_on__gpu_24gb)
    ).get_availability() == 1

    await dummy_manager.reserve_executor_class("token2", ExecutorClass.always_on__gpu_24gb, 10)
    assert (
        await dummy_manager.get_executor_class_pool(ExecutorClass.always_on__gpu_24gb)
    ).get_availability() == 0

    with pytest.raises(AllExecutorsBusy):
        await dummy_manager.reserve_executor_class("token3", ExecutorClass.always_on__gpu_24gb, 10)


@pytest.mark.asyncio
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
        except AllExecutorsBusy:
            return False

    async with dummy_manager.set_runtime_offset(0):
        results = await asyncio.gather(*[reserve(i) for i in range(5)])
    assert results.count(True) == 2
    assert results.count(False) == 3


@pytest.mark.parametrize(
    "base_class,overrides",
    [
        (
            BaseBaseExecutorManager,
            ["get_manifest", "_kill_executor", "_reserve_executor", "_wait_for_executor"],
        ),
        (
            V0BaseExecutorManager,
            ["get_manifest", "_kill_executor", "_reserve_executor", "_wait_for_executor"],
        ),
        (
            V1BaseExecutorManager,
            ["get_manifest", "kill_executor", "start_new_executor", "wait_for_executor"],
        ),
        (V0DockerExecutorManager, []),
        (V1DockerExecutorManager, []),
    ],
)
def test_subclassing_executor_manager(base_class: type, overrides: list[str]) -> None:
    """
    This is a dead-simple test that should stop us from adding abstract methods to public(-ish) executor managers, in
    which case miners' custom executor managers would blow up.
    This is by no means an exhaustive compatibility check.
    """

    async def async_noop(self):
        pass

    DerivedManager = type(
        "DerivedManager",
        (base_class,),
        {method: async_noop for method in overrides},
    )

    instance = DerivedManager()  # noqa


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("current_block", "expected"),
    [
        # peak cycle
        (REFERENCE_BLOCK_IN_PEAK_CYCLE, True),
        # following non-peak cycles, 722 = cycle length
        (REFERENCE_BLOCK_IN_PEAK_CYCLE + 722, False),
        (REFERENCE_BLOCK_IN_PEAK_CYCLE + 722 * 2, False),
        (REFERENCE_BLOCK_IN_PEAK_CYCLE + 722 * 3, False),
        (REFERENCE_BLOCK_IN_PEAK_CYCLE + 722 * 4, False),
        (REFERENCE_BLOCK_IN_PEAK_CYCLE + 722 * 5, False),
        (REFERENCE_BLOCK_IN_PEAK_CYCLE + 722 * 6, False),
        (REFERENCE_BLOCK_IN_PEAK_CYCLE + 722 * 7, False),
        (REFERENCE_BLOCK_IN_PEAK_CYCLE + 722 * 8, False),
        (REFERENCE_BLOCK_IN_PEAK_CYCLE + 722 * 9, False),
        # next peak cycle
        (REFERENCE_BLOCK_IN_PEAK_CYCLE + 722 * 10, True),
    ],
)
async def test_executor_manager_peak(current_block, expected, dummy_manager):
    with patch("bittensor.Subtensor") as mock_subtensor:
        mock_subtensor.return_value.get_current_block.return_value = current_block

        is_peak = await dummy_manager.is_peak()
        assert is_peak is expected
