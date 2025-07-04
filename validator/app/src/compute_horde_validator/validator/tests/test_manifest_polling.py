import logging
from datetime import timedelta
from unittest.mock import MagicMock, patch

import pytest
from compute_horde_core.executor_class import ExecutorClass

from compute_horde_validator.validator.models import (
    ComputeTimeAllowance,
    Cycle,
    MetagraphSnapshot,
    Miner,
    MinerManifest,
)
from compute_horde_validator.validator.tasks import (
    _get_latest_manifests,
    _poll_miner_manifests,
    _set_compute_time_allowances,
)
from compute_horde_validator.validator.tests.helpers import patch_constance

logger = logging.getLogger(__name__)


@pytest.fixture(autouse=True)
def common_test_setup():
    with patch_constance({"BITTENSOR_NETUID": 1}):
        yield


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_get_latest_manifests_returns_latest_per_executor_class():
    """Test that _get_latest_manifests returns the latest manifest for each executor class."""
    miner = await Miner.objects.acreate(hotkey="test_miner")

    await MinerManifest.objects.acreate(
        miner=miner,
        batch=None,
        executor_class=ExecutorClass.spin_up_4min__gpu_24gb,
        executor_count=5,
        online_executor_count=3,
    )
    await MinerManifest.objects.acreate(
        miner=miner,
        batch=None,
        executor_class=ExecutorClass.spin_up_4min__gpu_24gb,
        executor_count=5,
        online_executor_count=4,
    )
    await MinerManifest.objects.acreate(
        miner=miner,
        batch=None,
        executor_class=ExecutorClass.spin_up_4min__gpu_24gb,
        executor_count=6,
        online_executor_count=2,
    )

    await MinerManifest.objects.acreate(
        miner=miner,
        batch=None,
        executor_class=ExecutorClass.always_on__gpu_24gb,
        executor_count=3,
        online_executor_count=2,
    )
    await MinerManifest.objects.acreate(
        miner=miner,
        batch=None,
        executor_class=ExecutorClass.always_on__gpu_24gb,
        executor_count=1,
        online_executor_count=0,
    )

    await MinerManifest.objects.acreate(
        miner=miner,
        batch=None,
        executor_class=ExecutorClass.always_on__llm__a6000,
        executor_count=2,
        online_executor_count=1,
    )
    await MinerManifest.objects.acreate(
        miner=miner,
        batch=None,
        executor_class=ExecutorClass.always_on__llm__a6000,
        executor_count=3,
        online_executor_count=5,
    )

    manifests_dict = await _get_latest_manifests([miner])

    assert miner.hotkey in manifests_dict
    manifest = manifests_dict[miner.hotkey]

    assert manifest[ExecutorClass.spin_up_4min__gpu_24gb] == 2
    assert manifest[ExecutorClass.always_on__gpu_24gb] == 0
    assert manifest[ExecutorClass.always_on__llm__a6000] == 5


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_get_latest_manifests_multiple_miners():
    """Test that _get_latest_manifests works correctly with multiple miners."""
    miner1 = await Miner.objects.acreate(hotkey="test_miner_1")
    miner2 = await Miner.objects.acreate(hotkey="test_miner_2")

    await MinerManifest.objects.acreate(
        miner=miner1,
        batch=None,
        executor_class=ExecutorClass.spin_up_4min__gpu_24gb,
        executor_count=3,
        online_executor_count=2,
    )

    await MinerManifest.objects.acreate(
        miner=miner2,
        batch=None,
        executor_class=ExecutorClass.always_on__gpu_24gb,
        executor_count=2,
        online_executor_count=1,
    )

    await MinerManifest.objects.acreate(
        miner=miner1,
        batch=None,
        executor_class=ExecutorClass.spin_up_4min__gpu_24gb,
        executor_count=3,
        online_executor_count=3,
    )

    await MinerManifest.objects.acreate(
        miner=miner2,
        batch=None,
        executor_class=ExecutorClass.always_on__gpu_24gb,
        executor_count=2,
        online_executor_count=2,
    )

    manifests_dict = await _get_latest_manifests([miner1, miner2])

    assert len(manifests_dict) == 2
    assert "test_miner_1" in manifests_dict
    assert "test_miner_2" in manifests_dict

    # Check miner1's latest manifests
    miner1_manifest = manifests_dict["test_miner_1"]
    assert miner1_manifest[ExecutorClass.spin_up_4min__gpu_24gb] == 3

    # Check miner2's latest manifests
    miner2_manifest = manifests_dict["test_miner_2"]
    assert miner2_manifest[ExecutorClass.always_on__gpu_24gb] == 2


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_get_latest_manifests_no_miners():
    """Test that _get_latest_manifests handles empty miner list correctly."""
    manifests_dict = await _get_latest_manifests([])
    assert manifests_dict == {}


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_get_latest_manifests_miner_without_manifests():
    """Test that _get_latest_manifests handles miners without any manifests."""
    miner = await Miner.objects.acreate(hotkey="test_miner_no_manifests")

    manifests_dict = await _get_latest_manifests([miner])
    assert manifests_dict == {}


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_get_latest_manifests():
    """Test that _get_latest_manifests handles manifests with different timestamps."""
    miner = await Miner.objects.acreate(hotkey="test_miner_different_time")

    await MinerManifest.objects.acreate(
        miner=miner,
        batch=None,
        executor_class=ExecutorClass.spin_up_4min__gpu_24gb,
        executor_count=3,
        online_executor_count=2,
    )
    await MinerManifest.objects.acreate(
        miner=miner,
        batch=None,
        executor_class=ExecutorClass.always_on__gpu_24gb,
        executor_count=2,
        online_executor_count=1,
    )

    await MinerManifest.objects.acreate(
        miner=miner,
        batch=None,
        executor_class=ExecutorClass.spin_up_4min__gpu_24gb,
        executor_count=4,
        online_executor_count=5,
    )

    manifests_dict = await _get_latest_manifests([miner])

    assert miner.hotkey in manifests_dict
    manifest = manifests_dict[miner.hotkey]

    assert manifest[ExecutorClass.spin_up_4min__gpu_24gb] == 5
    assert manifest[ExecutorClass.always_on__gpu_24gb] == 1


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_poll_miner_manifests_creates_periodic_manifests():
    """Test that manifest polling creates periodic manifests (batch=None) from miner data."""
    miner = await Miner.objects.acreate(hotkey="test_miner")

    await MetagraphSnapshot.objects.acreate(
        id=MetagraphSnapshot.SnapshotType.LATEST,
        block=100,
        alpha_stake=[100.0],
        tao_stake=[100.0],
        stake=[100.0],
        uids=[1],
        hotkeys=["test_miner"],
        serving_hotkeys=["test_miner"],
    )

    mock_manifests = {
        "test_miner": {
            ExecutorClass.spin_up_4min__gpu_24gb: 3,
            ExecutorClass.always_on__gpu_24gb: 2,
        }
    }

    with patch(
        "compute_horde_validator.validator.tasks.get_manifests_from_miners",
        return_value=mock_manifests,
    ):
        await _poll_miner_manifests()

    manifests = [m async for m in MinerManifest.objects.filter(miner=miner, batch__isnull=True)]
    assert len(manifests) == 2

    manifest_dict = {m.executor_class: m.online_executor_count for m in manifests}
    assert manifest_dict[str(ExecutorClass.spin_up_4min__gpu_24gb)] == 3
    assert manifest_dict[str(ExecutorClass.always_on__gpu_24gb)] == 2


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_compute_time_allowances_use_polled_manifests():
    """Test that compute time allowances use manifests from polling instead of direct miner requests."""
    cycle = await Cycle.objects.acreate(start=100, stop=200)
    miner = await Miner.objects.acreate(hotkey="test_miner")
    validator = await Miner.objects.acreate(hotkey="test_validator")

    await MinerManifest.objects.acreate(
        miner=miner,
        batch=None,
        executor_class=ExecutorClass.spin_up_4min__gpu_24gb,
        executor_count=5,
        online_executor_count=3,
    )

    metagraph = MagicMock()
    metagraph.get_total_validator_stake.return_value = 1000.0
    metagraph.get_serving_hotkeys.return_value = ["test_miner"]
    metagraph.hotkeys = ["test_miner", "test_validator"]
    metagraph.stake = [0.0, 1000.0]

    with (
        patch("compute_horde_validator.validator.tasks.COMPUTE_TIME_OVERHEAD_SECONDS", 0),
        patch("compute_horde_validator.validator.tasks.MIN_VALIDATOR_STAKE", 1),
        patch_constance({"BITTENSOR_NETUID": 1}),
        patch_constance({"BITTENSOR_APPROXIMATE_BLOCK_DURATION": timedelta(seconds=12)}),
    ):
        is_set = await _set_compute_time_allowances(metagraph, cycle)
        assert is_set is True

        allowances = [cta async for cta in ComputeTimeAllowance.objects.all()]
        assert len(allowances) == 1

        allowance = allowances[0]
        allowance_miner = await Miner.objects.aget(pk=allowance.miner_id)
        allowance_validator = await Miner.objects.aget(pk=allowance.validator_id)
        allowance_cycle = await Cycle.objects.aget(pk=allowance.cycle_id)
        assert allowance_miner == miner
        assert allowance_validator == validator
        assert allowance_cycle == cycle
