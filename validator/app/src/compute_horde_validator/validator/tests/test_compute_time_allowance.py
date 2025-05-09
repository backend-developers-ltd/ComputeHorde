from datetime import timedelta
from unittest.mock import MagicMock, patch

import pytest
from django.conf import settings

from compute_horde_validator.validator.models import (
    ComputeTimeAllowance,
    Cycle,
    MetagraphSnapshot,
    Miner,
    MinerManifest,
    SyntheticJobBatch,
)
from compute_horde_validator.validator.tasks import set_compute_time_allowances
from compute_horde_validator.validator.tests.helpers import patch_constance


@pytest.fixture
def mock_get_cycle():
    with patch("compute_horde_validator.validator.tasks.get_cycle_containing_block") as mock:

        def side_effect(block, netuid):
            return range(0, 719) if block < 719 else range(719, 1441)

        mock.side_effect = side_effect
        yield mock


@pytest.fixture
def mock_latest_metagraph_snapshot():
    metagraph = MagicMock(spec=MetagraphSnapshot)
    metagraph.block = 720
    metagraph.get_stake_for_hotkey.return_value = 100.0
    metagraph.get_total_stake.return_value = 1000.0

    with patch("compute_horde_validator.validator.models.MetagraphSnapshot.get_latest") as mock:
        mock.return_value = metagraph
        yield metagraph


@patch("compute_horde_validator.validator.tasks.COMPUTE_TIME_OVERHEAD_SECONDS", 0)
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
@patch_constance({"BITTENSOR_NETUID": 1})
def test_set_compute_time_allowances_success(
    validator_keypair,
    mock_get_cycle,
    mock_latest_metagraph_snapshot,
):
    settings.BITTENSOR_NETUID = 1
    settings.BITTENSOR_APPROXIMATE_BLOCK_DURATION = timedelta(seconds=12)

    # setup db
    cycle = Cycle.objects.create(start=0, stop=719)
    batch = SyntheticJobBatch.objects.create(block=700, cycle=cycle)

    miner1 = Miner.objects.create(hotkey="miner1_hotkey")
    miner2 = Miner.objects.create(hotkey="miner2_hotkey")

    MinerManifest.objects.create(
        miner=miner1, batch=batch, executor_class="gpu.a100", online_executor_count=2
    )
    MinerManifest.objects.create(
        miner=miner2, batch=batch, executor_class="gpu.a100", online_executor_count=3
    )

    set_compute_time_allowances()

    # Check that allowances were created correctly
    allowances = ComputeTimeAllowance.objects.all().order_by("miner__hotkey")
    assert allowances.count() == 2

    # Stake proportion is 0.1 (100/1000)
    # Cycle duration is 360 blocks * 12 seconds = 4320 seconds

    assert allowances[0].miner.hotkey == "miner1_hotkey"
    assert allowances[0].cycle.start == 719
    assert allowances[0].initial_allowance == 2 * 4320 * 0.1
    assert allowances[0].remaining_allowance == 2 * 4320 * 0.1

    assert allowances[1].miner.hotkey == "miner2_hotkey"
    assert allowances[1].cycle.start == 719
    assert allowances[1].initial_allowance == 3 * 4320 * 0.1
    assert allowances[1].remaining_allowance == 3 * 4320 * 0.1
