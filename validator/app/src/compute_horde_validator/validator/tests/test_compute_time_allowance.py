import logging
from datetime import timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from compute_horde_validator.validator.models import (
    ComputeTimeAllowance,
    Cycle,
    MetagraphSnapshot,
    Miner,
    MinerManifest,
    SyntheticJobBatch,
    SystemEvent,
)
from compute_horde_validator.validator.tasks import (
    _set_compute_time_allowances,
    set_compute_time_allowances,
)
from compute_horde_validator.validator.tests.helpers import patch_constance

logger = logging.getLogger(__name__)


@pytest.fixture(autouse=True)
def common_test_setup():
    with (
        patch("compute_horde_validator.validator.tasks.COMPUTE_TIME_OVERHEAD_SECONDS", 0),
        patch("compute_horde_validator.validator.tasks.MIN_VALIDATOR_STAKE", 1),
        patch_constance({"BITTENSOR_NETUID": 1}),
        patch_constance({"BITTENSOR_APPROXIMATE_BLOCK_DURATION": timedelta(seconds=12)}),
    ):
        yield


async def mock_get_manifests_from_miners(miners: list[Miner], timeout: int = 5) -> dict[str, dict]:
    manifests = {}
    for i, miner in enumerate(miners):
        manifests[miner.hotkey] = {
            "gpu.a900": (i + 1) * 5,
            "gpu.t42": i * 5,
        }
    return manifests


def setup_db():
    cycle = Cycle.objects.create(start=708, stop=1430)
    metagraph = MagicMock(spec=MetagraphSnapshot)
    metagraph.block = 720
    return cycle, metagraph


@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
def test_set_allowances_skip():
    cycle, metagraph = setup_db()
    cycle.set_compute_time_allowance = True
    cycle.save()

    with patch(
        "compute_horde_validator.validator.tasks.MetagraphSnapshot.get_cycle_start",
        return_value=metagraph,
    ):
        set_compute_time_allowances()

    cycle.refresh_from_db()
    assert cycle.set_compute_time_allowance is True
    assert ComputeTimeAllowance.objects.count() == 0


@patch(
    "compute_horde_validator.validator.tasks.get_manifests_from_miners",
    mock_get_manifests_from_miners,
)
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
def test_set_allowances_success():
    cycle, metagraph = setup_db()
    with (
        patch(
            "compute_horde_validator.validator.tasks.MetagraphSnapshot.get_cycle_start",
            return_value=metagraph,
        ),
        patch(
            "compute_horde_validator.validator.tasks._set_compute_time_allowances",
            AsyncMock(return_value=True),
        ),
    ):
        set_compute_time_allowances()

    cycle.refresh_from_db()
    assert cycle.set_compute_time_allowance is True


@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
def test_set_allowances_failure():
    cycle, metagraph = setup_db()
    with (
        patch(
            "compute_horde_validator.validator.tasks.MetagraphSnapshot.get_cycle_start",
            return_value=metagraph,
        ),
        patch(
            "compute_horde_validator.validator.tasks._set_compute_time_allowances",
            AsyncMock(return_value=False),
        ),
    ):
        set_compute_time_allowances()

    cycle.refresh_from_db()
    assert cycle.set_compute_time_allowance is False
    assert SystemEvent.objects.count() == 0


@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
def test_set_allowances_exception():
    cycle, metagraph = setup_db()
    with (
        patch(
            "compute_horde_validator.validator.tasks.MetagraphSnapshot.get_cycle_start",
            return_value=metagraph,
        ),
        patch(
            "compute_horde_validator.validator.tasks._set_compute_time_allowances",
            side_effect=Exception("Test exception"),
        ),
    ):
        set_compute_time_allowances()

    cycle.refresh_from_db()
    assert cycle.set_compute_time_allowance is False
    assert SystemEvent.objects.count() == 1
    system_event = SystemEvent.objects.first()
    assert system_event.subtype == SystemEvent.EventSubType.GENERIC_ERROR


async def asetup_db(num_miners: int = 3, num_validators: int = 2):
    cycle = await Cycle.objects.acreate(start=708, stop=1430)
    batch = await SyntheticJobBatch.objects.acreate(block=725, cycle=cycle)

    miner_hotkeys = [f"miner{i}" for i in range(num_miners)]
    validator_hotkeys = [f"validator{i}" for i in range(num_validators)]
    miners = await Miner.objects.abulk_create(
        [Miner(hotkey=hotkey) for hotkey in miner_hotkeys + validator_hotkeys]
    )
    await MinerManifest.objects.abulk_create(
        [
            MinerManifest(
                miner=miner,
                batch=batch,
                executor_class="gpu.will_be_ignored",
                online_executor_count=0,
            )
            for miner in miners
        ]
    )

    metagraph = MagicMock(spec=MetagraphSnapshot)
    metagraph.block = 720
    metagraph.get_total_validator_stake.return_value = 1000.0
    metagraph.get_serving_hotkeys.return_value = miner_hotkeys
    metagraph.hotkeys = miner_hotkeys + validator_hotkeys
    metagraph.stake = [0.0] * len(miner_hotkeys) + [
        (i + 1) * 10 for i in range(len(validator_hotkeys))
    ]
    return cycle, metagraph


@patch(
    "compute_horde_validator.validator.tasks.get_manifests_from_miners",
    mock_get_manifests_from_miners,
)
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
@pytest.mark.asyncio
async def test_set_compute_time_allowances_success():
    num_miners, num_validators = 5, 3
    cycle, metagraph = await asetup_db(num_miners, num_validators)
    logger.info(f"Cycle: {cycle}, Metagraph: {metagraph.hotkeys}")

    is_set = await _set_compute_time_allowances(metagraph, cycle)
    assert is_set is True

    allowances = [cta async for cta in ComputeTimeAllowance.objects.order_by("initial_allowance")]
    assert len(allowances) == num_miners * num_validators
    assert allowances[0].initial_allowance == 432.0
    assert allowances[1].initial_allowance == 864.0
    assert allowances[2].initial_allowance == 1296.0

    assert await SystemEvent.objects.acount() == 1
    system_event = await SystemEvent.objects.aget()
    assert system_event.type == SystemEvent.EventType.COMPUTE_TIME_ALLOWANCE
    assert system_event.subtype == SystemEvent.EventSubType.SUCCESS
    assert system_event.data == {
        "cycle_start": cycle.start,
        "cycle_stop": cycle.stop,
        "total_allowance_records_created": num_miners * num_validators,
        "validator_stake_proportion": {
            "validator0": 0.01,
            "validator1": 0.02,
            "validator2": 0.03,
        },
    }


@patch(
    "compute_horde_validator.validator.tasks.get_manifests_from_miners",
    mock_get_manifests_from_miners,
)
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
@pytest.mark.asyncio
async def test_set_compute_time_allowances_failure_no_serving_hotkeys():
    cycle, metagraph = await asetup_db()
    metagraph.get_serving_hotkeys.return_value = []

    is_set = await _set_compute_time_allowances(metagraph, cycle)
    assert is_set is False
    assert await ComputeTimeAllowance.objects.acount() == 0
    assert await SystemEvent.objects.acount() == 1
    system_event = await SystemEvent.objects.aget()
    assert system_event.subtype == SystemEvent.EventSubType.GIVING_UP


@patch(
    "compute_horde_validator.validator.tasks.get_manifests_from_miners",
    AsyncMock(return_value={}),
)
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
@pytest.mark.asyncio
async def test_set_compute_time_allowances_failure_no_manifests():
    cycle, metagraph = await asetup_db()

    is_set = await _set_compute_time_allowances(metagraph, cycle)
    assert is_set is False
    assert await ComputeTimeAllowance.objects.acount() == 0
    assert await SystemEvent.objects.acount() == 1
    system_event = await SystemEvent.objects.aget()
    assert system_event.subtype == SystemEvent.EventSubType.GIVING_UP
