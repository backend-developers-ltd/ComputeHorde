import logging
from datetime import timedelta
from unittest.mock import AsyncMock, patch

import pytest
from compute_horde_core.executor_class import ExecutorClass
from pylon_common.models import AxonInfo, Metagraph

from compute_horde_validator.validator.models import (
    ComputeTimeAllowance,
    Cycle,
    Miner,
    MinerManifest,
    SyntheticJobBatch,
    SystemEvent,
)
from compute_horde_validator.validator.tasks import (
    _set_compute_time_allowances,
    set_compute_time_allowances,
)
from compute_horde_validator.validator.tests.helpers import mock_pylon_neuron, patch_constance

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
    metagraph_data = {"block": 720, "block_hash": "0x123", "neurons": {}}
    metagraph = Metagraph(**metagraph_data)
    return cycle, metagraph


@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
def test_set_allowances_skip(mock_pylon_client):
    cycle, metagraph = setup_db()
    cycle.set_compute_time_allowance = True
    cycle.save()

    with patch(
        "compute_horde_validator.validator.tasks.pylon_client",
        return_value=mock_pylon_client,
    ):
        mock_pylon_client.override("get_metagraph", metagraph)
        set_compute_time_allowances()

    cycle.refresh_from_db()
    assert cycle.set_compute_time_allowance is True
    assert ComputeTimeAllowance.objects.count() == 0


@patch(
    "compute_horde_validator.validator.tasks.get_manifests_from_miners",
    mock_get_manifests_from_miners,
)
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
def test_set_allowances_success(mock_pylon_client):
    cycle, metagraph = setup_db()
    with (
        patch(
            "compute_horde_validator.validator.tasks.pylon_client",
            return_value=mock_pylon_client,
        ),
        patch(
            "compute_horde_validator.validator.tasks._set_compute_time_allowances",
            AsyncMock(return_value=True),
        ),
    ):
        mock_pylon_client.override("get_metagraph", metagraph)
        set_compute_time_allowances()

    cycle.refresh_from_db()
    assert cycle.set_compute_time_allowance is True


@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
def test_set_allowances_failure(mock_pylon_client):
    cycle, metagraph = setup_db()
    with (
        patch(
            "compute_horde_validator.validator.tasks.pylon_client",
            return_value=mock_pylon_client,
        ),
        patch(
            "compute_horde_validator.validator.tasks._set_compute_time_allowances",
            AsyncMock(return_value=False),
        ),
    ):
        mock_pylon_client.override("get_metagraph", metagraph)
        set_compute_time_allowances()

    cycle.refresh_from_db()
    assert cycle.set_compute_time_allowance is False
    assert SystemEvent.objects.count() == 0


@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
def test_set_allowances_exception(mock_pylon_client):
    cycle, metagraph = setup_db()
    with (
        patch(
            "compute_horde_validator.validator.tasks.pylon_client",
            return_value=mock_pylon_client,
        ),
        patch(
            "compute_horde_validator.validator.tasks._set_compute_time_allowances",
            side_effect=Exception("Test exception"),
        ),
    ):
        mock_pylon_client.override("get_metagraph", metagraph)
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
    # Create manifests for each miner
    manifest_objects = []
    for i, miner in enumerate(miners):
        manifest_objects.extend(
            [
                MinerManifest(
                    miner=miner,
                    batch=batch,
                    executor_class=ExecutorClass.spin_up_4min__gpu_24gb,
                    online_executor_count=(i + 1) * 5,
                ),
                MinerManifest(
                    miner=miner,
                    batch=batch,
                    executor_class=ExecutorClass.always_on__llm__a6000,
                    online_executor_count=i * 5,
                ),
            ]
        )

    await MinerManifest.objects.abulk_create(manifest_objects)

    # Create proper Metagraph with neurons
    neurons = {}

    # Create miner neurons (with valid axon_info for serving)
    for i, hotkey in enumerate(miner_hotkeys):
        neurons[hotkey] = mock_pylon_neuron(i, hotkey, 800.0, True)

    # Create validator neurons (high stake, no axon_info)
    for i, hotkey in enumerate(validator_hotkeys):
        neurons[hotkey] = mock_pylon_neuron(i + len(miner_hotkeys), hotkey, (i + 1) * 1000, False)

    metagraph_data = {"block": 720, "block_hash": "0x123", "neurons": neurons}
    metagraph = Metagraph(**metagraph_data)
    return cycle, metagraph


@patch(
    "compute_horde_validator.validator.tasks.get_manifests_from_miners",
    mock_get_manifests_from_miners,
)
@patch("compute_horde_validator.validator.tasks.MIN_VALIDATOR_STAKE", 1000.0)
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
@pytest.mark.asyncio
async def test_set_compute_time_allowances_success(mock_pylon_client):
    num_miners, num_validators = 5, 3
    cycle, metagraph = await asetup_db(num_miners, num_validators)
    logger.info(
        f"Cycle: {cycle}, Metagraph block: {metagraph.block}, neurons: {len(metagraph.neurons)}"
    )

    with patch(
        "compute_horde_validator.validator.tasks.pylon_client", return_value=mock_pylon_client
    ):
        mock_pylon_client.override("get_metagraph", metagraph)
        is_set = await _set_compute_time_allowances(metagraph, cycle)
        assert is_set is True

    allowance_values = [
        cta.initial_allowance
        async for cta in ComputeTimeAllowance.objects.order_by("initial_allowance")
    ]
    assert len(allowance_values) == num_miners * num_validators
    assert allowance_values[0] == 4320.0
    assert allowance_values[1] == 8640.0
    assert allowance_values[2] == 12960.0

    assert await SystemEvent.objects.acount() == 1
    system_event = await SystemEvent.objects.aget()
    assert system_event.type == SystemEvent.EventType.COMPUTE_TIME_ALLOWANCE
    assert system_event.subtype == SystemEvent.EventSubType.SUCCESS
    assert system_event.data == {
        "cycle_start": cycle.start,
        "cycle_stop": cycle.stop,
        "total_allowance_records_created": num_miners * num_validators,
        "validator_stake_proportion": {
            "validator0": 0.1,
            "validator1": 0.2,
            "validator2": 0.3,
        },
    }


@patch(
    "compute_horde_validator.validator.tasks.get_manifests_from_miners",
    mock_get_manifests_from_miners,
)
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
@pytest.mark.asyncio
async def test_set_compute_time_allowances_failure_no_serving_hotkeys(mock_pylon_client):
    cycle, _ = await asetup_db()
    # Create metagraph with no serving miners (all have 0.0.0.0 IPs)
    neurons = {
        "validator0": {
            "uid": 0,
            "hotkey": "validator0",
            "coldkey": "coldkey_val_0",
            "stake": 10,
            "axon_info": AxonInfo(ip="0.0.0.0", port=0, protocol=4),
            "active": True,
            "rank": 0.1,
            "emission": 0.01,
            "incentive": 0.1,
            "consensus": 0.9,
            "trust": 0.9,
            "validator_trust": 0.9,
            "dividends": 0.01,
            "last_update": 12340,
            "validator_permit": True,
            "pruning_score": 0,
        }
    }
    metagraph = Metagraph(block=720, block_hash="0x123", neurons=neurons)

    with patch(
        "compute_horde_validator.validator.tasks.pylon_client", return_value=mock_pylon_client
    ):
        mock_pylon_client.override("get_metagraph", metagraph)
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
async def test_set_compute_time_allowances_failure_no_manifests(mock_pylon_client):
    cycle, metagraph = await asetup_db(num_miners=0)

    with patch(
        "compute_horde_validator.validator.tasks.pylon_client", return_value=mock_pylon_client
    ):
        mock_pylon_client.override("get_metagraph", metagraph)
        is_set = await _set_compute_time_allowances(metagraph, cycle)
    assert is_set is False
    assert await ComputeTimeAllowance.objects.acount() == 0
    assert await SystemEvent.objects.acount() == 1
    system_event = await SystemEvent.objects.aget()
    assert system_event.subtype == SystemEvent.EventSubType.GIVING_UP
