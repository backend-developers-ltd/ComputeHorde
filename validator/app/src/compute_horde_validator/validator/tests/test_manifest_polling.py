import logging
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import TypedDict
from unittest.mock import MagicMock, patch

import pytest
from compute_horde.miner_client.organic import OrganicMinerClient
from compute_horde.protocol_messages import V0ExecutorManifestRequest
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
    get_manifests_from_miners,
)
from compute_horde_validator.validator.tests.helpers import patch_constance
from compute_horde_validator.validator.tests.transport import SimulationTransport

logger = logging.getLogger(__name__)


class MinerConfig(TypedDict):
    """Configuration for creating a test miner with transport."""

    hotkey: str
    address: str
    port: int
    manifest: dict[ExecutorClass, int]
    job_uuid: str


async def create_test_miners_with_transport(
    miner_configs: list[MinerConfig],
) -> tuple[list[Miner], dict[str, SimulationTransport]]:
    """
    Create test miners with transport setup.
    """
    miners = []
    transport_map = {}

    for config in miner_configs:
        miner = await Miner.objects.acreate(
            hotkey=config["hotkey"], address=config["address"], port=config["port"]
        )
        miners.append(miner)

        transport = SimulationTransport(f"sim_{config['hotkey']}")

        manifest_request = V0ExecutorManifestRequest(
            job_uuid=config["job_uuid"], manifest=config["manifest"]
        )

        await transport.add_message(manifest_request, send_before=1)

        transport_map[config["hotkey"]] = transport

    return miners, transport_map


def create_mock_init_function(transport_map: dict[str, SimulationTransport]):
    """Create a mock init function for OrganicMinerClient."""
    original_init = OrganicMinerClient.__init__

    def mock_init(
        self, miner_hotkey, miner_address, miner_port, job_uuid, my_keypair, transport=None
    ):
        simulation_transport = transport_map.get(miner_hotkey)
        original_init(
            self,
            miner_hotkey,
            miner_address,
            miner_port,
            job_uuid,
            my_keypair,
            transport=simulation_transport,
        )

    return mock_init


@asynccontextmanager
async def mock_organic_miner_client(transport_map: dict[str, SimulationTransport]):
    """Context manager for mocking OrganicMinerClient with transport map."""
    mock_init = create_mock_init_function(transport_map)
    with patch.object(OrganicMinerClient, "__init__", mock_init):
        yield


def assert_manifest_contains(manifest: dict, expected_items: dict[ExecutorClass, int]):
    """Assert that a manifest contains the expected executor class counts."""
    for executor_class, expected_count in expected_items.items():
        assert manifest[executor_class] == expected_count, (
            f"Expected {executor_class} to have count {expected_count}, got {manifest.get(executor_class)}"
        )


async def setup_single_miner_transport(
    miner: Miner, manifest: dict[ExecutorClass, int], job_uuid: str
) -> dict[str, SimulationTransport]:
    """Set up transport for a single miner with a specific manifest."""
    transport = SimulationTransport(f"sim_{miner.hotkey}")
    manifest_request = V0ExecutorManifestRequest(job_uuid=job_uuid, manifest=manifest)
    await transport.add_message(manifest_request, send_before=1)
    return {miner.hotkey: transport}


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


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_get_manifests_from_miners_with_mocked_transport():
    """
    Test get_manifests_from_miners by mocking the transport.
    """
    miner_configs = [
        {
            "hotkey": "test_miner_1",
            "address": "192.168.1.1",
            "port": 8080,
            "manifest": {
                ExecutorClass.always_on__gpu_24gb: 2,
                ExecutorClass.spin_up_4min__gpu_24gb: 1,
            },
            "job_uuid": "123",
        },
        {
            "hotkey": "test_miner_2",
            "address": "192.168.1.2",
            "port": 8080,
            "manifest": {
                ExecutorClass.always_on__llm__a6000: 3,
                ExecutorClass.always_on__gpu_24gb: 1,
            },
            "job_uuid": "456",
        },
        {
            "hotkey": "test_miner_3",
            "address": "192.168.1.3",
            "port": 8080,
            "manifest": {
                ExecutorClass.spin_up_4min__gpu_24gb: 2,
            },
            "job_uuid": "789",
        },
    ]

    miners, transport_map = await create_test_miners_with_transport(miner_configs)

    async with mock_organic_miner_client(transport_map):
        manifests_dict = await get_manifests_from_miners(miners, timeout=5)

    assert len(manifests_dict) == 3
    assert "test_miner_1" in manifests_dict
    assert "test_miner_2" in manifests_dict
    assert "test_miner_3" in manifests_dict

    assert_manifest_contains(
        manifests_dict["test_miner_1"],
        {ExecutorClass.always_on__gpu_24gb: 2, ExecutorClass.spin_up_4min__gpu_24gb: 1},
    )
    assert_manifest_contains(
        manifests_dict["test_miner_2"],
        {ExecutorClass.always_on__llm__a6000: 3, ExecutorClass.always_on__gpu_24gb: 1},
    )
    assert_manifest_contains(
        manifests_dict["test_miner_3"], {ExecutorClass.spin_up_4min__gpu_24gb: 2}
    )


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_get_manifests_from_miners_with_partial_failures():
    """
    Test get_manifests_from_miners with some miners failing to respond.
    """
    miner_configs = [
        {
            "hotkey": "test_miner_1",
            "address": "192.168.1.1",
            "port": 8080,
            "manifest": {
                ExecutorClass.always_on__gpu_24gb: 2,
            },
            "job_uuid": "123",
        },
        {
            "hotkey": "test_miner_2",
            "address": "192.168.1.2",
            "port": 8080,
            "manifest": {},  # Empty manifest - will fail
            "job_uuid": "456",
        },
        {
            "hotkey": "test_miner_3",
            "address": "192.168.1.3",
            "port": 8080,
            "manifest": {},  # Empty manifest - will fail
            "job_uuid": "789",
        },
    ]

    miners, transport_map = await create_test_miners_with_transport(miner_configs)

    transport_map.pop("test_miner_2", None)
    transport_map.pop("test_miner_3", None)

    async with mock_organic_miner_client(transport_map):
        manifests_dict = await get_manifests_from_miners(miners, timeout=5)

    assert len(manifests_dict) == 1
    assert "test_miner_1" in manifests_dict
    assert "test_miner_2" not in manifests_dict
    assert "test_miner_3" not in manifests_dict

    assert_manifest_contains(manifests_dict["test_miner_1"], {ExecutorClass.always_on__gpu_24gb: 2})


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_get_manifests_from_miners_multiple_calls():
    """
    Test that multiple calls to get_manifests_from_miners fetch the latest manifest from the miner each time.
    """
    miner = await Miner.objects.acreate(hotkey="test_miner_1", address="192.168.1.1", port=8080)

    manifest_v1 = {
        ExecutorClass.always_on__gpu_24gb: 1,
    }
    manifest_v2 = {
        ExecutorClass.always_on__gpu_24gb: 2,
        ExecutorClass.spin_up_4min__gpu_24gb: 3,
    }
    manifest_v3 = {
        ExecutorClass.always_on__gpu_24gb: 5,
        ExecutorClass.spin_up_4min__gpu_24gb: 7,
        ExecutorClass.always_on__llm__a6000: 2,
    }

    transport_map = await setup_single_miner_transport(miner, manifest_v1, "123")
    async with mock_organic_miner_client(transport_map):
        manifests_dict = await get_manifests_from_miners([miner], timeout=5)
        assert_manifest_contains(
            manifests_dict["test_miner_1"], {ExecutorClass.always_on__gpu_24gb: 1}
        )

    transport_map = await setup_single_miner_transport(miner, manifest_v2, "456")
    async with mock_organic_miner_client(transport_map):
        manifests_dict = await get_manifests_from_miners([miner], timeout=5)
        assert_manifest_contains(
            manifests_dict["test_miner_1"],
            {ExecutorClass.always_on__gpu_24gb: 2, ExecutorClass.spin_up_4min__gpu_24gb: 3},
        )

    transport_map = await setup_single_miner_transport(miner, manifest_v3, "789")
    async with mock_organic_miner_client(transport_map):
        manifests_dict = await get_manifests_from_miners([miner], timeout=5)
        assert_manifest_contains(
            manifests_dict["test_miner_1"],
            {
                ExecutorClass.always_on__gpu_24gb: 5,
                ExecutorClass.spin_up_4min__gpu_24gb: 7,
                ExecutorClass.always_on__llm__a6000: 2,
            },
        )
