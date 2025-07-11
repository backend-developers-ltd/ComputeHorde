import logging
from contextlib import asynccontextmanager
from typing import TypedDict
from unittest.mock import patch

import pytest
from compute_horde.miner_client.organic import OrganicMinerClient
from compute_horde.protocol_messages import V0ExecutorManifestRequest
from compute_horde_core.executor_class import ExecutorClass

from compute_horde_validator.validator.models import (
    MetagraphSnapshot,
    Miner,
    MinerManifest,
)
from compute_horde_validator.validator.tasks import (
    _get_latest_manifests,
    _poll_miner_manifests,
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
async def test_get_manifests_from_miners():
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
async def test_poll_miner_manifests_with_partial_failures():
    """
    Test _poll_miner_manifests with some miners failing to respond.
    """
    miner1 = await Miner.objects.acreate(hotkey="test_miner_1", address="192.168.1.1", port=8080)
    miner2 = await Miner.objects.acreate(hotkey="test_miner_2", address="192.168.1.2", port=8080)
    miner3 = await Miner.objects.acreate(hotkey="test_miner_3", address="192.168.1.3", port=8080)

    await MetagraphSnapshot.objects.acreate(
        id=MetagraphSnapshot.SnapshotType.LATEST,
        block=100,
        alpha_stake=[100.0, 100.0, 100.0],
        tao_stake=[100.0, 100.0, 100.0],
        stake=[100.0, 100.0, 100.0],
        uids=[1, 2, 3],
        hotkeys=["test_miner_1", "test_miner_2", "test_miner_3"],
        serving_hotkeys=["test_miner_1", "test_miner_2", "test_miner_3"],
    )

    transport_map = {}

    transport1 = SimulationTransport("sim_test_miner_1")
    manifest1 = V0ExecutorManifestRequest(
        job_uuid="job_1",
        manifest={
            ExecutorClass.always_on__gpu_24gb: 2,
        },
    )
    await transport1.add_message(manifest1, send_before=1)
    transport_map["test_miner_1"] = transport1

    async with mock_organic_miner_client(transport_map):
        await _poll_miner_manifests()

    manifests_dict = await _get_latest_manifests([miner1, miner2, miner3])

    assert len(manifests_dict) == 1
    assert "test_miner_1" in manifests_dict
    assert "test_miner_2" not in manifests_dict
    assert "test_miner_3" not in manifests_dict

    miner1_manifest = manifests_dict["test_miner_1"]
    assert miner1_manifest[ExecutorClass.always_on__gpu_24gb] == 2


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_poll_miner_manifests_multiple_calls():
    """
    Test that multiple calls fetch the latest manifest from the miner each time.
    """
    miner = await Miner.objects.acreate(hotkey="test_miner_1", address="192.168.1.1", port=8080)

    await MetagraphSnapshot.objects.acreate(
        id=MetagraphSnapshot.SnapshotType.LATEST,
        block=100,
        alpha_stake=[100.0],
        tao_stake=[100.0],
        stake=[100.0],
        uids=[1],
        hotkeys=["test_miner_1"],
        serving_hotkeys=["test_miner_1"],
    )

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
        await _poll_miner_manifests()

    manifests_dict = await _get_latest_manifests([miner])
    assert_manifest_contains(manifests_dict["test_miner_1"], {ExecutorClass.always_on__gpu_24gb: 1})

    transport_map = await setup_single_miner_transport(miner, manifest_v2, "456")
    async with mock_organic_miner_client(transport_map):
        await _poll_miner_manifests()

    manifests_dict = await _get_latest_manifests([miner])
    assert_manifest_contains(
        manifests_dict["test_miner_1"],
        {ExecutorClass.always_on__gpu_24gb: 2, ExecutorClass.spin_up_4min__gpu_24gb: 3},
    )

    transport_map = await setup_single_miner_transport(miner, manifest_v3, "789")
    async with mock_organic_miner_client(transport_map):
        await _poll_miner_manifests()

    manifests_dict = await _get_latest_manifests([miner])
    assert_manifest_contains(
        manifests_dict["test_miner_1"],
        {
            ExecutorClass.always_on__gpu_24gb: 5,
            ExecutorClass.spin_up_4min__gpu_24gb: 7,
            ExecutorClass.always_on__llm__a6000: 2,
        },
    )


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_poll_miner_manifests_with_partial_transport_failures():
    """
    Test _poll_miner_manifests when some miners don't respond via transport.
    """
    miner1 = await Miner.objects.acreate(hotkey="test_miner_1", address="192.168.1.1", port=8080)
    miner2 = await Miner.objects.acreate(hotkey="test_miner_2", address="192.168.1.2", port=8080)

    await MinerManifest.objects.acreate(
        miner=miner2,
        batch=None,
        executor_class=ExecutorClass.always_on__gpu_24gb,
        executor_count=3,
        online_executor_count=2,
    )
    await MinerManifest.objects.acreate(
        miner=miner2,
        batch=None,
        executor_class=ExecutorClass.spin_up_4min__gpu_24gb,
        executor_count=1,
        online_executor_count=1,
    )

    await MetagraphSnapshot.objects.acreate(
        id=MetagraphSnapshot.SnapshotType.LATEST,
        block=100,
        alpha_stake=[100.0, 100.0],
        tao_stake=[100.0, 100.0],
        stake=[100.0, 100.0],
        uids=[1, 2],
        hotkeys=["test_miner_1", "test_miner_2"],
        serving_hotkeys=["test_miner_1", "test_miner_2"],
    )

    transport_map = {}

    transport1 = SimulationTransport("sim_test_miner_1")
    manifest1 = V0ExecutorManifestRequest(
        job_uuid="job_1",
        manifest={
            ExecutorClass.always_on__gpu_24gb: 2,
        },
    )
    await transport1.add_message(manifest1, send_before=1)
    transport_map["test_miner_1"] = transport1

    async with mock_organic_miner_client(transport_map):
        await _poll_miner_manifests()

    manifests_dict = await _get_latest_manifests([miner1, miner2])

    assert len(manifests_dict) == 2
    assert "test_miner_1" in manifests_dict
    assert "test_miner_2" in manifests_dict

    miner1_manifest = manifests_dict["test_miner_1"]
    assert miner1_manifest[ExecutorClass.always_on__gpu_24gb] == 2

    miner2_manifest = manifests_dict["test_miner_2"]
    assert miner2_manifest[ExecutorClass.always_on__gpu_24gb] == 0
    assert miner2_manifest[ExecutorClass.spin_up_4min__gpu_24gb] == 0
