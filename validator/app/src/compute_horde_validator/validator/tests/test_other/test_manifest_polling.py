import logging
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import TypedDict
from unittest.mock import patch

import pytest
from compute_horde.protocol_messages import V0ExecutorManifestRequest
from compute_horde_core.executor_class import ExecutorClass
from django.utils import timezone

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


def create_mock_http_session(miner_configs: list[MinerConfig]):
    """Create a mock HTTP session for testing."""
    class MockResponse:
        def __init__(self, status, data):
            self.status = status
            self._data = data
        
        async def json(self):
            return self._data
        
        async def __aenter__(self):
            return self
        
        async def __aexit__(self, exc_type, exc_val, exc_tb):
            return None
    
    class MockSession:
        def __init__(self, miner_configs):
            self.miner_configs = miner_configs
        
        async def get(self, url):
            # Get the miner config based on the URL
            # URL format: http://{address}:{port}/v0.1/manifest
            address, port = url.split("://")[1].split("/")[0].split(":")

            # Find the hotkey for this address by looking up in neurons
            target_manifest = None
            for config in self.miner_configs:
                if config["address"] == address and str(config["port"])== port:
                    target_manifest = config["manifest"]
                    break

            if target_manifest:
                # Return successful response
                return MockResponse(200, {"manifest": target_manifest})
            else:
                # Return error response for unknown miners
                return MockResponse(404, {"error": "Miner not found"})
        
        async def __aenter__(self):
            return self
        
        async def __aexit__(self, exc_type, exc_val, exc_tb):
            return None
    
    return MockSession(miner_configs)


@asynccontextmanager
async def mock_organic_miner_client(miner_configs: list[MinerConfig]):
    """Context manager for mocking the HTTP views for miners."""
    mock_session = create_mock_http_session(miner_configs)
    with patch("aiohttp.ClientSession", return_value=mock_session):
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
    Test get_manifests_from_miners by mocking the HTTP endpoint.
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

    # Add miners to the database
    miners = []
    for config in miner_configs:
        miner = await Miner.objects.acreate(
            hotkey=config["hotkey"], address=config["address"], port=config["port"],
        )
        miners.append(miner)

    async with mock_organic_miner_client(miner_configs):
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

    # Add mock manifest for miner1
    miner_configs = [
        {
            "hotkey": "test_miner_1",
            "address": "192.168.1.1",
            "port": 8080,
            "manifest": {ExecutorClass.always_on__gpu_24gb: 2},
            "job_uuid": "123",
        },
    ]

    async with mock_organic_miner_client(miner_configs):
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

    # Poll with first manifest
    miner_config = [
        {
            "hotkey": "test_miner_1",
            "address": "192.168.1.1",
            "port": 8080,
            "manifest": manifest_v1,
            "job_uuid": "123",
        },
    ]
    async with mock_organic_miner_client(miner_config):
        await _poll_miner_manifests()

    manifests_dict = await _get_latest_manifests([miner])
    assert_manifest_contains(manifests_dict["test_miner_1"], {ExecutorClass.always_on__gpu_24gb: 1})

    # Poll with second manifest
    miner_config[0]["manifest"] = manifest_v2
    miner_config[0]["job_uuid"] = "456"
    async with mock_organic_miner_client(miner_config):
        await _poll_miner_manifests()

    manifests_dict = await _get_latest_manifests([miner])
    assert_manifest_contains(
        manifests_dict["test_miner_1"],
        {ExecutorClass.always_on__gpu_24gb: 2, ExecutorClass.spin_up_4min__gpu_24gb: 3},
    )

    # Poll with third manifest
    miner_config[0]["manifest"] = manifest_v3
    miner_config[0]["job_uuid"] = "789"
    async with mock_organic_miner_client(miner_config):
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

    # Add miner manifest via HTTP view
    # The missing entry for test_miner_2 will result in a non-200 response and 
    # be interpreted by _poll_miner_manifests as a "transport failure", e.g. 
    # an unresponsive/offline miner.
    miner_config = [
        {
            "hotkey": "test_miner_1",
            "address": "192.168.1.1",
            "port": 8080,
            "manifest": {ExecutorClass.always_on__gpu_24gb: 2},
            "job_uuid": "job_1",
        },
    ]
    async with mock_organic_miner_client(miner_config):
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


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_poll_miner_manifests_no_duplicate_offline_records():
    """
    Test that when miners don't respond, only one record per executor class is created.
    """
    miner = await Miner.objects.acreate(hotkey="test_miner_1", address="192.168.1.1", port=8080)

    await MinerManifest.objects.acreate(
        miner=miner,
        batch=None,
        executor_class=ExecutorClass.always_on__gpu_24gb,
        executor_count=1,
        online_executor_count=1,
        created_at=timezone.now() - timedelta(minutes=10),
    )
    await MinerManifest.objects.acreate(
        miner=miner,
        batch=None,
        executor_class=ExecutorClass.always_on__gpu_24gb,
        executor_count=2,
        online_executor_count=2,
        created_at=timezone.now() - timedelta(minutes=5),
    )
    await MinerManifest.objects.acreate(
        miner=miner,
        batch=None,
        executor_class=ExecutorClass.always_on__gpu_24gb,
        executor_count=3,
        online_executor_count=3,
        created_at=timezone.now() - timedelta(minutes=1),
    )

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

    initial_count = await MinerManifest.objects.filter(miner=miner).acount()

    async with mock_organic_miner_client([]):
        await _poll_miner_manifests()

    final_count = await MinerManifest.objects.filter(miner=miner).acount()

    expected_new_records = 1
    actual_new_records = final_count - initial_count

    assert actual_new_records == expected_new_records, (
        f"Expected {expected_new_records} new record, got {actual_new_records}. "
        f"Initial count: {initial_count}, final count: {final_count}"
    )

    manifests_dict = await _get_latest_manifests([miner])

    assert len(manifests_dict) == 1
    assert "test_miner_1" in manifests_dict

    miner_manifest = manifests_dict["test_miner_1"]
    assert ExecutorClass.always_on__gpu_24gb in miner_manifest
    assert miner_manifest[ExecutorClass.always_on__gpu_24gb] == 0, (
        f"Expected online_executor_count=0, got {miner_manifest[ExecutorClass.always_on__gpu_24gb]}"
    )
