import logging
import uuid
from collections.abc import Generator
from unittest.mock import patch

import bittensor
import pytest
from compute_horde.executor_class import EXECUTOR_CLASS, ExecutorClass
from pytest_mock import MockerFixture

from .helpers import MockNeuron, MockSyntheticMinerClient

logger = logging.getLogger(__name__)


@pytest.fixture
def some() -> Generator[int, None, None]:
    # setup code
    yield 1
    # teardown code


@pytest.fixture(autouse=True)
def _patch_get_streaming_job_executor_classes(mocker: MockerFixture):
    mocker.patch(
        "compute_horde_validator.validator.synthetic_jobs.batch_run.get_streaming_job_executor_classes",
        return_value={},
    )


@pytest.fixture(scope="session", autouse=True)
def wallet():
    wallet = bittensor.wallet(name="test_validator")
    try:
        # workaround the overwrite flag
        wallet.regenerate_coldkey(seed="a" * 64, use_password=False, overwrite=True)
        wallet.regenerate_hotkey(seed="b" * 64, use_password=False, overwrite=True)
    except Exception as e:
        logger.error(f"Failed to create wallet: {e}")


@pytest.fixture
def validator_keypair():
    return bittensor.Keypair.create_from_mnemonic(
        "slot excuse valid grief praise rifle spoil auction weasel glove pen share"
    )

@pytest.fixture
def miner_keypair():
    return bittensor.Keypair.create_from_mnemonic(
        "almost fatigue race slim picnic mass better clog deal solve already champion"
    )

@pytest.fixture(scope="function", autouse=True)
def clear_cache():
    yield
    from django.core.cache import cache

    cache.clear()


@pytest.fixture
def override_weights_version_v2(settings):
    settings.DEBUG_OVERRIDE_WEIGHTS_VERSION = 2


@pytest.fixture
def override_weights_version_v1(settings):
    settings.DEBUG_OVERRIDE_WEIGHTS_VERSION = 1


@pytest.fixture
def mocked_synthetic_miner_client():
    with patch(
        "compute_horde_validator.validator.synthetic_jobs.batch_run.MinerClient"
    ) as MockedMinerClient:
        MockedMinerClient.instance = None

        def side_effect(*args, **kwargs):
            if MockedMinerClient.instance is not None:
                raise RuntimeError("You can create only single instance of mocked MinerClient")
            MockedMinerClient.instance = MockSyntheticMinerClient(*args, **kwargs)
            return MockedMinerClient.instance

        MockedMinerClient.side_effect = side_effect
        yield MockedMinerClient


@pytest.fixture
def small_spin_up_times(monkeypatch):
    monkeypatch.setattr(EXECUTOR_CLASS[ExecutorClass.spin_up_4min__gpu_24gb], "spin_up_time", 4)


@pytest.fixture
def validators():
    return [MockNeuron(hotkey=f"mock_validator_hotkey_{i}", uid=i * 3) for i in range(10)]


@pytest.fixture
def validators_with_this_hotkey(settings, validators):
    this_hotkey = settings.BITTENSOR_WALLET().get_hotkey().ss58_address
    return [*validators, MockNeuron(hotkey=this_hotkey, uid=17)]


@pytest.fixture(scope="session")
def run_uuid():
    return str(uuid.uuid4())
