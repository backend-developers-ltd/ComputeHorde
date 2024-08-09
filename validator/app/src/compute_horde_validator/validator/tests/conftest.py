import logging
from collections.abc import Generator
from unittest.mock import patch

import bittensor
import pytest

from .helpers import MockMinerClient, MockNeuron

logger = logging.getLogger(__name__)


@pytest.fixture
def some() -> Generator[int, None, None]:
    # setup code
    yield 1
    # teardown code


@pytest.fixture(scope="session", autouse=True)
def wallet():
    wallet = bittensor.wallet(name="test_validator")
    try:
        # workaround the overwrite flag
        wallet.create_new_coldkey(n_words=12, use_password=False, overwrite=True)
        wallet.create_new_hotkey(n_words=12, use_password=False, overwrite=True)
    except Exception as e:
        logger.error(f"Failed to create wallet: {e}")


@pytest.fixture
def override_weights_version_v2(settings):
    settings.DEBUG_OVERRIDE_WEIGHTS_VERSION = 2


@pytest.fixture
def override_weights_version_v1(settings):
    settings.DEBUG_OVERRIDE_WEIGHTS_VERSION = 1


@pytest.fixture
def mocked_synthetic_miner_client():
    with patch(
        "compute_horde_validator.validator.synthetic_jobs.utils.MinerClient"
    ) as MockedMinerClient:
        MockedMinerClient.instance = None

        def side_effect(*args, **kwargs):
            if MockedMinerClient.instance is not None:
                raise RuntimeError("You can create only single instance of mocked MinerClient")
            MockedMinerClient.instance = MockMinerClient(*args, **kwargs)
            return MockedMinerClient.instance

        MockedMinerClient.side_effect = side_effect
        yield MockedMinerClient


@pytest.fixture
def validators():
    return [MockNeuron(hotkey=f"mock_validator_hotkey_{i}", uid=i) for i in range(10)]


@pytest.fixture
def validators_with_this_hotkey(settings, validators):
    this_hotkey = settings.BITTENSOR_WALLET().get_hotkey().ss58_address
    return [*validators, MockNeuron(hotkey=this_hotkey, uid=100)]
