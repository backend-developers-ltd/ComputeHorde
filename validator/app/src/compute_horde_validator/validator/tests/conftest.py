import logging
from collections.abc import Generator
from unittest.mock import patch

import bittensor
import pytest
from compute_horde.executor_class import EXECUTOR_CLASS, ExecutorClass

from .helpers import MockSyntheticMinerClient

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
