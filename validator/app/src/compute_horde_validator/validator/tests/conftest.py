import logging
from collections.abc import Generator

import bittensor
import pytest

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
