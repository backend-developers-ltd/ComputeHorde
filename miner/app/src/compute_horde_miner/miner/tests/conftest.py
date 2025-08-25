import logging
from collections.abc import Generator

import pytest
from compute_horde.test_wallet import (
    get_test_miner_wallet,
    get_test_validator_wallet,
)

logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def validator_wallet():
    return get_test_validator_wallet()


@pytest.fixture(scope="session", autouse=True)
def miner_wallet():
    return get_test_miner_wallet()


@pytest.fixture
def some() -> Generator[int, None, None]:
    # setup code
    yield 1
    # teardown code
