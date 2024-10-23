from collections.abc import Generator

import bittensor
import pytest


@pytest.fixture(scope="session")
def validator_wallet():
    wallet = bittensor.wallet(name="test_validator")
    # workaround the overwrite flag
    wallet.regenerate_coldkey(seed="0" * 64, use_password=False, overwrite=True)
    wallet.regenerate_hotkey(seed="1" * 64, use_password=False, overwrite=True)
    return wallet


@pytest.fixture(scope="function")
def miner_wallet(settings):
    wallet = bittensor.wallet(name="test_miner")
    # workaround the overwrite flag
    wallet.regenerate_coldkey(seed="2" * 64, use_password=False, overwrite=True)
    wallet.regenerate_hotkey(seed="3" * 64, use_password=False, overwrite=True)
    return wallet


@pytest.fixture
def some() -> Generator[int, None, None]:
    # setup code
    yield 1
    # teardown code
