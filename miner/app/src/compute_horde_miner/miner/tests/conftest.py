import logging
from collections.abc import Generator

import bittensor
import pytest

logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def validator_wallet():
    wallet = bittensor.wallet(name="test_validator")
    wallet.regenerate_coldkey(
        mnemonic="local ghost evil lizard decade own lecture absurd vote despair predict cage",
        use_password=False,
        overwrite=True,
    )
    wallet.regenerate_hotkey(
        mnemonic="position chicken ugly key sugar expect another require cinnamon rubber rich veteran",
        use_password=False,
        overwrite=True,
    )
    return wallet


@pytest.fixture(scope="session", autouse=True)
def miner_wallet():
    wallet = bittensor.wallet(name="test_miner")
    wallet.regenerate_coldkey(
        mnemonic="marine oyster umbrella sail over speak emerge rude matrix glue argue learn",
        use_password=False,
        overwrite=True,
    )
    wallet.regenerate_hotkey(
        mnemonic="radio busy purpose chicken nose soccer private bridge nephew float ten media",
        use_password=False,
        overwrite=True,
    )
    return wallet


@pytest.fixture
def some() -> Generator[int, None, None]:
    # setup code
    yield 1
    # teardown code
