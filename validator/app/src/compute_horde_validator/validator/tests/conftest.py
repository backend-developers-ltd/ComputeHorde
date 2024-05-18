from collections.abc import Generator

import bittensor
import pytest


@pytest.fixture(scope='session', autouse=True)
def wallet():
    bittensor.wallet().create(coldkey_use_password=False, hotkey_use_password=False)
