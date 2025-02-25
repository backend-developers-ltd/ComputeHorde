from __future__ import annotations

import os
from unittest.mock import patch

import bittensor
import pytest
from freezegun import freeze_time

pytest.register_assert_rewrite("test.unit")


@pytest.fixture
def homedir(tmp_path_factory):
    yield tmp_path_factory.mktemp("test_homedir")


@pytest.fixture
def env(homedir, monkeypatch):
    """Get ENV for running b2 command from shell level."""
    monkeypatch.setenv("HOME", str(homedir))
    monkeypatch.delenv("XDG_CONFIG_HOME", raising=False)
    monkeypatch.setenv("SHELL", "/bin/bash")  # fix for running under github actions
    if "TERM" not in os.environ:
        monkeypatch.setenv("TERM", "xterm")
    yield os.environ


@pytest.fixture
def bittensor_keypair():
    return bittensor.Keypair.create_from_seed(b"test" * 8)


@pytest.fixture
def bittensor_wallet(env, bittensor_keypair):
    wallet = bittensor.wallet(name="ch_facilitator_test_wallet")
    wallet.set_hotkey(bittensor_keypair)
    return wallet


@pytest.fixture
def sleep_mock():
    with freeze_time() as frozen_time, patch("time.sleep") as sleep_mock:

        def tick(seconds):
            frozen_time.tick(delta=seconds)

        sleep_mock.side_effect = tick
        yield sleep_mock


@pytest.fixture
def async_sleep_mock():
    with freeze_time() as frozen_time, patch("asyncio.sleep") as sleep_mock:

        async def tick(seconds):
            frozen_time.tick(delta=seconds)

        sleep_mock.side_effect = tick
        yield sleep_mock
