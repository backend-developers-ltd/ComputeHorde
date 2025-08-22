import os
import pathlib

import bittensor_wallet
from compute_horde.test_wallet import (
    MINER_WALLET_HOTKEY,
    MINER_WALLET_NAME,
    get_miner_wallet,
)

os.environ.update(
    {
        "DEBUG_TOOLBAR": "False",
    }
)

from compute_horde_miner.settings import *  # noqa: E402,F403

PROMETHEUS_EXPORT_MIGRATIONS = False


EXECUTOR_MANAGER_CLASS_PATH = "compute_horde_miner.miner.tests.executor_manager:StubExecutorManager"
DEBUG_TURN_AUTHENTICATION_OFF = True

BITTENSOR_NETUID = 49
BITTENSOR_NETWORK = "test"

BITTENSOR_WALLET_DIRECTORY = pathlib.Path("~").expanduser() / ".bittensor" / "wallets"
BITTENSOR_WALLET_NAME = MINER_WALLET_NAME
BITTENSOR_WALLET_HOTKEY_NAME = MINER_WALLET_HOTKEY


def BITTENSOR_WALLET() -> bittensor_wallet.Wallet:
    return get_miner_wallet()


CONSTANCE_DATABASE_CACHE_BACKEND = None
