import os
import pathlib

import bittensor

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
BITTENSOR_WALLET_NAME = "test_miner_miner"
BITTENSOR_WALLET_HOTKEY_NAME = "default"


def BITTENSOR_WALLET() -> bittensor.wallet:  # type: ignore
    if not BITTENSOR_WALLET_NAME or not BITTENSOR_WALLET_HOTKEY_NAME:
        raise RuntimeError("Wallet not configured")
    wallet = bittensor.wallet(
        name=BITTENSOR_WALLET_NAME,
        hotkey=BITTENSOR_WALLET_HOTKEY_NAME,
        path=str(BITTENSOR_WALLET_DIRECTORY),
    )
    wallet.hotkey_file.get_keypair()  # this raises errors if the keys are inaccessible
    return wallet


CONSTANCE_DATABASE_CACHE_BACKEND = None
