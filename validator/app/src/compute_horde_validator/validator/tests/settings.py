import os
import pathlib

import bittensor

os.environ.update(
    {
        "DEBUG_TOOLBAR": "False",
    }
)

from compute_horde_validator.settings import *  # noqa: E402,F403

PROMETHEUS_EXPORT_MIGRATIONS = False


BITTENSOR_NETUID = 12
BITTENSOR_NETWORK = "local"

BITTENSOR_WALLET_DIRECTORY = pathlib.Path("~").expanduser() / ".bittensor" / "wallets"
BITTENSOR_WALLET_NAME = "test_validator"
BITTENSOR_WALLET_HOTKEY_NAME = "default"

STATS_COLLECTOR_URL = "http://fakehost:8000"
MANIFEST_INCENTIVE_MULTIPLIER = 1.05


def BITTENSOR_WALLET() -> bittensor.wallet:
    if not BITTENSOR_WALLET_NAME or not BITTENSOR_WALLET_HOTKEY_NAME:
        raise RuntimeError("Wallet not configured")
    wallet = bittensor.wallet(
        name=BITTENSOR_WALLET_NAME,
        hotkey=BITTENSOR_WALLET_HOTKEY_NAME,
        path=str(BITTENSOR_WALLET_DIRECTORY),
    )
    wallet.hotkey_file.get_keypair()  # this raises errors if the keys are inaccessible
    return wallet
