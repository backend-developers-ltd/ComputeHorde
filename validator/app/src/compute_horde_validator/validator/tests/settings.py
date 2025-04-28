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

CELERY_TASK_ALWAYS_EAGER = True

BITTENSOR_WALLET_DIRECTORY = pathlib.Path("~").expanduser() / ".bittensor" / "wallets"
BITTENSOR_WALLET_NAME = "test_validator"
BITTENSOR_WALLET_HOTKEY_NAME = "default"

BITTENSOR_EVM_RPC_URL = "http://fakehost:8545"
COLLATERAL_CONTRACT_ADDRESS = "0x0000000000000000000000000000000000000000"

STATS_COLLECTOR_URL = "http://fakehost:8000"


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


AWS_ACCESS_KEY_ID = "fake_access_key_id"
AWS_SECRET_ACCESS_KEY = "fake_secret_access_key"

S3_BUCKET_NAME_PROMPTS = "fake_bucket_prompts"
S3_BUCKET_NAME_ANSWERS = "fake_bucket_answers"

TRUSTED_MINER_ADDRESS = "fakehost"
TRUSTED_MINER_PORT = 1234

CACHES = {"default": {"BACKEND": "django.core.cache.backends.locmem.LocMemCache"}}
CONSTANCE_DATABASE_CACHE_BACKEND = None

DEBUG_MINER_KEY = None
