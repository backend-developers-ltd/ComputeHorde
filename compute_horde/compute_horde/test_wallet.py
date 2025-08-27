import os
import secrets
from functools import cache

import bittensor_wallet

# use unique wallet name when running in Github Actions,
# to ensure hardcoded wallet names like "test_validator" fail,
# but don't do it when running locally to prevent creating lots
# of wallets since we never delete them
_UNIQUE_WALLET_TAG = f"_{secrets.token_hex(4)}" if os.getenv("GITHUB_ACTIONS") == "true" else ""

MISC_WALLET_NAME = f"test_misc{_UNIQUE_WALLET_TAG}"
MISC_WALLET_HOTKEY = "default"

MINER_WALLET_NAME = f"test_miner{_UNIQUE_WALLET_TAG}"
MINER_WALLET_HOTKEY = "default"

VALIDATOR_WALLET_NAME = f"test_validator{_UNIQUE_WALLET_TAG}"
VALIDATOR_WALLET_HOTKEY = "default"


@cache
def get_test_misc_wallet() -> bittensor_wallet.Wallet:
    wallet = bittensor_wallet.Wallet(
        name=MISC_WALLET_NAME,
        hotkey=MISC_WALLET_HOTKEY,
    )
    wallet.regenerate_coldkey(
        mnemonic="couch tomato plug night antenna illegal miss afford champion shine life coin",
        use_password=False,
        overwrite=True,
    )
    wallet.regenerate_hotkey(
        mnemonic="arm clerk film target discover victory secret orbit until loop shrimp cover",
        use_password=False,
        overwrite=True,
    )
    wallet.hotkey_file.get_keypair()  # this raises errors if the keys are inaccessible
    return wallet


@cache
def get_test_miner_wallet() -> bittensor_wallet.Wallet:
    wallet = bittensor_wallet.Wallet(
        name=MINER_WALLET_NAME,
        hotkey=MINER_WALLET_HOTKEY,
    )
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
    wallet.hotkey_file.get_keypair()  # this raises errors if the keys are inaccessible
    return wallet


@cache
def get_test_validator_wallet() -> bittensor_wallet.Wallet:
    wallet = bittensor_wallet.Wallet(
        name=VALIDATOR_WALLET_NAME,
        hotkey=VALIDATOR_WALLET_HOTKEY,
    )
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
    wallet.hotkey_file.get_keypair()  # this raises errors if the keys are inaccessible
    return wallet
