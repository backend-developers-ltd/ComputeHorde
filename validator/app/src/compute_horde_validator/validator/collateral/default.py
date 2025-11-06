import functools
import hashlib
import json
import logging
import pathlib
import re
from decimal import Decimal
from typing import Any

import requests
import turbobt
from asgiref.sync import async_to_sync
from compute_horde.smart_contracts.utils import get_web3_connection
from constance import config
from django.conf import settings
from eth_account import Account
from eth_account.signers.local import LocalAccount
from hexbytes import HexBytes
from web3 import Web3
from web3.contract.contract import ContractFunction
from web3.exceptions import Web3RPCError
from web3.types import Wei

from compute_horde_validator.validator.models import Miner

from .base import CollateralBase
from .types import (
    CollateralException,
    MinerCollateral,
    NonceTooHighCollateralException,
    NonceTooLowCollateralException,
    ReplacementUnderpricedCollateralException,
    SlashCollateralError,
)

logger = logging.getLogger(__name__)

_cached_contract_address: str | None = None


_ERROR_PATTERNS = [
    (NonceTooLowCollateralException, re.compile(r"nonce too low", re.I)),
    (NonceTooHighCollateralException, re.compile(r"nonce too high", re.I)),
    (
        ReplacementUnderpricedCollateralException,
        re.compile(r"replacement transaction underpriced", re.I),
    ),
]


@functools.cache
def _get_private_key() -> str | None:
    wallet = settings.BITTENSOR_WALLET()
    path = (
        pathlib.Path(settings.BITTENSOR_WALLET_DIRECTORY) / wallet.name / "h160" / wallet.hotkey_str
    )
    try:
        content = json.loads(path.read_text())
        private_key: str = content["private_key"]
        return private_key
    except (FileNotFoundError, KeyError, json.JSONDecodeError):
        return None


@functools.cache
def _get_collateral_abi() -> Any:
    """Retrieve the ABI definition for the collateral smart contract."""
    path = pathlib.Path(__file__).parent / ".." / "collateral_abi.json"
    abi = json.loads(path.read_text())
    return abi


def _get_collateral_exception_from_web3(e: Web3RPCError):
    raw_msg = getattr(e, "message", "") or str(e)

    for exception_cls, pattern in _ERROR_PATTERNS:
        if pattern.search(raw_msg):
            return exception_cls(raw_msg)

    return CollateralException(raw_msg)


class Collateral(CollateralBase):
    def list_miners_with_sufficient_collateral(self, min_amount_wei: int) -> list[MinerCollateral]:
        miners = Miner.objects.filter(collateral_wei__gte=Decimal(min_amount_wei))
        return [
            MinerCollateral(
                hotkey=m.hotkey,
                collateral_wei=int(m.collateral_wei),
            )
            for m in miners
        ]

    def slash_collateral(
        self,
        miner_hotkey: str,
        url: str,
    ) -> None:
        private_key = self._get_private_key()
        assert private_key is not None, "EVM private key not found"

        try:
            miner = Miner.objects.get(hotkey=miner_hotkey)
            miner_address = miner.evm_address
            if not miner_address:
                raise SlashCollateralError(f"Miner {miner_hotkey} has no associated EVM address")
        except Miner.DoesNotExist:
            raise SlashCollateralError(f"Miner {miner_hotkey} not found")

        w3 = get_web3_connection(network=settings.BITTENSOR_NETWORK)

        amount_wei = config.DYNAMIC_COLLATERAL_SLASH_AMOUNT_WEI
        if amount_wei <= 0:
            raise SlashCollateralError("Slash amount must be greater than 0")

        contract_address = self._get_collateral_contract_address()
        if contract_address is None:
            raise SlashCollateralError("Collateral contract address not configured")

        abi = self._get_collateral_abi()
        account: LocalAccount = Account.from_key(private_key)
        contract_checksum_address = w3.to_checksum_address(contract_address)
        miner_checksum_address = w3.to_checksum_address(miner_address)
        contract = w3.eth.contract(address=contract_checksum_address, abi=abi)

        # Calculate MD5 checksum if URL is valid
        if url.startswith(("http://", "https://")):
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            md5_checksum = hashlib.md5(response.content).digest()
        else:
            md5_checksum = b"\x00" * 16

        function = contract.functions.slashCollateral(
            miner_checksum_address, amount_wei, url, md5_checksum
        )

        tx_hash = self._build_and_send_transaction(w3, function, account, gas_limit=200_000)

        receipt = w3.eth.wait_for_transaction_receipt(tx_hash, 300, 2)
        if receipt["status"] == 0:
            raise SlashCollateralError("collateral slashing transaction failed")

    def _get_collateral_contract_address(self) -> str | None:
        global _cached_contract_address
        if _cached_contract_address:
            return _cached_contract_address

        hotkey = settings.BITTENSOR_WALLET().hotkey.ss58_address

        async def _fetch_contract_address() -> str | None:
            async with turbobt.Bittensor(settings.BITTENSOR_NETWORK) as bt_client:
                subnet = bt_client.subnet(settings.BITTENSOR_NETUID)
                raw_commitment = await subnet.commitments.get(hotkey)
                if not raw_commitment:
                    return None

                try:
                    data = json.loads(raw_commitment)
                    address: str = data["contract"]["address"]
                    return address
                except (TypeError, KeyError, json.JSONDecodeError):
                    return None

        _cached_contract_address = async_to_sync(_fetch_contract_address)()
        return _cached_contract_address

    def _get_private_key(self) -> str | None:
        return _get_private_key()

    def _get_collateral_abi(self) -> Any:
        """Retrieve the ABI definition for the collateral smart contract."""
        return _get_collateral_abi()

    def _build_and_send_transaction(
        self,
        w3: Web3,
        function: ContractFunction,
        account: LocalAccount,
        gas_limit: int = 100000,
        value: int = 0,
    ) -> HexBytes:
        """Build, sign and send a transaction.

        Args:
            w3: Web3 instance
            function: Contract function call to execute
            account: Account to send transaction from
            gas_limit: Maximum gas to use for the transaction
            value: Amount of ETH to send with the transaction (in Wei)
        """
        nonce = None
        gas_price = None
        chain_id = None

        try:
            nonce = w3.eth.get_transaction_count(account.address)
            gas_price = w3.eth.gas_price
            chain_id = w3.eth.chain_id

            logger.info(
                "Building and sending transaction from=%s nonce=%s gas_price=%s chain_id=%s value=%s",
                account.address,
                nonce,
                gas_price,
                chain_id,
                value,
            )

            transaction = function.build_transaction(
                {
                    "from": account.address,
                    "nonce": nonce,
                    "gas": gas_limit,
                    "gasPrice": gas_price,
                    "chainId": chain_id,
                    "value": Wei(value),
                }
            )

            signed_txn = w3.eth.account.sign_transaction(transaction, account.key)
            tx_hash = w3.eth.send_raw_transaction(signed_txn.raw_transaction)
        except Web3RPCError as e:
            logger.warning(
                "Web3RPCError while sending transaction: %s from=%s nonce=%s gas_price=%s chain_id=%s value=%s",
                str(e),
                account.address,
                nonce,
                gas_price,
                chain_id,
                value,
                exc_info=True,
            )

            raise _get_collateral_exception_from_web3(e) from e
        else:
            return tx_hash


_collateral_instance: Collateral | None = None


def collateral() -> Collateral:
    global _collateral_instance
    if _collateral_instance is None:
        _collateral_instance = Collateral()
    return _collateral_instance
