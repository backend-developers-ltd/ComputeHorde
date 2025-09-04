import functools
import hashlib
import json
import logging
import pathlib
from decimal import Decimal
from typing import Any

import requests
import turbobt
from django.conf import settings
from eth_account import Account
from eth_account.signers.local import LocalAccount
from hexbytes import HexBytes
from web3 import Web3
from web3.contract.contract import ContractFunction
from web3.types import Wei

from compute_horde_validator.validator.models import Miner

from .base import CollateralBase
from .types import MinerCollateral, SlashCollateralError, SlashedEvent

logger = logging.getLogger(__name__)

_cached_contract_address: str | None = None


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


class Collateral(CollateralBase):
    def list_miners_with_sufficient_collateral(self, min_amount_wei: int) -> list[MinerCollateral]:
        miners = Miner.objects.filter(collateral_wei__gte=Decimal(min_amount_wei))
        return [
            MinerCollateral(
                hotkey=m.hotkey,
                uid=m.uid,
                evm_address=m.evm_address,
                collateral_wei=int(m.collateral_wei),
            )
            for m in miners
        ]

    def slash_collateral(
        self,
        w3: Web3,
        contract_address: str,
        miner_address: str,
        amount_wei: int,
        url: str,
    ) -> SlashedEvent:
        private_key = self._get_private_key()
        assert private_key is not None, "EVM private key not found"

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

        raw_event = contract.events.Slashed().process_receipt(receipt)
        return SlashedEvent.from_dict(raw_event[0])

    async def get_collateral_contract_address(self) -> str | None:
        global _cached_contract_address
        if _cached_contract_address:
            return _cached_contract_address

        hotkey = settings.BITTENSOR_WALLET().hotkey.ss58_address

        async with turbobt.Bittensor(settings.BITTENSOR_NETWORK) as bt_client:
            subnet = bt_client.subnet(settings.BITTENSOR_NETUID)
            raw_commitment = await subnet.commitments.get(hotkey)
            if not raw_commitment:
                return None

            try:
                data = json.loads(raw_commitment)
                _cached_contract_address = data["contract"]["address"]
                return _cached_contract_address
            except (TypeError, KeyError, json.JSONDecodeError):
                return None

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
        transaction = function.build_transaction(
            {
                "from": account.address,
                "nonce": w3.eth.get_transaction_count(account.address),
                "gas": gas_limit,
                "gasPrice": w3.eth.gas_price,
                "chainId": w3.eth.chain_id,
                "value": Wei(value),
            }
        )

        signed_txn = w3.eth.account.sign_transaction(transaction, account.key)
        tx_hash = w3.eth.send_raw_transaction(signed_txn.raw_transaction)
        return tx_hash


_collateral_instance: Collateral | None = None


def collateral() -> Collateral:
    global _collateral_instance
    if _collateral_instance is None:
        _collateral_instance = Collateral()
    return _collateral_instance
