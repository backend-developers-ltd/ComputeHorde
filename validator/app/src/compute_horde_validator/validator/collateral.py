import functools
import hashlib
import json
import pathlib
from dataclasses import dataclass
from typing import Any

import bittensor.utils
import requests
import turbobt
from asgiref.sync import async_to_sync
from django.conf import settings
from eth_account import Account
from eth_account.signers.local import LocalAccount
from hexbytes import HexBytes
from web3 import Web3
from web3.contract.contract import ContractFunction
from web3.types import Wei


@functools.cache
def get_private_key() -> str | None:
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


_cached_contract_address: str | None = None


async def get_collateral_contract_address_async() -> str | None:
    # async functions can't have functools.cache :(
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


get_collateral_contract_address = async_to_sync(get_collateral_contract_address_async)


@functools.cache
def get_collateral_abi() -> Any:
    """Retrieve the ABI definition for the collateral smart contract."""
    path = pathlib.Path(__file__).parent / "collateral_abi.json"
    abi = json.loads(path.read_text())
    return abi


def get_web3_connection(network: str) -> Web3:
    """Connects to a Web3 provider using the provided network."""
    _, rpc_url = bittensor.utils.determine_chain_endpoint_and_network(network)
    w3 = Web3(Web3.LegacyWebSocketProvider(rpc_url))
    if not w3.is_connected():
        raise ConnectionError(f"Failed to connect to RPC node at {rpc_url}")
    return w3


def get_miner_collateral(
    w3: Web3,
    contract_address: str,
    miner_address: str,
    block_identifier: int | None = None,
) -> int:
    """
    Query the collateral amount for a given miner address.

    Args:
        w3: Web3 instance to use for blockchain interaction.
        contract_address: Address of the Collateral contract.
        miner_address: EVM address of the miner to query.
        block_identifier: Block number to query the latest block. Defaults to None, which queries the latest block.

    Returns:
        Collateral amount in Wei
    """
    abi = get_collateral_abi()
    contract_checksum_address = w3.to_checksum_address(contract_address)
    miner_checksum_address = w3.to_checksum_address(miner_address)

    contract = w3.eth.contract(address=contract_checksum_address, abi=abi)
    collateral: int = contract.functions.collaterals(miner_checksum_address).call(
        block_identifier=block_identifier
    )
    return collateral


async def get_evm_key_associations(
    subtensor: turbobt.Subtensor,
    netuid: int,
    block_hash: str | None = None,
) -> dict[int, str]:
    """
    Retrieve all EVM key associations for a specific subnet.

    Arguments:
        subtensor (turbobt.Subtensor): The Subtensor object to use for querying the network.
        netuid (int): The NetUID for which to retrieve EVM key associations.
        block_hash (str | None, optional): The block hash to query. Defaults to None, which queries the latest block.

    Returns:
        dict: A dictionary mapping UIDs (int) to their associated EVM key addresses (str).
    """
    associations = await subtensor.subtensor_module.AssociatedEvmAddress.query(
        netuid,
        block_hash=block_hash,
    )

    return {uid: evm_address for (netuid, uid), (evm_address, block) in associations}


def build_and_send_transaction(
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


class SlashCollateralError(Exception): ...


@dataclass
class SlashedEvent:
    event: str
    logIndex: int
    transactionIndex: int
    transactionHash: HexBytes
    address: str
    blockHash: HexBytes
    blockNumber: int

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "SlashedEvent":
        return cls(
            event=data["event"],
            logIndex=data["logIndex"],
            transactionIndex=data["transactionIndex"],
            transactionHash=data["transactionHash"],
            address=data["address"],
            blockHash=data["blockHash"],
            blockNumber=data["blockNumber"],
        )


def slash_collateral(
    w3: Web3,
    contract_address: str,
    miner_address: str,
    amount_wei: int,
    url: str,
) -> SlashedEvent:
    """Slash collateral from a miner.

    Args:
        w3: Web3 instance to use for blockchain interaction.
        contract_address: Address of the Collateral contract.
        miner_address: EVM address of the miner to slash.
        amount_wei: Amount of Wei to slash.
        url: URL containing information about the slash.

    Returns:
        Transaction receipt with slash event details
    """
    private_key = get_private_key()
    assert private_key is not None, "EVM private key not found"

    abi = get_collateral_abi()
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
    tx_hash = build_and_send_transaction(w3, function, account, gas_limit=200_000)

    receipt = w3.eth.wait_for_transaction_receipt(tx_hash, 300, 2)
    if receipt["status"] == 0:
        raise SlashCollateralError("collateral slashing transaction failed")

    raw_event = contract.events.Slashed().process_receipt(receipt)
    return SlashedEvent.from_dict(raw_event[0])
