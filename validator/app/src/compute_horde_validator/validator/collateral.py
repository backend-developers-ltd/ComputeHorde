import hashlib
import json
import pathlib
from dataclasses import dataclass
from typing import Any

import bittensor
import bittensor.utils
import requests
from eth_account import Account
from eth_keys.datatypes import PrivateKey
from eth_utils import keccak
from hexbytes import HexBytes
from web3 import Web3

WEI_PER_TAO = 10**18


def get_collateral_abi() -> Any:
    """Retrieve the ABI definition for the collateral smart contract."""
    path = pathlib.Path(__file__).parent / "collateral_abi.json"
    abi = json.loads(path.read_text())
    return abi


def get_web3_connection(network: str) -> Web3:
    """Connects to a Web3 provider using the provided RPC URL."""
    _, rpc_url = bittensor.utils.determine_chain_endpoint_and_network(network)
    w3 = Web3(Web3.WebsocketProvider(rpc_url))
    if not w3.is_connected():
        raise ConnectionError(f"Failed to connect to RPC node at {rpc_url}")
    return w3


def wei_to_tao(wei: int) -> float:
    """Convert Wei to TAO."""
    return wei / WEI_PER_TAO


def tao_to_wei(tao: float) -> int:
    """Convert TAO to Wei."""
    return int(tao * WEI_PER_TAO)


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
    contract_checksum_address = Web3.to_checksum_address(contract_address)
    miner_checksum_address = Web3.to_checksum_address(miner_address)

    contract = w3.eth.contract(address=contract_checksum_address, abi=abi)
    collateral: int = contract.functions.collaterals(miner_checksum_address).call(
        block_identifier=block_identifier
    )
    return collateral


@dataclass
class DepositEvent:
    account: str
    amount: int
    block_number: int
    transaction_hash: str


def get_deposit_events(
    w3: Web3, contract_address: str, block_num_low: int, block_num_high: int
) -> list[DepositEvent]:
    """Fetch all Deposit events emitted by the Collateral contract within a block range.

    Args:
        w3 (Web3): Web3 instance to use for blockchain interaction
        contract_address (str): The address of the deployed Collateral contract
        block_num_low (int): The starting block number (inclusive)
        block_num_high (int): The ending block number (inclusive)

    Returns:
        list[DepositEvent]: List of Deposit events
    """
    abi = get_collateral_abi()
    contract_checksum_address = Web3.to_checksum_address(contract_address)
    contract = w3.eth.contract(address=contract_checksum_address, abi=abi)

    event_signature = "Deposit(address,uint256)"
    event_topic = w3.keccak(text=event_signature).hex()

    filter_params = {
        "address": contract_checksum_address,
        "fromBlock": hex(block_num_low),
        "toBlock": hex(block_num_high),
        "topics": [event_topic],
    }

    logs = w3.eth.get_logs(filter_params)

    events = []
    for log in logs:
        account_address = "0x" + log["topics"][1].hex()[-40:]
        account = w3.to_checksum_address(account_address)
        decoded_event = contract.events.Deposit().process_log(log)
        events.append(
            DepositEvent(
                account=account,
                amount=decoded_event["args"]["amount"],
                block_number=log["blockNumber"],
                transaction_hash=log["transactionHash"].hex(),
            )
        )

    return events


def associate_evm_key(
    wallet: bittensor.wallet,
    evm_address: str,
    evm_private_key: str,
    network: str,
    netuid: int,
) -> None:
    """
    Associate an EVM key with a given wallet for a specific subnet.

    Args:
        wallet (bittensor.wallet): The wallet object containing the hotkey for signing
            the transaction. The wallet.hotkey will be associated with the EVM key.
        evm_address (str): The Ethereum Virtual Machine (EVM) address to be associated
            with the wallet.
        evm_private_key (str): The private key corresponding to the EVM address, used
            for signing the message.
        network (str): The name of the Subtensor network where the EVM key is to be
            associated.
        netuid (int): The numerical identifier (UID) of the Subtensor network.
    """
    subtensor = bittensor.subtensor(network=network)
    block_number = subtensor.get_current_block()

    # subtensor encodes the u64 block number as little endian bytes before hashing
    # https://github.com/opentensor/subtensor/blob/6b86ebf30d3fb83f9d43ed4ce713c43204394e67/pallets/subtensor/src/tests/evm.rs#L44
    # https://github.com/paritytech/parity-scale-codec/blob/v3.6.12/src/codec.rs#L220
    # https://github.com/paritytech/parity-scale-codec/blob/v3.6.12/src/codec.rs#L1439
    encoded_block_number = block_number.to_bytes(length=8, byteorder="little")
    hashed_block_number = bytes(keccak(encoded_block_number))

    hotkey_bytes: bytes = wallet.hotkey.public_key
    message = hotkey_bytes + hashed_block_number
    signature = PrivateKey(HexBytes(evm_private_key)).sign_msg(message)

    call = subtensor.substrate.compose_call(
        call_module="SubtensorModule",
        call_function="associate_evm_key",
        call_params={
            "netuid": netuid,
            "hotkey": wallet.hotkey.ss58_address,
            "evm_key": evm_address,
            "block_number": block_number,
            "signature": signature.to_bytes(),
        },
    )

    success, error_message = subtensor.sign_and_send_extrinsic(
        call,
        wallet,
        wait_for_inclusion=True,
        wait_for_finalization=True,
    )

    if not success:
        raise ValueError(error_message)  # TODO: fix exception type


def get_evm_key_associations(subtensor: bittensor.Subtensor, netuid: int) -> dict[int, str]:
    """
    Retrieve all EVM key associations for a specific subnet.

    Arguments:
        subtensor (bittensor.Subtensor): The Subtensor object to use for querying the network.
        netuid (int): The NetUID for which to retrieve EVM key associations.

    Returns:
        dict: A dictionary mapping UIDs (int) to their associated EVM key addresses (str).
    """
    associations = subtensor.query_map_subtensor("AssociatedEvmAddress", params=[netuid])
    uid_evm_address_map = {}
    for uid, scale_obj in associations:
        evm_address_raw, block = scale_obj.value
        evm_address = "0x" + bytes(evm_address_raw[0]).hex()
        uid_evm_address_map[uid] = evm_address
    return uid_evm_address_map


def calculate_md5_checksum(url: str) -> bytes:
    """Calculate the MD5 checksum of url contents."""
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    return hashlib.md5(response.content).digest()


def build_and_send_transaction(w3, function_call, account, gas_limit=100000, value=0):
    """Build, sign and send a transaction.

    Args:
        w3: Web3 instance
        function_call: Contract function call to execute
        account: Account to send transaction from
        gas_limit: Maximum gas to use for the transaction
        value: Amount of ETH to send with the transaction (in Wei)
    """
    transaction = function_call.build_transaction(
        {
            "from": account.address,
            "nonce": w3.eth.get_transaction_count(account.address),
            "gas": gas_limit,
            "gasPrice": w3.eth.gas_price,
            "chainId": w3.eth.chain_id,
            "value": value,
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
    private_key: str,
    miner_address: str,
    amount_tao: float,
    url: str,
) -> SlashedEvent:
    """Slash collateral from a miner.

    Args:
        w3: Web3 instance to use for blockchain interaction.
        contract_address: Address of the Collateral contract.
        private_key: Private key of the validator.
        miner_address: EVM address of the miner to query.
        amount_tao: Amount of TAO to slash.
        url: URL containing information about the slash.

    Returns:
        Transaction receipt with slash event details
    """
    abi = get_collateral_abi()
    account = Account.from_key(private_key)
    contract_checksum_address = Web3.to_checksum_address(contract_address)
    miner_checksum_address = Web3.to_checksum_address(miner_address)

    contract = w3.eth.contract(address=contract_checksum_address, abi=abi)

    # Calculate MD5 checksum if URL is valid
    if url.startswith(("http://", "https://")):
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        md5_checksum = hashlib.md5(response.content).digest()
    else:
        md5_checksum = b"\x00" * 16

    call = contract.functions.slashCollateral(
        miner_checksum_address, tao_to_wei(amount_tao), url, md5_checksum
    )
    tx_hash = build_and_send_transaction(w3, call, account, gas_limit=200_000)

    receipt = w3.eth.wait_for_transaction_receipt(tx_hash, 300, 2)
    if receipt["status"] == 0:
        raise SlashCollateralError("collateral slashing transaction failed")

    raw_event = contract.events.Slashed().process_receipt(receipt)
    return SlashedEvent.from_dict(raw_event[0])
