import json
import pathlib
from dataclasses import dataclass
from typing import Any

import bittensor
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


def get_web3_connection(rpc_url: str) -> Web3:
    """Connects to a Web3 provider using the provided RPC URL."""
    w3 = Web3(Web3.HTTPProvider(rpc_url))
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
    w3: Web3, contract_address: str, miner_address: str, block_identifier: int | None = None
) -> int:
    """
    Query the collateral amount for a given miner address.

    Args:
        w3: Web3 instance to use for blockchain interaction.
        contract_address: Address of the Collateral contract
        miner_address: H160 address of the miner to query
        block_identifier: Block number to query the latest block. Defaults to None, which queries the latest block.

    Returns:
        number: Collateral amount in Wei
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


def get_evm_key_associations(netuid: int, network: str) -> dict[int, str]:
    """
    Retrieve all EVM key associations for a specific subnet.

    Arguments:
        netuid (int): The NetUID for which to retrieve EVM key associations.
        network (str): The Subtensor network name to connect to.

    Returns:
        dict: A dictionary mapping UIDs (int) to their associated EVM key addresses (str).
    """
    with bittensor.subtensor(network=network) as subtensor:
        # TODO: check if there is a way to get the association for a specific uid
        # this gets all the uid to evm key associations for the netuid
        associations = subtensor.query_map_subtensor("AssociatedEvmAddress", params=[netuid])

        uid_evm_address_map = {}
        for uid, scale_obj in associations:
            evm_address_raw, block = scale_obj.value
            evm_address = bytes(evm_address_raw[0]).hex()
            uid_evm_address_map[uid] = evm_address

        return uid_evm_address_map
