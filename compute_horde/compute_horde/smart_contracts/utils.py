import functools
import json
import pathlib
from typing import Any

import bittensor.utils
from web3 import Web3
from web3.exceptions import ProviderConnectionError


def get_web3_connection(network: str) -> Web3:
    """
    Establish a Web3 connection to the specified network.

    :raise ProviderConnectionError: If failed to connect to RPC node.
    """
    _, rpc_url = bittensor.utils.determine_chain_endpoint_and_network(network)
    w3 = Web3(Web3.LegacyWebSocketProvider(rpc_url))
    if not w3.is_connected(show_traceback=True):
        raise ProviderConnectionError(f"Failed to connect to RPC node at {rpc_url}")
    return w3


@functools.cache
def get_contract_abi(abi_filename: str) -> Any:
    """
    Retrieve the ABI definition for a smart contract from a JSON file.

    :param abi_filename: The name of the ABI JSON file.
    :return: The ABI as a Python object.
    """
    path = pathlib.Path(__file__).parent / "abi/" / abi_filename
    return json.loads(path.read_text())
