import json
import logging
from typing import Any

from django.conf import settings
from web3 import Web3
from web3.contract.contract import Contract

from compute_horde.smart_contracts.utils import get_contract_abi, get_web3_connection

logger = logging.getLogger(__name__)


def get_web3_contract(w3: Web3, contract_address: str) -> Contract:
    """
    Returns a web3 contract instance for the given contract address.
    """
    if not Web3.is_address(contract_address):
        raise ValueError(f"Invalid contract address {contract_address}")

    abi = get_contract_abi("map_abi.json")
    return w3.eth.contract(
        address=w3.to_checksum_address(contract_address),
        abi=abi,
    )


def get_dynamic_configs_from_contract(configs: list[str], contract_address: str) -> dict[str, Any]:
    """
    Fetches dynamic config values from the Map contract.
    Casts the values to the specified types.

    :param configs: List of config keys to fetch from the contract.
    :param contract_address: The address of the Map contract.
    :return: A dictionary with config keys and their corresponding values from the contract.
    """
    w3 = get_web3_connection(settings.BITTENSOR_NETWORK)
    map_contract = get_web3_contract(w3, contract_address)

    with w3.batch_requests() as batch:
        for key in configs:
            batch.add(map_contract.functions.value(key))
        values: list[str] = batch.execute()  # type: ignore

    result = {}
    for key, value in zip(configs, values):
        if value == "":
            logger.warning(f"Dynamic config {key} not found in contract {contract_address}")
            continue

        try:
            result[key] = json.loads(value)
        except json.JSONDecodeError:
            logger.warning(f"Failed to parse dynamic config {key} from contract {contract_address}")

    return result


def get_dynamic_config_types_from_settings() -> list[str]:
    """
    Returns a list of dynamic config keys from settings.
    Only keys starting with 'DYNAMIC_' are considered.
    """
    return [key for key in settings.CONSTANCE_CONFIG if key.startswith("DYNAMIC_")]
