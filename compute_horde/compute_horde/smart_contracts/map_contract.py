import logging
from typing import Any

from django.conf import settings
from web3 import Web3
from web3.contract.contract import Contract
from web3.exceptions import BadFunctionCallOutput

from compute_horde.smart_contracts.utils import get_contract_abi, get_web3_connection

logger = logging.getLogger(__name__)


def get_web3_contract(contract_address: str) -> Contract:
    """
    Returns a web3 contract instance for the given contract address.
    """
    if not Web3.is_address(contract_address):
        raise ValueError(f"Invalid contract address {contract_address}")

    w3 = get_web3_connection(settings.BITTENSOR_NETWORK)
    abi = get_contract_abi("map_abi.json")
    return w3.eth.contract(
        address=w3.to_checksum_address(contract_address),
        abi=abi,
    )


def get_dynamic_configs_from_contract(
    configs: dict[str, type], contract_address: str
) -> dict[str, Any]:
    """
    Fetches dynamic config values from the Map contract.
    Casts the values to the specified types.

    :param configs: List of config keys to fetch from the contract.
    :param contract_address: The address of the Map contract.
    :return: A dictionary with config keys and their corresponding values from the contract.
    """
    result = {}
    map_contract = get_web3_contract(contract_address)
    for key, type_object in configs.items():
        value = None
        try:
            value = map_contract.functions.value(key).call()
            if value == "":
                logger.warning(f"Dynamic config {key} not found in contract {contract_address}")
                continue

            result[key] = type_object(value)
        except BadFunctionCallOutput as e:
            raise ValueError(
                f"Failed to fetch dynamic config {key} from contract {contract_address}: {e}"
            )
        except ValueError as e:
            logger.warning(
                f"Failed to cast dynamic config {key}={value} to type {type_object}: {e}"
            )
        except Exception as e:
            logger.error(f"Failed to fetch dynamic config from contract {contract_address}: {e}")
    return result


def get_dynamic_config_types_from_settings() -> dict[str, type]:
    """
    Returns a dictionary of dynamic config keys and their types from settings.
    Only keys starting with 'DYNAMIC_' are considered.
    """
    return {
        key: config_definition[2]
        for key, config_definition in settings.CONSTANCE_CONFIG.items()
        if key.startswith("DYNAMIC_")
    }
