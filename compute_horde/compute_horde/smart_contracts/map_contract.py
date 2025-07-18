import logging
from typing import Any

from django.conf import settings

from compute_horde.smart_contracts.utils import get_contract_abi, get_web3_connection

logger = logging.getLogger(__name__)


def get_contract_value(contract_address: str, config_key: str) -> Any:
    """
    Fetches a value from the Map contract at the specified address.

    :param contract_address: The address of the Map contract.
    :param config_key: The key for which the value is to be fetched.
    :return: The value associated with the key in the Map contract.
    """
    w3 = get_web3_connection(settings.BITTENSOR_NETWORK)
    abi = get_contract_abi("map_abi.json")
    contract = w3.eth.contract(address=w3.to_checksum_address(contract_address), abi=abi)
    return contract.functions.value(config_key).call()


def get_dynamic_configs_from_contract(contract_address: str) -> dict[str, Any]:
    """
    Fetches all dynamic config values from the Map contract at the specified address.

    :param contract_address: The address of the Map contract.
    :return: A dictionary containing all dynamic config values.
    """
    result = {}
    available_configs = [
        key for key in getattr(settings, "CONSTANCE_CONFIG", {}) if key.startswith("DYNAMIC")
    ]
    for key in available_configs:
        try:
            result[key] = get_contract_value(contract_address, key)
        except Exception as e:
            logger.error(f"Failed to fetch dynamic config from contract {contract_address}: {e}")
    return result
