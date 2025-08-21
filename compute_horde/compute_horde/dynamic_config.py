import datetime
import logging
from typing import Any, Protocol

import requests
from pydantic import BaseModel

from compute_horde.smart_contracts import map_contract

logger = logging.getLogger(__name__)


class ParamItem(BaseModel):
    value: Any
    effective_from: datetime.datetime | None = None  # None = from the beginning of time (-inf)
    reason: str | None = None


class Param(BaseModel):
    description: str
    items: list[ParamItem]


class SupportsSetAttr(Protocol):
    def __setattr__(self, key: str, value: Any) -> None: ...


def sync_dynamic_config(config_url: str, namespace: SupportsSetAttr) -> None:
    """
    Fetches dynamic config from a URL and sets them to namespace.
    Only the keys with `DYNAMIC_` prefix are considered.

    :param config_url: URL to a JSON object of dynamic config
    :param namespace: An object where the config values will be set as attributes
    """
    # https://docs.github.com/en/rest/using-the-rest-api/troubleshooting-the-rest-api?apiVersion=2022-11-28#user-agent-required
    response = requests.get(config_url, headers={"User-Agent": "backend-developers-ltd"})
    response.raise_for_status()
    for param_key, raw_param in response.json().items():
        if not param_key.startswith("DYNAMIC_"):
            continue

        param = Param.model_validate(raw_param)
        now = datetime.datetime.now(datetime.UTC)
        for param_item in param.items:
            if param_item.effective_from is not None and param_item.effective_from > now:
                logger.warning(f"Ignoring dynamic config {param_key}={param_item.value}")
                continue

            try:
                setattr(namespace, param_key, param_item.value)
            except AttributeError:
                logger.warning(f"Failed to set dynamic config {param_key}={param_item.value}")
            else:
                msg = f"Set dynamic config {param_key}={param_item.value}"
                if param_item.reason:
                    msg += f", reason={param_item.reason}"
                logger.info(msg)


def fetch_dynamic_configs_from_contract(
    dynamic_configs: list[str], contract_address: str, namespace: SupportsSetAttr
) -> None:
    """
    Fetches dynamic configs from a smart contract and sets them to the provided namespace.

    :param dynamic_configs: List of dynamic config keys to fetch
    :param contract_address: Address of the smart contract to fetch configs from
    :param namespace: An object where the config values will be set as attributes
    """
    contract_dynamic_configs = map_contract.get_dynamic_configs_from_contract(
        dynamic_configs, contract_address
    )

    for key, value in contract_dynamic_configs.items():
        try:
            setattr(namespace, key, value)
        except AttributeError:
            logger.warning(f"Failed to set dynamic config {key}={value}")
        except Exception as e:
            logger.error(
                f"Failed to fetch dynamic config {key} from contract {contract_address}: {e}"
            )
        else:
            logger.info(f"Set dynamic config {key}={value}. From contract: {contract_address}")
