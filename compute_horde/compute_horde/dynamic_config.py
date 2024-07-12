import datetime
import logging
from typing import Any, Protocol

import requests
from pydantic import BaseModel

logger = logging.getLogger(__name__)


class ParamItem(BaseModel):
    value: Any
    effective_from: datetime.datetime | None = None  # None = from the beginning of time (-inf)
    effective_until: datetime.datetime | None = None  # None = to the end of time (+inf)
    reason: str | None = None


class Param(BaseModel):
    description: str
    items: list[ParamItem]


class SupportsSetAttr(Protocol):
    def __setattr__(self, key: str, value: Any) -> None: ...


def sync_dynamic_config(
    config_url: str, ignore_keys: list[str], namespace: SupportsSetAttr
) -> None:
    """
    Fetches dynamic config from a URL and sets them to namespace.

    :param config_url: URL to a JSON object of dynamic config
    :param ignore_keys: keys to ignore from the JSON config
    :param namespace: An object where the config values will be set as attributes
    """
    response = requests.get(config_url)
    response.raise_for_status()
    for param_key, raw_param in response.json().items():
        if param_key in ignore_keys:
            continue

        param = Param.model_validate(raw_param)
        now = datetime.datetime.now(datetime.UTC)
        for param_item in param.items:
            if param_item.effective_from is not None and param_item.effective_from > now:
                logger.warning(f"Ignoring dynamic config {param_key}={param_item.value}")
                continue

            if param_item.effective_until is not None and param_item.effective_until < now:
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
