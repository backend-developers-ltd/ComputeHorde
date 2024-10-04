import asyncio
import time
from collections.abc import Callable
from contextlib import suppress
from typing import Any

import constance.utils
from asgiref.sync import sync_to_async
from compute_horde.executor_class import ExecutorClass
from constance import config
from django.conf import settings


class DynamicConfigHolder:
    CACHE_TIMEOUT = 300

    def __init__(self):
        self._time_set: float = 0
        self._lock = asyncio.Lock()
        self._config = {k: v[0] for k, v in settings.CONSTANCE_CONFIG.items()}

    async def get(self, key):
        async with self._lock:
            if time.time() - self._time_set > self.CACHE_TIMEOUT:
                self._config = await sync_to_async(constance.utils.get_values)()
                self._time_set = time.time()

        return self._config[key]


dynamic_config_holder = DynamicConfigHolder()


async def aget_config(key):
    return await dynamic_config_holder.get(key)


async def aget_weights_version() -> int:
    if settings.DEBUG_OVERRIDE_WEIGHTS_VERSION is not None:
        return int(settings.DEBUG_OVERRIDE_WEIGHTS_VERSION)
    return int(await aget_config("DYNAMIC_WEIGHTS_VERSION"))


# this is called from a sync context, and rarely, so we don't need caching
def get_synthetic_jobs_flow_version():
    if settings.DEBUG_OVERRIDE_SYNTHETIC_JOBS_FLOW_VERSION is not None:
        return settings.DEBUG_OVERRIDE_SYNTHETIC_JOBS_FLOW_VERSION
    return config.DYNAMIC_SYNTHETIC_JOBS_FLOW_VERSION


def executor_class_value_map_parser(
    value_map_str: str, value_parser: Callable[[str], Any] | None = None
) -> dict[ExecutorClass, Any]:
    result = {}
    for pair in value_map_str.split(","):
        # ignore errors for misconfiguration, i,e. non-existent executor classes,
        # non-integer/negative counts etc.
        with suppress(ValueError):
            executor_class_str, value_str = pair.split("=")
            executor_class = ExecutorClass(executor_class_str)
            if value_parser is not None:
                parsed_value = value_parser(value_str)
            else:
                parsed_value = value_str
            result[executor_class] = parsed_value
    return result


async def get_miner_max_executors_per_class() -> dict[ExecutorClass, int]:
    miner_max_executors_per_class: str = await aget_config("DYNAMIC_MINER_MAX_EXECUTORS_PER_CLASS")
    result = {
        executor_class: count
        for executor_class, count in executor_class_value_map_parser(
            miner_max_executors_per_class, value_parser=int
        ).items()
        if count >= 0
    }
    return result


def get_executor_class_weights() -> dict[ExecutorClass, float]:
    return executor_class_value_map_parser(
        config.DYNAMIC_EXECUTOR_CLASS_WEIGHTS, value_parser=float
    )
