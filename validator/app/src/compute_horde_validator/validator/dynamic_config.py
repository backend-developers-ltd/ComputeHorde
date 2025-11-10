from collections.abc import Callable
from contextlib import suppress
from typing import Any

from asgiref.sync import sync_to_async
from compute_horde_core.executor_class import ExecutorClass
from constance import config


def get_config(key: str) -> Any:
    return getattr(config, key)


async def aget_config(key: str) -> Any:
    return await sync_to_async(lambda: getattr(config, key))()


def executor_class_value_map_parser(
    value_map_str: str, value_parser: Callable[[str], Any] | None = None
) -> dict[ExecutorClass, Any]:
    result = {}
    for pair in value_map_str.split(","):
        # ignore errors for misconfiguration, i,e. non-existent executor classes,
        # non-integer/negative counts, etc.
        with suppress(ValueError):
            executor_class_str, value_str = pair.split("=")
            executor_class = ExecutorClass(executor_class_str)
            if value_parser is not None:
                parsed_value = value_parser(value_str)
            else:
                parsed_value = value_str
            result[executor_class] = parsed_value
    return result


def get_miner_max_executors_per_class_sync() -> dict[ExecutorClass, int]:
    miner_max_executors_per_class: str = get_config("DYNAMIC_MINER_MAX_EXECUTORS_PER_CLASS")
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
        get_config("DYNAMIC_EXECUTOR_CLASS_WEIGHTS"), value_parser=float
    )
