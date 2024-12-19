from typing import Any

from asgiref.sync import sync_to_async
from constance import config


async def aget_config(key: str) -> Any:
    return await sync_to_async(lambda: getattr(config, key))()
