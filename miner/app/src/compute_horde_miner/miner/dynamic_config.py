import asyncio
import time

import constance.utils
from asgiref.sync import sync_to_async
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
