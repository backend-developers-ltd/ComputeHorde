import asyncio

import aiohttp
from celery.utils.log import get_task_logger
from compute_horde_core.executor_class import ExecutorClass
from django.conf import settings

from compute_horde_validator.validator.models import (
    SystemEvent,
)

logger = get_task_logger(__name__)


async def save_compute_time_allowance_event(subtype, msg, data):
    await SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).acreate(
        type=SystemEvent.EventType.COMPUTE_TIME_ALLOWANCE,
        subtype=subtype,
        long_description=msg,
        data=data,
    )


async def get_single_manifest(
    address: str, port: int, hotkey: str, timeout: float = 30
) -> tuple[str, dict[ExecutorClass, int] | None]:
    """Get manifest from a single miner via HTTP"""
    try:
        async with asyncio.timeout(timeout):
            async with aiohttp.ClientSession() as session:
                url = f"http://{address}:{port}/v0.1/manifest"
                async with await session.get(url) as response:
                    response_json = await response.json()
                    if response.status == 200:
                        manifest = response_json.get("manifest", {})
                        return hotkey, manifest
                    else:
                        msg = f"HTTP {response.status} fetching manifest for {hotkey}"
                        await save_compute_time_allowance_event(
                            SystemEvent.EventSubType.MANIFEST_ERROR, msg, data={"hotkey": hotkey}
                        )
                        logger.warning(msg)
                        return hotkey, None

    except TimeoutError:
        msg = f"Timeout fetching manifest for {hotkey}"
        await save_compute_time_allowance_event(
            SystemEvent.EventSubType.MANIFEST_TIMEOUT,
            msg,
            {"hotkey": hotkey},
        )
        logger.warning(msg)
        return hotkey, None

    except Exception as exc:
        msg = f"Failed to fetch manifest for {hotkey}: {exc}"
        await save_compute_time_allowance_event(
            SystemEvent.EventSubType.MANIFEST_ERROR,
            msg,
            {"hotkey": hotkey},
        )
        logger.warning(msg)
        return hotkey, None
