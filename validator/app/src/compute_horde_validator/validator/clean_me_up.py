import asyncio
import functools
from collections.abc import Callable
from typing import ParamSpec, TypeVar

from asgiref.sync import async_to_sync, sync_to_async
from bt_ddos_shield.turbobt import ShieldedBittensor
from celery.utils.log import get_task_logger
from compute_horde.miner_client.organic import OrganicMinerClient
from compute_horde.transport.base import TransportConnectionError
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
    client: OrganicMinerClient, timeout: float = 30
) -> tuple[str, dict[ExecutorClass, int] | None]:
    """Get manifest from a single miner client"""
    hotkey = client.miner_hotkey
    data = {"hotkey": hotkey}
    try:
        async with asyncio.timeout(timeout):
            try:
                # Connect to the miner - this will trigger the manifest to be sent
                await client.connect()
                manifest = await client.miner_manifest
                return hotkey, manifest

            except TransportConnectionError as exc:
                msg = f"Error fetching manifest for {hotkey}: {exc}"
                await save_compute_time_allowance_event(
                    SystemEvent.EventSubType.MANIFEST_ERROR, msg, data=data
                )
                logger.warning(msg)
                return hotkey, None

    except TimeoutError:
        msg = f"Timeout fetching manifest for {hotkey}"
        await save_compute_time_allowance_event(
            SystemEvent.EventSubType.MANIFEST_TIMEOUT,
            msg,
            data,
        )
        logger.warning(msg)
        return hotkey, None

    except Exception as exc:
        msg = f"Failed to fetch manifest for {hotkey}: {exc}"
        await save_compute_time_allowance_event(
            SystemEvent.EventSubType.MANIFEST_ERROR,
            msg,
            data,
        )
        logger.warning(msg)
        return hotkey, None


P = ParamSpec("P")
R = TypeVar("R")


def bittensor_client(func: Callable[P, R]) -> Callable[..., R]:
    @async_to_sync
    async def synced_bittensor(*args, bittensor=None, **kwargs):
        async with ShieldedBittensor(
            settings.BITTENSOR_NETWORK,
            ddos_shield_netuid=settings.BITTENSOR_NETUID,
            ddos_shield_options=settings.BITTENSOR_SHIELD_METAGRAPH_OPTIONS(),
            wallet=settings.BITTENSOR_WALLET(),
        ) as bittensor:
            return await sync_to_async(func)(*args, bittensor=bittensor, **kwargs)

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        return synced_bittensor(*args, **kwargs)

    return wrapper
