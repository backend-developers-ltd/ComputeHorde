import asyncio
import functools

import bittensor
from asgiref.sync import async_to_sync
from django.conf import settings
from django.core.cache import cache

_BLOCK_CACHE_KEY = getattr(
    settings, "COMPUTE_HORDE_BLOCK_CACHE_KEY", "compute_horde.blockchain.block_cache.current_block"
)
_BLOCK_CACHE_TIMEOUT = getattr(settings, "COMPUTE_HORDE_BLOCK_CACHE_TIMEOUT", 2)


class BlockNotInCacheError(KeyError):
    pass


@functools.cache
def _get_subtensor(network):
    return bittensor.subtensor(network)


async def aget_current_block(timeout: float = 1.0) -> int:
    """
    Gets the current block number from cache. Waits for ``timeout`` seconds if it's not there.

    :param timeout: Number of seconds to wait for the block. Pass 0 to return or raise an exception immediately instead.
    :raise BlockNotInCacheError: If block was not found in cache.
    """
    # Note: cache.get here is not async.
    current_block: int = cache.get(_BLOCK_CACHE_KEY)
    if current_block is not None:
        return current_block

    if timeout <= 0:
        raise BlockNotInCacheError(_BLOCK_CACHE_KEY)

    await asyncio.sleep(timeout)

    return await aget_current_block(timeout=0)


get_current_block = async_to_sync(aget_current_block)


def cache_current_block() -> None:
    subtensor = _get_subtensor(network=settings.BITTENSOR_NETWORK)
    current_block = subtensor.get_current_block()

    cache.set(_BLOCK_CACHE_KEY, current_block, _BLOCK_CACHE_TIMEOUT)
