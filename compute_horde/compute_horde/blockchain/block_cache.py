import asyncio
import functools

import bittensor
from asgiref.sync import async_to_sync
from django.conf import settings
from django.core.cache import cache

_BLOCK_CACHE_KEY = getattr(
    settings, "COMPUTE_HORDE_BLOCK_CACHE_KEY", "compute_horde.blockchain.block_cache.current_block"
)
_BLOCK_CACHE_TIMEOUT = getattr(settings, "COMPUTE_HORDE_BLOCK_CACHE_TIMEOUT", 12)


class BlockNotInCacheError(KeyError):
    pass


@functools.cache
def _get_subtensor(network):
    return bittensor.subtensor(network)


def _clear_subtensor_cache():
    """Clears the memoized subtensor client. Intended for tests to avoid cross-test leakage."""
    _get_subtensor.cache_clear()


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


def get_current_block() -> int:
    try:
        return async_to_sync(aget_current_block)(timeout=0)
    except BlockNotInCacheError:
        return cache_current_block()


def cache_current_block() -> int:
    subtensor = _get_subtensor(network=settings.BITTENSOR_NETWORK)
    current_block: int = subtensor.get_current_block()

    cache.set(_BLOCK_CACHE_KEY, current_block, _BLOCK_CACHE_TIMEOUT)
    return current_block
