import bittensor
from django.conf import settings
from django.core.cache import cache

_BLOCK_CACHE_KEY = getattr(settings, "COMPUTE_HORDE_BLOCK_CACHE_KEY", "compute_horde.blockchain.block_cache.current_block")
_BLOCK_CACHE_TIMEOUT = getattr(settings, "COMPUTE_HORDE_BLOCK_CACHE_TIMEOUT", 2)

def get_subtensor(network):
    return bittensor.subtensor(network)


def get_current_block() -> int:
    block = cache.get(_BLOCK_CACHE_KEY)
    if block is not None:
        return block

    return cache_current_block()


def cache_current_block() -> int:
    subtensor = get_subtensor(network=settings.BITTENSOR_NETWORK)
    current_block = subtensor.get_current_block()

    cache.set(_BLOCK_CACHE_KEY, current_block, _BLOCK_CACHE_TIMEOUT)

    return current_block
