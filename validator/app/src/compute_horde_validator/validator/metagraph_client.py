import asyncio

import bittensor
from asgiref.sync import sync_to_async
from django.conf import settings


class AsyncMetagraphClient:
    def __init__(self):
        self._metagraph_future = None
        self._future_lock = asyncio.Lock()

    async def get_metagraph(self):
        future = None
        set_result = False
        async with self._future_lock:
            if self._metagraph_future is None:
                future = self._metagraph_future = asyncio.Future()
                set_result = True
            else:
                future = self._metagraph_future
        if set_result:
            try:
                result = await self._get_metagraph()
            except Exception as exc:
                future.set_exception(exc)
                raise
            else:
                future.set_result(result)
                return result
            finally:
                async with self._future_lock:
                    self._metagraph_future = None
        else:
            return await future

    @sync_to_async(thread_sensitive=False)
    def _get_metagraph(self):
        return bittensor.metagraph(netuid=settings.BITTENSOR_NETUID, network=settings.BITTENSOR_NETWORK)


async_metagraph_client = AsyncMetagraphClient()

async def get_miner_axon_info(hotkey: str) -> bittensor.AxonInfo:
    metagraph = await async_metagraph_client.get_metagraph()
    neurons = [n for n in metagraph.neurons if n.hotkey == hotkey]
    if not neurons:
        raise ValueError(f'Miner with {hotkey=} not present in this subnetwork')
    return neurons[0].axon_info
