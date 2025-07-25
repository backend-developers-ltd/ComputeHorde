import asyncio
import functools
from functools import lru_cache

import bittensor_wallet
import turbobt

from bt_ddos_shield.turbobt import ShieldedBittensor
from bt_ddos_shield.shield_metagraph import ShieldMetagraphOptions

from compute_horde.blockchain.block_cache import get_current_block

DEFAULT_TIMEOUT = 30.0

class SuperTensorTimeout(TimeoutError):
    pass

def make_sync(func):
    @functools.wraps(func)
    def wrapper(s: 'SuperTensor', *args, **kwargs):
        try:
            return s.loop.run_until_complete(asyncio.wait_for(func(s, *args, **kwargs), timeout=DEFAULT_TIMEOUT))
        except TimeoutError as ex:
            raise SuperTensorTimeout from ex
    return wrapper


def archive_fallback(func):
    @functools.wraps(func)
    def wrapper(s: 'SuperTensor', *args, **kwargs):
        try:
            return func(s, s.bittensor, s.subnet, *args, **kwargs)
        except turbobt.substrate.exceptions.UnknownBlock:
            return func(s, s.archive_bittensor, s.archive_subnet, *args, **kwargs)
    return wrapper


class SuperTensor:
    def __init__(
            self,
            network: str | None = None,
            archive_network: str | None = None,
            netuid: int | None = None,
            wallet: bittensor_wallet.Wallet | None = None,
            shield_metagraph_options: ShieldMetagraphOptions | None = None,
    ):
        if network is None:
            from django.conf import settings
            self.network = settings.BITTENSOR_NETWORK
        if netuid is None:
            from django.conf import settings
            self.netuid = settings.BITTENSOR_NETUID
        if archive_network is None:
            from django.conf import settings
            archive_network = settings.BITTENSOR_ARCHIVE_NETWORK
        if wallet is None:
            from django.conf import settings
            self.wallet = settings.BITTENSOR_WALLET()
        if shield_metagraph_options is None:
            from django.conf import settings
            self.shield_metagraph_options = settings.SHIELD_METAGRAPH_OPTIONS()

        self.bittensor = turbobt.Bittensor(self.network)
        self.subnet = self.bittensor.subnet(netuid)

        self.archive_bittensor = turbobt.Bittensor(archive_network)
        self.archive_subnet = self.archive_bittensor.subnet(netuid)

        self.loop = asyncio.get_event_loop()

    @archive_fallback
    @make_sync
    async def list_neurons(self, bittensor: turbobt.Bittensor, subnet: turbobt.Subnet, block_number: int) -> list[turbobt.Neuron]:
        async with bittensor.block(block_number):
            return await subnet.list_neurons()

    @archive_fallback
    @make_sync
    async def list_validators(self, bittensor: turbobt.Bittensor, subnet: turbobt.Subnet, block_number: int) -> list[turbobt.Neuron]:
        async with bittensor.block(block_number):
            return await subnet.list_validators()

    @archive_fallback
    @make_sync
    async def get_block_timestamp(self, bittensor: turbobt.Bittensor, subnet: turbobt.Subnet, block_num):
        async with bittensor.block(block_num) as block:
            return await block.get_timestamp()

    @make_sync
    async def get_shielded_neurons(self) -> list[turbobt.Neuron]:
        async with ShieldedBittensor(
            self.network,
            ddos_shield_netuid=self.netuid,
            ddos_shield_options=self.shield_metagraph_options,
            wallet=self.wallet,
        ) as bittensor:
            return await bittensor.subnet(self.netuid).subnet.list_neurons()

    def get_current_block(self) -> int:
        return get_current_block()


@lru_cache(maxsize=1)
def supertensor():
    return SuperTensor()
