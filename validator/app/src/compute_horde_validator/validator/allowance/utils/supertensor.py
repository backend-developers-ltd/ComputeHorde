import abc
import asyncio
import contextvars
import datetime
import functools
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

import bittensor_wallet
import turbobt
from bt_ddos_shield.shield_metagraph import ShieldMetagraphOptions
from bt_ddos_shield.turbobt import ShieldedBittensor
from compute_horde.blockchain.block_cache import get_current_block

DEFAULT_TIMEOUT = 30.0

# Context variables for bittensor and subnet
bittensor_context: contextvars.ContextVar[turbobt.Bittensor] = contextvars.ContextVar("bittensor")
subnet_context: contextvars.ContextVar[turbobt.subnet.SubnetReference] = contextvars.ContextVar(
    "subnet"
)


class SuperTensorTimeout(TimeoutError):
    pass


T = TypeVar("T")
P = TypeVar("P")


def make_sync(func: Callable[..., Awaitable[T]]) -> Callable[..., T]:
    @functools.wraps(func)
    def wrapper(s: "SuperTensor", *args, **kwargs) -> T:
        try:
            return s.loop.run_until_complete(
                asyncio.wait_for(func(s, *args, **kwargs), timeout=DEFAULT_TIMEOUT)
            )
        except TimeoutError as ex:
            raise SuperTensorTimeout from ex

    return wrapper


F = TypeVar("F", bound=Callable[..., Any])


def archive_fallback(func: F) -> F:
    @functools.wraps(func)
    def wrapper(s: "SuperTensor", *args, **kwargs):
        try:
            # Set context variables and call the function
            bittensor_context.set(s.bittensor)
            subnet_context.set(s.subnet)
            return func(s, *args, **kwargs)
        except turbobt.substrate.exceptions.UnknownBlock:
            # Set archive context variables and call the function
            bittensor_context.set(s.archive_bittensor)
            subnet_context.set(s.archive_subnet)
            return func(s, *args, **kwargs)

    return wrapper  # type: ignore[return-value]


class BaseSuperTensor(abc.ABC):
    @abc.abstractmethod
    def list_neurons(self, block_number: int) -> list[turbobt.Neuron]: ...

    @abc.abstractmethod
    def list_validators(self, block_number: int) -> list[turbobt.Neuron]: ...

    @abc.abstractmethod
    def get_block_timestamp(self, block_number: int) -> datetime.datetime: ...

    @abc.abstractmethod
    def get_shielded_neurons(self) -> list[turbobt.Neuron]: ...

    @abc.abstractmethod
    def get_current_block(self) -> int: ...

    @abc.abstractmethod
    def wallet(self) -> bittensor_wallet.Wallet: ...


class SuperTensor(BaseSuperTensor):
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

            network = settings.BITTENSOR_NETWORK
        if netuid is None:
            from django.conf import settings

            netuid = settings.BITTENSOR_NETUID
        if archive_network is None:
            from django.conf import settings

            archive_network = settings.BITTENSOR_ARCHIVE_NETWORK
        if wallet is None:
            from django.conf import settings

            wallet = settings.BITTENSOR_WALLET()
        if shield_metagraph_options is None:
            from django.conf import settings

            shield_metagraph_options = settings.BITTENSOR_SHIELD_METAGRAPH_OPTIONS()

        self.network = network
        self.netuid = netuid
        self._wallet = wallet
        self._shield_metagraph_options = shield_metagraph_options

        self.bittensor = turbobt.Bittensor(self.network)
        self.subnet = self.bittensor.subnet(netuid)

        self.archive_bittensor = turbobt.Bittensor(archive_network)
        self.archive_subnet = self.archive_bittensor.subnet(netuid)

        self.loop = asyncio.get_event_loop()

    @archive_fallback
    @make_sync
    async def list_neurons(self, block_number: int) -> list[turbobt.Neuron]:
        bittensor = bittensor_context.get()
        subnet = subnet_context.get()
        async with bittensor.block(block_number):
            result: list[turbobt.Neuron] = await subnet.list_neurons()
            return result

    @archive_fallback
    @make_sync
    async def list_validators(self, block_number: int) -> list[turbobt.Neuron]:
        bittensor = bittensor_context.get()
        subnet = subnet_context.get()
        async with bittensor.block(block_number):
            result: list[turbobt.Neuron] = await subnet.list_validators()
            return result

    @archive_fallback
    @make_sync
    async def get_block_timestamp(self, block_num):
        bittensor = bittensor_context.get()
        async with bittensor.block(block_num) as block:
            return await block.get_timestamp()

    @make_sync
    async def get_shielded_neurons(self) -> list[turbobt.Neuron]:
        # instantiate ShieldedBittensor lazily and keep the reference
        async with ShieldedBittensor(
            self.network,
            ddos_shield_netuid=self.netuid,
            ddos_shield_options=self._shield_metagraph_options,
            wallet=self.wallet(),
        ) as bittensor:
            result: list[turbobt.Neuron] = await bittensor.subnet(self.netuid).subnet.list_neurons()
            return result

    def wallet(self) -> bittensor_wallet.Wallet:
        return self._wallet

    def get_current_block(self) -> int:
        return get_current_block()


_supertensor_instance: SuperTensor | None = None


def supertensor() -> SuperTensor:
    global _supertensor_instance
    if _supertensor_instance is None:
        _supertensor_instance = SuperTensor()
    return _supertensor_instance
