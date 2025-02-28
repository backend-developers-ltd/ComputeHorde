import abc
import functools
import random
from collections.abc import Sequence

import asgiref.sync
import bittensor
from bittensor.core.errors import SubstrateRequestException
from compute_horde.subtensor import get_cycle_containing_block, get_peak_cycle
from django.conf import settings

from compute_horde_miner.miner.models import ClusterMiner

UNKNOWN_BLOCK_ERROR = -32000


class SelectorException(Exception):
    pass


class NoActiveHotkeysException(SelectorException):
    pass


class BaseSelector(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def active(self, hotkeys: Sequence[str], block: int = None) -> str | None:
        """Select an active hotkey in a specified block (or active block if omitted)."""


class HistoricalRandomMinerSelector(BaseSelector):
    """Simulate `lookback` number of cycles until the current one without repetitions."""

    def __init__(
        self,
        seed: str,
        lookback: int = 10,
    ):
        self._seed = seed
        self._lookback = lookback

    @functools.cached_property
    def subtensor(self) -> bittensor.Subtensor:
        return bittensor.Subtensor(network=settings.BITTENSOR_NETWORK)

    @functools.cached_property
    def subtensor_archive(self) -> bittensor.Subtensor:
        return bittensor.Subtensor(network="archive")

    @asgiref.sync.sync_to_async
    def fetch_current_block(self) -> int:
        return self.subtensor.get_current_block()  # type: ignore

    @asgiref.sync.sync_to_async
    def fetch_neurons(self, netuid: int, block: int) -> list[bittensor.NeuronInfoLite]:
        try:
            return self.subtensor.neurons_lite(netuid, block)  # type: ignore
        except SubstrateRequestException as e:
            if e.args[0]["code"] != UNKNOWN_BLOCK_ERROR:
                raise

            return self.subtensor_archive.neurons_lite(netuid, block)  # type: ignore

    async def filter_active_hotkeys(self, hotkeys: Sequence[str], block: int) -> list[str]:
        active_hotkeys = [
            miner
            async for miner in ClusterMiner.objects.filter(block=block).values_list(
                "hotkey", flat=True
            )
            if miner in hotkeys
        ]

        if active_hotkeys:
            return active_hotkeys

        neurons = {
            neuron.hotkey
            for neuron in await self.fetch_neurons(netuid=settings.BITTENSOR_NETUID, block=block)
        }

        active_hotkeys = [hotkey for hotkey in hotkeys if hotkey in neurons]

        await ClusterMiner.objects.abulk_create(
            [
                ClusterMiner(
                    hotkey=hotkey,
                    block=block,
                )
                for hotkey in active_hotkeys
            ],
            update_conflicts=True,
            unique_fields=[
                "hotkey",
            ],
            update_fields=[
                "block",
            ],
        )

        return active_hotkeys

    async def active(self, hotkeys: Sequence[str], block: int | None = None) -> str | None:
        if not block:
            block = await self.fetch_current_block()

        cycle = get_cycle_containing_block(block=block, netuid=settings.BITTENSOR_NETUID)

        active_hotkeys = await self.filter_active_hotkeys(hotkeys, cycle.start)
        active_hotkeys = sorted(active_hotkeys)

        if not active_hotkeys:
            raise NoActiveHotkeysException()

        if len(active_hotkeys) == 1:
            return active_hotkeys[0]

        peak_cycle = get_peak_cycle(block=block, netuid=settings.BITTENSOR_NETUID)
        peak_cycles = [
            peak_cycle,
        ]

        for i in range(self._lookback):
            peak_cycles.append(
                get_peak_cycle(block=peak_cycles[i].start - 1, netuid=settings.BITTENSOR_NETUID)
            )

        generators = [
            random.Random(f"{self._seed}+{peak_cycle.start}") for peak_cycle in peak_cycles
        ]

        selected = None

        for generator in reversed(generators):
            selected = generator.choice([hotkey for hotkey in active_hotkeys if hotkey != selected])

        return selected
