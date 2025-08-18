import abc
import asyncio
import contextvars
import datetime
import enum
import functools
import logging
import threading
import time
from collections import deque
from collections.abc import Awaitable, Callable
from queue import Queue
from typing import Any, TypeVar, assert_never

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


logger = logging.getLogger(__name__)


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
        self.archive_network = archive_network
        self._wallet = wallet
        self._shield_metagraph_options = shield_metagraph_options

        self.bittensor = turbobt.Bittensor(self.network)
        self.subnet = self.bittensor.subnet(netuid)

        self.archive_bittensor = turbobt.Bittensor(self.archive_network)
        self.archive_subnet = self.archive_bittensor.subnet(netuid)

        self.loop = asyncio.get_event_loop()

        self._neuron_list_cache: deque[tuple[int, list[turbobt.Neuron]]] = deque(maxlen=15)

    def list_neurons(self, block_number: int) -> list[turbobt.Neuron]:
        cache = dict(self._neuron_list_cache)
        if hit := cache.get(block_number):
            return hit
        result: list[turbobt.Neuron] = self.real_list_neurons(block_number)
        self._neuron_list_cache.append((block_number, result))
        return result

    @archive_fallback
    @make_sync
    async def real_list_neurons(self, block_number: int) -> list[turbobt.Neuron]:
        bittensor = bittensor_context.get()
        subnet = subnet_context.get()
        async with bittensor.block(block_number):
            result: list[turbobt.Neuron] = await subnet.list_neurons()
            return result

    @archive_fallback
    @make_sync
    async def list_validators(self, block_number: int) -> list[turbobt.Neuron]:
        return [n for n in self.list_neurons(block_number) if n.stake >= 1000]
        # TODO: yes this is reimplementing `subnet.list_validators()` but since turbobt doesn't support caching yet
        # (or i don't know how to use it) it's just too time consuming to be doing it properly

    @archive_fallback
    @make_sync
    async def get_block_timestamp(self, block_num) -> datetime.datetime:
        bittensor = bittensor_context.get()
        async with bittensor.block(block_num) as block:
            ret: datetime.datetime = await block.get_timestamp()
            logger.debug(f"Block {block_num} timestamp: {ret}")
            return ret

    @make_sync
    async def get_shielded_neurons(self) -> list[turbobt.Neuron]:
        # instantiate ShieldedBittensor lazily and keep the reference
        async with ShieldedBittensor(
            self.network,
            ddos_shield_netuid=self.netuid,
            ddos_shield_options=self._shield_metagraph_options,
            wallet=self.wallet(),
        ) as bittensor:
            result: list[turbobt.Neuron] = await bittensor.subnet(self.netuid).list_neurons()
            return result

    def wallet(self) -> bittensor_wallet.Wallet:
        return self._wallet

    def get_current_block(self) -> int:
        return get_current_block() - 5


N_THREADS = 10
CACHE_AHEAD = 10


class TaskType(enum.Enum):
    NEURONS = "NEURONS"
    BLOCK_TIMESTAMP = "BLOCK_TIMESTAMP"


class BaseCache(abc.ABC):
    @abc.abstractmethod
    def put_neurons(self, block_number: int, neurons: list[turbobt.Neuron]): ...

    @abc.abstractmethod
    def put_block_timestamp(self, block_number: int, timestamp: datetime.datetime): ...

    @abc.abstractmethod
    def get_neurons(self, block_number: int) -> list[turbobt.Neuron] | None: ...

    @abc.abstractmethod
    def get_block_timestamp(self, block_number: int) -> datetime.datetime | None: ...


class InMemoryCache(BaseCache):
    def __init__(self):
        self._neuron_cache: dict[int, list[turbobt.Neuron]] = {}
        self._block_timestamp_cache: dict[int, datetime.datetime] = {}

    def put_neurons(self, block_number: int, neurons: list[turbobt.Neuron]):
        self._neuron_cache[block_number] = neurons

    def put_block_timestamp(self, block_number: int, timestamp: datetime.datetime):
        self._block_timestamp_cache[block_number] = timestamp

    def get_neurons(self, block_number: int) -> list[turbobt.Neuron] | None:
        return self._neuron_cache.get(block_number)

    def get_block_timestamp(self, block_number: int) -> datetime.datetime | None:
        return self._block_timestamp_cache.get(block_number)


class PrecachingSuperTensor(SuperTensor):
    """
    SuperTensor that tries to do clever forward caching.
    """

    def __init__(
        self,
        *args,
        cache: BaseCache | None = None,
        throw_on_cache_miss: bool = False,
        **kwargs,
    ):
        if cache is None:
            cache = InMemoryCache()
        self.cache = cache
        self.throw_on_cache_miss = throw_on_cache_miss
        super().__init__(*args, **kwargs)
        self.task_queue: Queue[tuple[TaskType, int]] = Queue()
        self.highest_block_requested: int | None = None
        self.highest_block_submitted: int | None = None
        self.start_workers()
        self.start_producer()

    def worker(self, ind: int):
        while True:
            try:
                asyncio.set_event_loop(asyncio.new_event_loop())
                super_tensor = SuperTensor(
                    network=self.network,
                    archive_network=self.archive_network,
                    netuid=self.netuid,
                    wallet=self._wallet,
                    shield_metagraph_options=self._shield_metagraph_options,
                )
                while True:
                    task: TaskType
                    block_number: int
                    task, block_number = self.task_queue.get()
                    logger.debug(f"Worker {ind} processing task {task} for block {block_number}")
                    if task == TaskType.NEURONS:
                        if self.cache.get_neurons(block_number) is not None:
                            logger.debug(
                                f"Worker {ind} skipping task {task} for block {block_number} (cached)"
                            )
                            continue
                        self.cache.put_neurons(
                            block_number, super_tensor.list_neurons(block_number)
                        )
                    elif task == TaskType.BLOCK_TIMESTAMP:
                        if self.cache.get_block_timestamp(block_number) is not None:
                            logger.debug(
                                f"Worker {ind} skipping task {task} for block {block_number} (cached)"
                            )
                            continue
                        self.cache.put_block_timestamp(
                            block_number, super_tensor.get_block_timestamp(block_number)
                        )
                    else:
                        assert_never(task)
                    logger.debug(f"Worker {ind} finished task {task} for block {block_number}")
            except Exception as e:
                logger.error(f"Error in worker ({ind=}) thread: {e}", exc_info=True)
                time.sleep(1)

    def start_workers(self):
        for ind in range(N_THREADS):
            threading.Thread(target=self.worker, args=(ind,), daemon=True).start()

    def produce_thread(self):
        while True:
            try:
                current_block = self.get_current_block()
                if self.highest_block_requested is None or self.highest_block_submitted is None:
                    time.sleep(0.1)
                    continue
                if self.highest_block_submitted - self.highest_block_requested >= CACHE_AHEAD:
                    time.sleep(0.1)
                    continue
                if self.highest_block_submitted >= current_block:
                    time.sleep(0.1)
                    continue
                block_to_submit = self.highest_block_submitted + 1
                logger.debug(f"Submitting tasks for block {block_to_submit}")
                self.task_queue.put((TaskType.NEURONS, block_to_submit))
                self.task_queue.put((TaskType.BLOCK_TIMESTAMP, block_to_submit))
                self.highest_block_submitted = block_to_submit
            except Exception as e:
                logger.error(f"Error in producer thread: {e}", exc_info=True)
                time.sleep(1)

    def start_producer(self):
        threading.Thread(target=self.produce_thread, daemon=True).start()

    def set_starting_block(self, block_number: int):
        self.highest_block_requested = max(self.highest_block_requested or 0, block_number)
        if self.highest_block_submitted is None:
            self.highest_block_submitted = block_number - 1

    def list_neurons(self, block_number: int) -> list[turbobt.Neuron]:
        self.set_starting_block(block_number)
        neurons = self.cache.get_neurons(block_number)
        if neurons is not None:
            return neurons
        elif self.throw_on_cache_miss:
            raise SuperTensorTimeout(f"Cache miss for block {block_number}")
        else:
            logger.debug(f"Cache miss for block {block_number}")
            return super().list_neurons(block_number)

    def get_block_timestamp(self, block_number: int) -> datetime.datetime:
        self.set_starting_block(block_number)
        timestamp = self.cache.get_block_timestamp(block_number)
        if timestamp is not None:
            return timestamp
        elif self.throw_on_cache_miss:
            raise SuperTensorTimeout(f"Cache miss for block {block_number}")
        else:
            logger.debug(f"Cache miss for block {block_number}")
            return super().get_block_timestamp(block_number)


_supertensor_instance: SuperTensor | None = None


def supertensor() -> SuperTensor:
    global _supertensor_instance
    if _supertensor_instance is None:
        _supertensor_instance = SuperTensor()
    return _supertensor_instance
