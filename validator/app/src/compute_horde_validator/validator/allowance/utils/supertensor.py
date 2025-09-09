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
import tenacity
import turbobt
import websockets
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


class SuperTensorError(Exception):
    pass


class ArchiveSubtensorNotConfigured(SuperTensorError):
    pass


class SuperTensorTimeout(SuperTensorError, TimeoutError):
    pass


class CannotGetCurrentBlock(SuperTensorError):
    pass


# Tenacity retry policy: up to 3 attempts, only on SuperTensorTimeout, with small backoff
RETRY_ON_TIMEOUT = tenacity.retry(
    reraise=True,
    retry=tenacity.retry_if_exception_type(SuperTensorTimeout),
    stop=tenacity.stop_after_attempt(3),
    wait=tenacity.wait_exponential(multiplier=0.1, min=0.1, max=0.8),
)


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
        except RuntimeError:
            raise

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
            if s.archive_bittensor is None:
                raise ArchiveSubtensorNotConfigured
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

    @abc.abstractmethod
    def oldest_reachable_block(self) -> float | int: ...


LITE_BLOCK_LOOKBACK = 200


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

        if self.archive_network:
            self.archive_bittensor = turbobt.Bittensor(self.archive_network)
            self.archive_subnet = self.archive_bittensor.subnet(netuid)
        else:
            self.archive_bittensor = None
            self.archive_subnet = None

        self.loop = asyncio.get_event_loop()

        self._neuron_list_cache: deque[tuple[int, list[turbobt.Neuron]]] = deque(maxlen=15)

    def oldest_reachable_block(self) -> float | int:
        if self.archive_bittensor is not None:
            return float("-inf")
        return self.get_current_block() - LITE_BLOCK_LOOKBACK

    def _list_neurons(self, block_number: int) -> list[turbobt.Neuron]:
        cache = dict(self._neuron_list_cache)
        if hit := cache.get(block_number):
            return hit
        result: list[turbobt.Neuron] = self._real_list_neurons(block_number)
        self._neuron_list_cache.append((block_number, result))
        return result

    @RETRY_ON_TIMEOUT
    def list_neurons(self, block_number: int) -> list[turbobt.Neuron]:
        return self._list_neurons(block_number)

    @archive_fallback
    @make_sync
    async def _real_list_neurons(self, block_number: int) -> list[turbobt.Neuron]:
        bittensor = bittensor_context.get()
        subnet = subnet_context.get()
        async with bittensor.block(block_number):
            result: list[turbobt.Neuron] = await subnet.list_neurons()
            return result

    @RETRY_ON_TIMEOUT
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
    async def _get_block_timestamp(self, block_num) -> datetime.datetime:
        bittensor = bittensor_context.get()
        async with bittensor.block(block_num) as block:
            ret: datetime.datetime = await block.get_timestamp()
            logger.debug(f"Block {block_num} timestamp: {ret}")
            return ret

    @RETRY_ON_TIMEOUT
    def get_block_timestamp(self, block_number: int) -> datetime.datetime:
        return self._get_block_timestamp(block_number)

    @archive_fallback
    @make_sync
    async def _get_subnet_state(self, block_number: int) -> turbobt.subnet.SubnetState:
        bittensor = bittensor_context.get()
        subnet = subnet_context.get()
        async with bittensor.block(block_number):
            return await subnet.get_state()

    @RETRY_ON_TIMEOUT
    def get_subnet_state(self, block_number: int) -> turbobt.subnet.SubnetState:
        return self._get_subnet_state(block_number)

    @RETRY_ON_TIMEOUT
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
        try:
            current_block = get_current_block()
        except websockets.exceptions.ConcurrencyError as ex:
            raise CannotGetCurrentBlock("Cannot get current block from blockchain") from ex
        return current_block - 5

    def close(self):
        self.loop.run_until_complete(self.bittensor.close())
        if self.archive_bittensor is not None:
            self.loop.run_until_complete(self.archive_bittensor.close())


N_THREADS = 10
CACHE_AHEAD = 10


class TaskType(enum.Enum):
    NEURONS = "NEURONS"
    BLOCK_TIMESTAMP = "BLOCK_TIMESTAMP"
    THE_END = "THE_END"


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


class PrecachingSuperTensorCacheMiss(SuperTensorError):
    pass


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
        self.closing = False
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
            asyncio.set_event_loop(asyncio.new_event_loop())
            super_tensor = SuperTensor(
                network=self.network,
                archive_network=self.archive_network,
                netuid=self.netuid,
                wallet=self._wallet,
                shield_metagraph_options=self._shield_metagraph_options,
            )
            try:
                while True:
                    task: TaskType
                    block_number: int
                    if self.closing:
                        logger.debug(f"Worker {ind} quitting")
                        return
                    task, block_number = self.task_queue.get()
                    logger.debug(f"Worker {ind} processing task {task} for block {block_number}")
                    if task == TaskType.THE_END:
                        logger.debug(f"Worker {ind} quitting")
                        return
                    elif task == TaskType.NEURONS:
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
            except ArchiveSubtensorNotConfigured:
                pass
            except Exception as e:
                logger.error(f"Error in worker ({ind=}) thread: {e}", exc_info=True)
                time.sleep(1)
            finally:
                super_tensor.close()

    def start_workers(self):
        for ind in range(N_THREADS):
            threading.Thread(target=self.worker, args=(ind,), daemon=True).start()

    def produce_thread(self):
        while True:
            try:
                if self.closing:
                    logger.debug("Producer quitting")
                    return
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
            except CannotGetCurrentBlock:
                time.sleep(1)
                continue
            except Exception as e:
                logger.error(f"Error in producer thread: {e}", exc_info=True)
                time.sleep(1)

    def start_producer(self):
        threading.Thread(target=self.produce_thread, daemon=True).start()

    def set_starting_block(self, block_number: int):
        self.highest_block_requested = max(self.highest_block_requested or 0, block_number)
        if self.highest_block_submitted is None:
            self.highest_block_submitted = block_number - 1

    def _list_neurons(self, block_number: int) -> list[turbobt.Neuron]:
        self.set_starting_block(block_number)
        neurons = self.cache.get_neurons(block_number)
        if neurons is not None:
            return neurons
        elif self.throw_on_cache_miss:
            raise PrecachingSuperTensorCacheMiss(f"Cache miss for block {block_number}")
        else:
            logger.debug(f"Cache miss for block {block_number}")
            neurons = super()._list_neurons(block_number)
            self.cache.put_neurons(block_number, neurons)
            return neurons

    @RETRY_ON_TIMEOUT
    def get_block_timestamp(self, block_number: int) -> datetime.datetime:
        self.set_starting_block(block_number)
        timestamp = self.cache.get_block_timestamp(block_number)
        if timestamp is not None:
            return timestamp
        elif self.throw_on_cache_miss:
            raise PrecachingSuperTensorCacheMiss(f"Cache miss for block {block_number}")
        else:
            logger.debug(f"Cache miss for block {block_number}")
            return super()._get_block_timestamp(block_number)

    def close(self):
        self.closing = True
        for _ in range(N_THREADS):
            self.task_queue.put((TaskType.THE_END, 0))
        super().close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


_supertensor_instance: SuperTensor | None = None


def supertensor() -> SuperTensor:
    global _supertensor_instance
    if _supertensor_instance is None:
        _supertensor_instance = SuperTensor()
    return _supertensor_instance
