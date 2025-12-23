import abc
import asyncio
import concurrent.futures
import contextvars
import datetime
import enum
import functools
import logging
import threading
import time
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
from compute_horde.utils import MIN_VALIDATOR_STAKE, VALIDATORS_LIMIT
from pylon.v1 import GetNeuronsResponse, Neuron
from turbobt.subtensor.runtime.subnet_info import SubnetHyperparams

from compute_horde_validator.validator.allowance.types import MetagraphData, ValidatorModel
from compute_horde_validator.validator.pylon import pylon_client

DEFAULT_TIMEOUT = 30.0

T = TypeVar("T")
P = TypeVar("P")


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


class AsyncContextError(SuperTensorError):
    """Raised when SuperTensor sync methods are called from async contexts."""

    pass


class SuperTensorClosed(SuperTensorError):
    """Raised when SuperTensor methods are called after close()."""

    pass


class SuperTensorNotInitialized(SuperTensorError):
    """Raised when SuperTensor background loop is not initialized."""

    pass


# Tenacity retry policy: up to 3 attempts, only on SuperTensorTimeout, with small backoff
RETRY_ON_TIMEOUT = tenacity.retry(
    reraise=True,
    retry=tenacity.retry_if_exception_type(SuperTensorTimeout),
    stop=tenacity.stop_after_attempt(3),
    wait=tenacity.wait_exponential(multiplier=0.1, min=0.1, max=0.8),
)


def make_sync(func: Callable[..., Awaitable[T]]) -> Callable[..., T]:
    @functools.wraps(func)
    def wrapper(s: "SuperTensor", *args, **kwargs) -> T:
        # Guard against direct async usage - enforce sync_to_async pattern
        try:
            asyncio.get_running_loop()
            raise AsyncContextError(
                f"SuperTensor.{func.__name__}() cannot be called from async contexts. "
                f"Call the async variant directly (await ...) or wrap with asgiref.sync.sync_to_async."
            )
        except RuntimeError:
            # No running loop - this is expected for sync contexts
            pass

        # Check if SuperTensor has been closed
        if s._closed:
            raise SuperTensorClosed("SuperTensor has been closed")

        # Dispatch coroutine to dedicated background loop
        if s.loop is None:
            raise SuperTensorNotInitialized("SuperTensor background loop not initialized")

        coro = func(s, *args, **kwargs)
        future = asyncio.run_coroutine_threadsafe(coro, s.loop)

        try:
            return future.result(timeout=DEFAULT_TIMEOUT)
        except concurrent.futures.TimeoutError as ex:
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
            if s.archive_bittensor is None:
                raise ArchiveSubtensorNotConfigured
            # Set archive context variables and call the function
            bittensor_context.set(s.archive_bittensor)
            subnet_context.set(s.archive_subnet)
            return func(s, *args, **kwargs)

    return wrapper  # type: ignore[return-value]


class BaseSuperTensor(abc.ABC):
    @abc.abstractmethod
    def list_neurons(self, block_number: int) -> list[Neuron]: ...

    @abc.abstractmethod
    def list_validators(self, block_number: int) -> list[ValidatorModel]: ...

    @abc.abstractmethod
    def get_metagraph(self, block_number: int | None = None) -> MetagraphData: ...

    @abc.abstractmethod
    def get_block_timestamp(self, block_number: int) -> datetime.datetime: ...

    @abc.abstractmethod
    def get_block_hash(self, block_number: int) -> str: ...

    @abc.abstractmethod
    def get_shielded_neurons(self) -> list[turbobt.Neuron]: ...

    @abc.abstractmethod
    def get_current_exact_block(self) -> int: ...

    def get_current_block(self) -> int:
        return self.get_current_exact_block() - 5

    @abc.abstractmethod
    def wallet(self) -> bittensor_wallet.Wallet: ...

    @abc.abstractmethod
    def oldest_reachable_block(self) -> float | int: ...

    @abc.abstractmethod
    def get_commitments(self, block_number: int) -> dict[str, bytes]: ...

    @abc.abstractmethod
    def get_hyperparameters(self, block_number: int) -> SubnetHyperparams | None: ...


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

        self._closed = False
        self.loop: asyncio.AbstractEventLoop | None = None
        self._background_thread: threading.Thread | None = None
        self._loop_ready = threading.Event()
        self._setup_background_loop()

    def _setup_background_loop(self) -> None:
        """Set up dedicated background event loop for all async operations."""

        def loop_runner() -> None:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            self.loop = loop

            self._loop_ready.set()

            try:
                loop.run_forever()
            finally:
                loop.close()

        self._background_thread = threading.Thread(
            target=loop_runner, daemon=True, name=f"SuperTensor-{id(self)}"
        )
        self._background_thread.start()

        self._loop_ready.wait(timeout=5.0)

    def oldest_reachable_block(self) -> float | int:
        if self.archive_bittensor is not None:
            return float("-inf")
        return self.get_current_block() - LITE_BLOCK_LOOKBACK

    def get_neurons(self, block_number: int) -> GetNeuronsResponse:
        with pylon_client() as client:
            return client.identity.get_neurons(block_number)

    def list_neurons(self, block_number: int) -> list[Neuron]:
        return list(self.get_neurons(block_number).neurons.values())

    def list_validators(self, block_number: int) -> list[ValidatorModel]:
        neurons = self.list_neurons(block_number)

        # Filter out neurons with a stake lower than MIN_VALIDATOR_STAKE
        maybe_validators = [
            ValidatorModel(
                uid=neuron.uid, hotkey=neuron.hotkey, effective_stake=neuron.stakes.total
            )
            for neuron in neurons
            if neuron.stakes.total >= MIN_VALIDATOR_STAKE
        ]

        # We accept up to VALIDATORS_LIMIT validators, preferring the ones with the highest stake
        maybe_validators.sort(key=lambda v: v.effective_stake, reverse=True)
        validators = maybe_validators[:VALIDATORS_LIMIT]

        return validators

    def get_metagraph(self, block_number: int | None = None) -> MetagraphData:
        if block_number is None:
            block_number = self.get_current_block()
        neurons_data = self.get_neurons(block_number)
        neurons_sorted = sorted(neurons_data.neurons.values(), key=lambda n: n.uid)
        total_stake = [neuron.stakes.total for neuron in neurons_sorted]
        uids = [neuron.uid for neuron in neurons_sorted]
        hotkeys = [neuron.hotkey for neuron in neurons_sorted]
        serving_hotkeys = [
            neuron.hotkey
            for neuron in neurons_sorted
            if neuron.axon_info and str(neuron.axon_info.ip) != "0.0.0.0"
        ]

        return MetagraphData.model_construct(
            block=neurons_data.block.number,
            block_hash=neurons_data.block.hash,
            total_stake=total_stake,
            uids=uids,
            hotkeys=hotkeys,
            serving_hotkeys=serving_hotkeys,
        )

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

    def get_commitments(self, block_number: int) -> dict[str, bytes]:
        with pylon_client() as client:
            response = client.identity.get_commitments()
            return {
                hotkey: bytes.fromhex(commitment.removeprefix("0x"))
                for hotkey, commitment in response.commitments.items()
            }

    @archive_fallback
    @make_sync
    async def _get_hyperparameters(self, block_number: int) -> SubnetHyperparams | None:
        bittensor = bittensor_context.get()
        subnet = subnet_context.get()
        async with bittensor.block(block_number):
            result: SubnetHyperparams | None = await subnet.get_hyperparameters()
            logger.debug(f"Hyperparameters for block {block_number}: {result}")
            return result

    @RETRY_ON_TIMEOUT
    def get_hyperparameters(self, block_number: int) -> SubnetHyperparams | None:
        return self._get_hyperparameters(block_number)

    @archive_fallback
    @make_sync
    async def _get_block_hash(self, block_number: int) -> str:
        bittensor = bittensor_context.get()
        async with bittensor.block(block_number) as block:
            return str(block.hash)

    @RETRY_ON_TIMEOUT
    def get_block_hash(self, block_number: int) -> str:
        return self._get_block_hash(block_number)

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

    def get_current_exact_block(self) -> int:
        try:
            current_block = get_current_block()
        except websockets.exceptions.ConcurrencyError as ex:
            raise CannotGetCurrentBlock("Cannot get current block from blockchain") from ex
        return current_block

    def close(self):
        if self._closed or self.loop is None or self._background_thread is None:
            return

        self._closed = True

        async def _close_resources():
            await self.bittensor.close()
            if self.archive_bittensor is not None:
                await self.archive_bittensor.close()

        future = asyncio.run_coroutine_threadsafe(_close_resources(), self.loop)

        try:
            future.result(timeout=DEFAULT_TIMEOUT)
        except TimeoutError:
            logger.warning("SuperTensor resource cleanup timed out")
        except Exception as e:
            logger.error(f"Error during SuperTensor resource cleanup: {e}")

        self.loop.call_soon_threadsafe(self.loop.stop)

        if self._background_thread.is_alive():
            self._background_thread.join(timeout=5.0)
            if self._background_thread.is_alive():
                logger.warning("SuperTensor background thread did not shut down cleanly")

        self.loop = None
        self._background_thread = None


N_THREADS = 10
CACHE_AHEAD = 10


class TaskType(enum.Enum):
    NEURONS = "NEURONS"
    BLOCK_TIMESTAMP = "BLOCK_TIMESTAMP"
    BLOCK_HASH = "BLOCK_HASH"
    SUBNET_STATE = "SUBNET_STATE"
    VALIDATORS = "VALIDATORS"
    COMMITMENTS = "COMMITMENTS"
    THE_END = "THE_END"


class BaseCache(abc.ABC):
    @abc.abstractmethod
    def put_neurons(self, block_number: int, neurons: list[Neuron]): ...

    @abc.abstractmethod
    def put_block_timestamp(self, block_number: int, timestamp: datetime.datetime): ...

    @abc.abstractmethod
    def put_block_hash(self, block_number: int, block_hash: str): ...

    @abc.abstractmethod
    def get_neurons(self, block_number: int) -> list[Neuron] | None: ...

    @abc.abstractmethod
    def get_block_timestamp(self, block_number: int) -> datetime.datetime | None: ...

    @abc.abstractmethod
    def get_block_hash(self, block_number: int) -> str | None: ...

    @abc.abstractmethod
    def put_subnet_state(self, block_number: int, state: turbobt.subnet.SubnetState): ...

    @abc.abstractmethod
    def get_subnet_state(self, block_number: int) -> turbobt.subnet.SubnetState | None: ...

    @abc.abstractmethod
    def put_validators(self, block_number: int, validators: list[ValidatorModel]): ...

    @abc.abstractmethod
    def get_validators(self, block_number: int) -> list[ValidatorModel] | None: ...

    @abc.abstractmethod
    def put_commitments(self, block_number: int, commitments: dict[str, bytes]): ...

    @abc.abstractmethod
    def get_commitments(self, block_number: int) -> dict[str, bytes] | None: ...


class InMemoryCache(BaseCache):
    def __init__(self):
        self._neuron_cache: dict[int, list[Neuron]] = {}
        self._block_timestamp_cache: dict[int, datetime.datetime] = {}
        self._block_hash_cache: dict[int, str] = {}
        self._subnet_state_cache: dict[int, turbobt.subnet.SubnetState] = {}
        self._validators_cache: dict[int, list[ValidatorModel]] = {}
        self._commitments_cache: dict[int, dict[str, bytes]] = {}

    def put_neurons(self, block_number: int, neurons: list[Neuron]):
        self._neuron_cache[block_number] = neurons

    def put_block_timestamp(self, block_number: int, timestamp: datetime.datetime):
        self._block_timestamp_cache[block_number] = timestamp

    def put_block_hash(self, block_number: int, block_hash: str):
        self._block_hash_cache[block_number] = block_hash

    def get_neurons(self, block_number: int) -> list[Neuron] | None:
        return self._neuron_cache.get(block_number)

    def get_block_timestamp(self, block_number: int) -> datetime.datetime | None:
        return self._block_timestamp_cache.get(block_number)

    def get_block_hash(self, block_number: int) -> str | None:
        return self._block_hash_cache.get(block_number)

    def put_subnet_state(self, block_number: int, state: turbobt.subnet.SubnetState):
        self._subnet_state_cache[block_number] = state

    def get_subnet_state(self, block_number: int) -> turbobt.subnet.SubnetState | None:
        return self._subnet_state_cache.get(block_number)

    def put_validators(self, block_number: int, validators: list[ValidatorModel]):
        self._validators_cache[block_number] = validators

    def get_validators(self, block_number: int) -> list[ValidatorModel] | None:
        return self._validators_cache.get(block_number)

    def put_commitments(self, block_number: int, commitments: dict[str, bytes]):
        self._commitments_cache[block_number] = commitments

    def get_commitments(self, block_number: int) -> dict[str, bytes] | None:
        return self._commitments_cache.get(block_number)


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
        enable_workers: bool = True,
        **kwargs,
    ):
        self.closing = False
        if cache is None:
            cache = InMemoryCache()
        self.cache = cache
        self.throw_on_cache_miss = throw_on_cache_miss
        self.enable_workers = enable_workers
        super().__init__(*args, **kwargs)
        self.task_queue: Queue[tuple[TaskType, int]] = Queue()
        self.highest_block_requested: int | None = None
        self.highest_block_submitted: int | None = None
        if self.enable_workers:
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
                    elif task == TaskType.BLOCK_HASH:
                        if self.cache.get_block_hash(block_number) is not None:
                            logger.debug(
                                f"Worker {ind} skipping task {task} for block {block_number} (cached)"
                            )
                            continue
                        self.cache.put_block_hash(
                            block_number, super_tensor.get_block_hash(block_number)
                        )
                    elif task == TaskType.SUBNET_STATE:
                        if self.cache.get_subnet_state(block_number) is not None:
                            logger.debug(
                                f"Worker {ind} skipping task {task} for block {block_number} (cached)"
                            )
                            continue
                        self.cache.put_subnet_state(
                            block_number, super_tensor.get_subnet_state(block_number)
                        )
                    elif task == TaskType.VALIDATORS:
                        if self.cache.get_validators(block_number) is not None:
                            logger.debug(
                                f"Worker {ind} skipping task {task} for block {block_number} (cached)"
                            )
                            continue
                        self.cache.put_validators(
                            block_number, super_tensor.list_validators(block_number)
                        )
                    elif task == TaskType.COMMITMENTS:
                        if self.cache.get_commitments(block_number) is not None:
                            logger.debug(
                                f"Worker {ind} skipping task {task} for block {block_number} (cached)"
                            )
                            continue
                        self.cache.put_commitments(
                            block_number, super_tensor.get_commitments(block_number)
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
                self.task_queue.put((TaskType.BLOCK_HASH, block_to_submit))
                self.task_queue.put((TaskType.SUBNET_STATE, block_to_submit))
                self.task_queue.put((TaskType.VALIDATORS, block_to_submit))
                self.task_queue.put((TaskType.COMMITMENTS, block_to_submit))
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

    def list_neurons(self, block_number: int) -> list[Neuron]:
        self.set_starting_block(block_number)
        neurons = self.cache.get_neurons(block_number)
        if neurons is not None:
            return neurons
        elif self.throw_on_cache_miss:
            raise PrecachingSuperTensorCacheMiss(f"Cache miss for block {block_number}")
        else:
            logger.debug(f"Cache miss for block {block_number}")
            neurons = super().list_neurons(block_number)
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

    @RETRY_ON_TIMEOUT
    def get_block_hash(self, block_number: int) -> str:
        self.set_starting_block(block_number)
        block_hash = self.cache.get_block_hash(block_number)
        if block_hash is not None:
            return block_hash
        elif self.throw_on_cache_miss:
            raise PrecachingSuperTensorCacheMiss(f"Cache miss for block {block_number}")
        else:
            logger.debug(f"Cache miss for block {block_number}")
            block_hash = super().get_block_hash(block_number)
            self.cache.put_block_hash(block_number, block_hash)
            return block_hash

    @RETRY_ON_TIMEOUT
    def get_subnet_state(self, block_number: int) -> turbobt.subnet.SubnetState:
        self.set_starting_block(block_number)
        state = self.cache.get_subnet_state(block_number)
        if state is not None:
            return state
        elif self.throw_on_cache_miss:
            raise PrecachingSuperTensorCacheMiss(
                f"Cache miss for block {block_number} (subnet state)"
            )
        else:
            logger.debug(f"Cache miss for block {block_number}")
            state = super()._get_subnet_state(block_number)
            self.cache.put_subnet_state(block_number, state)
            return state

    def list_validators(self, block_number: int) -> list[ValidatorModel]:
        self.set_starting_block(block_number)
        validators = self.cache.get_validators(block_number)
        if validators is not None:
            return validators
        elif self.throw_on_cache_miss:
            raise PrecachingSuperTensorCacheMiss(f"Cache miss for block {block_number}")
        else:
            logger.debug(f"Cache miss for block {block_number} (validators)")
            validators = super().list_validators(block_number)
            self.cache.put_validators(block_number, validators)
            return validators

    def close(self):
        self.closing = True
        if self.enable_workers:
            for _ in range(N_THREADS):
                self.task_queue.put((TaskType.THE_END, 0))
        super().close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


_supertensor_instance: SuperTensor | None = None


def supertensor() -> SuperTensor:
    # Return a singleton PrecachingSuperTensor that serves cached allowance data and backfills on misses.
    global _supertensor_instance
    if _supertensor_instance is None:
        from .supertensor_django_cache import DjangoCache

        _supertensor_instance = PrecachingSuperTensor(
            cache=DjangoCache(),
            enable_workers=False,  # Consumer mode - read from cache only
        )
    return _supertensor_instance
