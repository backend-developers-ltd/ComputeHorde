import abc
import asyncio
import logging
import random
from collections.abc import Awaitable, Callable
from typing import Generic, TypeAlias, TypeVar

from pydantic import BaseModel, ValidationError

from compute_horde.protocol_messages import GenericError
from compute_horde.transport import AbstractTransport, TransportConnectionError

logger = logging.getLogger(__name__)
ErrorCallback: TypeAlias = Callable[[str], Awaitable[None]]

FromMinerType = TypeVar("FromMinerType", bound=BaseModel)
ToMinerType = TypeVar("ToMinerType", bound=BaseModel)


class AbstractMinerClient(Generic[FromMinerType, ToMinerType], metaclass=abc.ABCMeta):
    def __init__(self, miner_name: str, transport: AbstractTransport):
        self.miner_name = miner_name
        self.read_messages_task: asyncio.Task[None] | None = None
        self.deferred_send_tasks: list[asyncio.Task[None]] = []
        self.transport = transport

    @abc.abstractmethod
    def miner_url(self) -> str: ...

    @abc.abstractmethod
    def parse_message(self, raw_msg: str | bytes) -> FromMinerType:
        """Parse raw message into a pydantic model"""

    @abc.abstractmethod
    async def handle_message(self, msg: FromMinerType) -> None:
        """
        Handle the message based on its type or raise UnsupportedMessageReceived
        """
        ...

    async def connect(self) -> None:
        logger.debug(f"Connecting to miner {self.miner_name} at {self.miner_url()}")
        await self.transport.start()
        if self.read_messages_task is None or self.read_messages_task.done():
            self.read_messages_task = asyncio.create_task(self.read_messages())
        logger.debug(f"Connected to miner {self.miner_name} at {self.miner_url()}")

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def close(self) -> None:
        logger.debug(f"Closing connection to miner {self.miner_name} at {self.miner_url()}")
        for deferred_send_task in self.deferred_send_tasks:
            if not deferred_send_task.done():
                deferred_send_task.cancel()
                try:
                    await deferred_send_task
                except asyncio.CancelledError:
                    pass
                except Exception as ex:
                    logger.debug("Exception raised on task cancel: %r", ex)

        if self.read_messages_task is not None and not self.read_messages_task.done():
            self.read_messages_task.cancel()
            try:
                await self.read_messages_task
            except asyncio.CancelledError:
                pass
            except Exception as ex:
                logger.debug("Exception raised on task cancel: %r", ex)

        await self.transport.stop()
        logger.debug(f"Closed connection to miner {self.miner_name} at {self.miner_url()}")

    async def send_model(
        self, model: ToMinerType, error_event_callback: ErrorCallback | None = None
    ) -> None:
        logger.debug(f"Sending model {model} to miner {self.miner_name}")
        await self.send(model.model_dump_json(), error_event_callback)

    async def send(
        self, data: str | bytes, error_event_callback: ErrorCallback | None = None
    ) -> None:
        while True:
            try:
                await self.transport.send(data)
            except TransportConnectionError as ex:
                msg = f"Could not send to miner {self.miner_name}: {str(ex)}"
                logger.warning(msg)
                if error_event_callback:
                    try:
                        await error_event_callback(msg)
                    except Exception as callback_ex:
                        logger.error("Could not execute error event callback: %s", str(callback_ex))
                await asyncio.sleep(1 + random.random())
                continue
            return

    def deferred_send_model(self, model: ToMinerType) -> None:
        task = asyncio.create_task(self.send_model(model))
        self.deferred_send_tasks.append(task)

    async def read_messages(self) -> None:
        async for raw_msg in self.transport:
            try:
                msg = self.parse_message(raw_msg)
            except ValidationError as ex:
                if raw_msg == "PING":
                    continue
                error_msg = f"Malformed message {raw_msg} from miner {self.miner_name}: {ex.json()}"
                logger.info(error_msg)
                self.deferred_send_model(GenericError(details=error_msg))  # type: ignore[arg-type]
                continue

            try:
                await self.handle_message(msg)
            except UnsupportedMessageReceived:
                error_msg = f"Unsupported message from miner {self.miner_name}: {type(msg)}"
                logger.error(error_msg)
                self.deferred_send_model(GenericError(details=error_msg))  # type: ignore[arg-type]


class UnsupportedMessageReceived(Exception):
    def __init__(self, msg: BaseModel):
        self.msg = msg

    def __str__(self):
        return f"{type(self).__name__}: {self.msg.model_dump_json()}"

    __repr__ = __str__
