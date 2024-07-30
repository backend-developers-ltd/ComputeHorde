import abc
import asyncio
import logging
import random

from compute_horde.base_requests import BaseRequest, ValidationError
from compute_horde.transport import AbstractTransport, TransportConnectionError

logger = logging.getLogger(__name__)


class AbstractMinerClient(abc.ABC):
    def __init__(self, miner_name: str, transport: AbstractTransport):
        self.miner_name = miner_name
        self.read_messages_task: asyncio.Task | None = None
        self.deferred_send_tasks: list[asyncio.Task] = []
        self.transport = transport

    @abc.abstractmethod
    def miner_url(self) -> str: ...

    @abc.abstractmethod
    def accepted_request_type(self) -> type[BaseRequest]:
        pass

    @abc.abstractmethod
    def incoming_generic_error_class(self) -> type[BaseRequest]:
        pass

    @abc.abstractmethod
    def outgoing_generic_error_class(self) -> type[BaseRequest]:
        pass

    @abc.abstractmethod
    async def handle_message(self, msg: BaseRequest):
        """
        Handle the message based on its type or raise UnsupportedMessageReceived
        """
        ...

    async def connect(self):
        await self.transport.start()
        if self.read_messages_task is None or self.read_messages_task.done():
            self.read_messages_task = asyncio.create_task(self.read_messages())

    async def __aenter__(self):
        await self.connect()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def close(self):
        for t in self.deferred_send_tasks:
            t.cancel()

        if self.read_messages_task is not None and not self.read_messages_task.done():
            self.read_messages_task.cancel()

        await self.transport.stop()

    async def send_model(self, model: BaseRequest, error_event_callback=None):
        while True:
            try:
                await self.transport.send(model.model_dump_json())
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

    def deferred_send_model(self, model: BaseRequest):
        task = asyncio.create_task(self.send_model(model))
        self.deferred_send_tasks.append(task)

    async def read_messages(self):
        async for msg in self.transport:
            try:
                msg = self.accepted_request_type().parse(msg)
            except ValidationError as ex:
                error_msg = f"Malformed message from miner {self.miner_name}: {str(ex)}"
                logger.info(error_msg)
                self.deferred_send_model(self.outgoing_generic_error_class()(details=error_msg))
                continue

            try:
                await self.handle_message(msg)
            except UnsupportedMessageReceived:
                error_msg = f"Unsupported message from miner {self.miner_name}"
                logger.exception(error_msg)
                self.deferred_send_model(self.outgoing_generic_error_class()(details=error_msg))


class UnsupportedMessageReceived(Exception):
    def __init__(self, msg: BaseRequest):
        self.msg = msg

    def __str__(self):
        return f"{type(self).__name__}: {self.msg.model_dump_json()}"

    __repr__ = __str__
