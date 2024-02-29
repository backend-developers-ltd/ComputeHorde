import abc
import asyncio
import logging
import random

import websockets

from compute_horde.base_requests import BaseRequest, ValidationError

logger = logging.getLogger(__name__)


class AbstractMinerClient(abc.ABC):

    def __init__(self, loop: asyncio.AbstractEventLoop, miner_name: str):
        self.debounce_counter = 0
        self.loop = loop
        self.miner_name = miner_name
        self.ws: websockets.WebSocketClientProtocol | None = None
        self.read_messages_task: asyncio.Task | None = None
        self.deferred_send_tasks: list[asyncio.Task] = []

    @abc.abstractmethod
    def miner_url(self) -> str:
        ...

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

    async def __aenter__(self):
        await self.await_connect()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        for t in self.deferred_send_tasks:
            t.cancel()

        if self.read_messages_task is not None and not self.read_messages_task.done():
            self.read_messages_task.cancel()

        if self.ws is not None and not self.ws.closed:
            await self.ws.close()

    async def _connect(self):
        return await websockets.connect(self.miner_url())

    async def await_connect(self):
        while True:
            try:
                if self.debounce_counter:
                    sleep_time = self.sleep_time()
                    logger.info(f'Retrying connection to miner {self.miner_name} in {sleep_time:0.2f}')
                    await asyncio.sleep(sleep_time)
                self.debounce_counter += 1
                self.ws = await self._connect()
                self.read_messages_task = self.loop.create_task(self.read_messages())
                return
            except (websockets.WebSocketException, OSError) as ex:
                logger.info(f'Could not connect to miner {self.miner_name}: {str(ex)}')

    def sleep_time(self):
        return (2 ** self.debounce_counter) + random.random()

    async def ensure_connected(self):
        if self.ws is None or self.ws.closed:
            if self.read_messages_task is not None and not self.read_messages_task.done():
                self.read_messages_task.cancel()
            await self.await_connect()

    async def send_model(self, model: BaseRequest):
        while True:
            await self.ensure_connected()
            try:
                await self.ws.send(model.json())
            except websockets.WebSocketException as ex:
                logger.error(f'Could not send to miner {self.miner_name}: {str(ex)}')
                await asyncio.sleep(1 + random.random())
                continue
            return

    def deferred_send_model(self, model: BaseRequest):
        task = self.loop.create_task(self.send_model(model))
        self.deferred_send_tasks.append(task)

    async def read_messages(self):
        while True:
            try:
                msg = await self.ws.recv()
            except websockets.WebSocketException as ex:
                logger.info(f'Connection to miner {self.miner_name} lost: {str(ex)}')
                self.loop.create_task(self.await_connect())
                return

            try:
                msg = self.accepted_request_type().parse(msg)
            except ValidationError as ex:
                error_msg = f'Malformed message from miner {self.miner_name}: {str(ex)}'
                logger.info(error_msg)
                self.deferred_send_model(self.outgoing_generic_error_class()(details=error_msg))
                continue

            if isinstance(msg, self.incoming_generic_error_class()):
                try:
                    raise RuntimeError(f'Received error message from miner {self.miner_name}: {msg.json()}')
                except Exception:
                    logger.exception('')
                continue
            try:
                await self.handle_message(msg)
            except UnsupportedMessageReceived:
                error_msg = f'Unsupported message from miner {self.miner_name}'
                logger.exception(error_msg)
                self.deferred_send_model(self.outgoing_generic_error_class()(details=error_msg))


class UnsupportedMessageReceived(Exception):
    def __init__(self, msg: BaseRequest):
        self.msg = msg

    def __str__(self):
        return f'{type(self).__name__}: {self.msg.json()}'

    __repr__ = __str__
