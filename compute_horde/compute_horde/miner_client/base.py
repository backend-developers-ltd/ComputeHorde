import abc
import asyncio
import logging

import websockets

from compute_horde.base_requests import BaseRequest, ValidationError
from compute_horde.em_protocol.executor_requests import BaseExecutorRequest


logger = logging.getLogger(__name__)


class AbstractMinerClient(abc.ABC):

    def __init__(self, loop: asyncio.AbstractEventLoop):
        self.loop = loop
        self.ws: websockets.WebSocketClientProtocol | None = None
        self.read_messages_task: asyncio.Task | None = None

    @abc.abstractmethod
    def miner_url(self) -> str:
        ...

    @abc.abstractmethod
    def accepted_request_type(self) -> type[BaseRequest]:
        pass

    @abc.abstractmethod
    def incoming_generic_error_class(self):
        pass

    @abc.abstractmethod
    def outgoing_generic_error_class(self):
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
        if self.read_messages_task is not None and not self.read_messages_task.done():
            self.read_messages_task.cancel()

        if self.ws is not None and not self.ws.closed:
            await self.ws.close()

    async def _connect(self):
        return await websockets.connect(self.miner_url())

    async def await_connect(self):
        while True:
            try:
                self.ws = await self._connect()
                self.read_messages_task = self.loop.create_task(self.read_messages())
                return
            except websockets.WebSocketException as ex:
                logger.error(f'Could not connect to miner: {str(ex)}')
            await asyncio.sleep(1)

    async def ensure_connected(self):
        if self.ws is None or self.ws.closed:
            if self.read_messages_task is not None and not self.read_messages_task.done():
                self.read_messages_task.cancel()
            await self.await_connect()

    async def send_model(self, model: BaseExecutorRequest):
        await self.ensure_connected()
        await self.ws.send(model.json())

    async def read_messages(self):
        while True:
            try:
                msg = await self.ws.recv()
            except websockets.WebSocketException as ex:
                logger.error(f'Connection to miner lost: {str(ex)}')
                self.loop.create_task(self.await_connect())
                return

            try:
                msg = self.accepted_request_type().parse(msg)
            except ValidationError as ex:
                logger.error(f'Malformed message from miner: {str(ex)}')
                await self.ws.send(self.outgoing_generic_error_class()(details=f'Malformed message: {str(ex)}').json())
                continue

            if isinstance(msg, self.incoming_generic_error_class()):
                try:
                    raise RuntimeError(f'Received error message: {msg.json()}')
                except Exception:
                    logger.exception('')
                continue
            try:
                await self.handle_message(msg)
            except UnsupportedMessageReceived:
                logger.exception('')


class UnsupportedMessageReceived(Exception):
    def __init__(self, msg: BaseRequest):
        self.msg = msg

    def __str__(self):
        return f'{type(self).__name__}: {self.msg.json()}'

    __repr__ = __str__
