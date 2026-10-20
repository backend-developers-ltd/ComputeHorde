import asyncio
import logging
import random
from collections.abc import Callable

import websockets

from .base import AbstractTransport, TransportConnectionError

logger = logging.getLogger(__name__)


class WSTransport(AbstractTransport):
    """
    A WebSocket transport layer for exchanging messages between parties.

    An instance needs to be started before sending or receiving messages and stopped after use. That's where the actual connection is established and terminated. Not started instances will raise a runtime error when trying to send or receive messages.

    ```python
    transport = WSTransport("miner", "ws://example.com")
    await transport.start()

    await transport.send("Hello, world!")

    message = await transport.receive()

    await transport.stop()
    ```

    Continuously receiving messages can be done using an async for loop.

    ```python
    transport = WSTransport("miner", "ws://example.com")
    await transport.start()

    async for message in transport:
        print(message)

    await transport.stop()
    ```

    `send()` and `receive()` reopen a dropped connection, which the peer sees as a new one.
    A protocol that starts with a greeting, such as an authentication message, passes
    `reconnect_greeting`: it is called for every reopened connection and its result is sent
    before anything else. `start()` does not send it; the caller sends the first greeting.
    """

    def __init__(
        self,
        name: str,
        url: str,
        *,
        max_retries: int = 5,
        base_retry_delay: float = 1,
        retry_jitter: float = 1,
        reconnect_greeting: Callable[[], str] | None = None,
    ):
        super().__init__(name)
        self.url = url
        self.base_retry_delay = base_retry_delay
        self.retry_jitter = retry_jitter
        self.max_retries = max_retries
        self.reconnect_greeting = reconnect_greeting
        self.connect_lock = asyncio.Lock()
        self._ws: websockets.ClientConnection | None = None

    @property
    def ws(self) -> websockets.ClientConnection:
        if self._ws is None:
            raise RuntimeError("WebSocket connection has not been established")
        return self._ws

    def _get_retry_delay(self, attempt: int):
        return self.base_retry_delay * 2**attempt + random.uniform(0, self.retry_jitter)

    async def start(self) -> None:
        async with self.connect_lock:
            await self.connect()

    async def stop(self) -> None:
        async with self.connect_lock:
            if self._ws and self._ws.state is websockets.State.OPEN:
                await self._ws.close()

    async def connect(self, *, greeting: Callable[[], str] | None = None) -> None:
        """
        Opens the connection unless it is already open; `greeting` produces the message sent
        on it before any other. An `OSError` or websockets error while producing or sending the
        greeting counts as a failed attempt; any other error propagates. Either way the new
        connection is closed.
        """
        if self._ws and self._ws.state is websockets.State.OPEN:
            return

        loop = asyncio.get_running_loop()
        start_time = loop.time()
        attempt = 0

        while self.max_retries == 0 or attempt < self.max_retries:
            try:
                self._ws = await self._open_connection(greeting)
                logger.info(f"Connected to {self.name} after {attempt} attempts")
                return
            except (websockets.WebSocketException, OSError):
                attempt += 1
                delay = self._get_retry_delay(attempt)
                logger.info(f"Retrying connection to {self.name} in {delay:0.2f}")
                await asyncio.sleep(delay)

        time_took = loop.time() - start_time
        raise TransportConnectionError(
            f"Could not connect to {self.name} after {attempt} attempts in {time_took:0.2f} seconds"
        )

    async def _open_connection(
        self, greeting: Callable[[], str] | None
    ) -> websockets.ClientConnection:
        ws = await websockets.connect(
            self.url, max_size=50 * (2**20), ping_timeout=120
        )  # 50MB - ping timeout 2min in case of blocking job sends
        if greeting is not None:
            # The connection is not self._ws yet, so stop() would not close it.
            try:
                await ws.send(greeting())
            except BaseException:
                await ws.close()
                raise
        return ws

    async def _reconnect(self) -> None:
        # send() and receive() notice a dropped connection independently; the lock makes the
        # later one find the connection already reopened instead of opening a second one.
        async with self.connect_lock:
            await self.connect(greeting=self.reconnect_greeting)

    async def send(self, data: str | bytes) -> None:
        while True:
            try:
                await self.ws.send(data)
                await asyncio.sleep(0)
                # Summary: https://github.com/python-websockets/websockets/issues/867
                # Longer discussion: https://github.com/python-websockets/websockets/issues/865
                # logger.debug(f"Sent message to {self.name}: {data}")
                return
            except (websockets.WebSocketException, OSError):
                logger.info(f"Could not send msg to {self.name}. Reconnecting...")
                await self._reconnect()

    async def receive(self) -> str | bytes:
        while True:
            try:
                msg = await self.ws.recv()
                # logger.debug(f"Received message from {self.name}: {msg}")
                return msg
            except (websockets.WebSocketException, OSError):
                logger.info(f"Could not receive msg from {self.name}. Reconnecting...")
                await self._reconnect()
