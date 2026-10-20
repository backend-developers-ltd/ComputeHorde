import asyncio
from abc import ABC, abstractmethod
from collections.abc import Callable

import websockets


async def wait_until(condition: Callable[[], bool], timeout: float = 2) -> None:
    async with asyncio.timeout(timeout):
        while not condition():
            await asyncio.sleep(0.01)


class AbstractWSTestServer(ABC):
    """A WebSocket server on localhost that `async with` starts and stops."""

    host = "localhost"

    def __init__(self, port: int) -> None:
        self.port = port
        self.ws_server: websockets.Server | None = None

    async def start(self) -> None:
        self.ws_server = await websockets.serve(
            self._serve, self.host, self.port, process_request=self._process_request
        )

    async def stop(self) -> None:
        self.ws_server.close()
        await self.ws_server.wait_closed()

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.stop()

    @property
    def url(self) -> str:
        return f"ws://{self.host}:{self.port}"

    @property
    def connections(self) -> set[websockets.ServerConnection]:
        return self.ws_server.connections

    @abstractmethod
    async def _serve(self, ws: websockets.ServerConnection) -> None: ...

    async def _process_request(
        self, connection: websockets.ServerConnection, request: websockets.Request
    ) -> None:
        """Called during every opening handshake, before the connection is accepted."""


class ReconnectingWSTestServer(AbstractWSTestServer):
    """
    A WebSocket peer that closes the first connection after `drop_first_connection_after`
    messages, so the client's next send() or receive() has to reconnect. Every later handshake
    takes `handshake_delay` seconds, which keeps a reconnect in flight long enough for a
    concurrent one to start. Received messages are queued as (connection number, message).
    """

    def __init__(
        self, port: int, *, drop_first_connection_after: int = 0, handshake_delay: float = 0
    ) -> None:
        super().__init__(port)
        self.drop_first_connection_after = drop_first_connection_after
        self.handshake_delay = handshake_delay
        self.connection_count = 0
        self.delayed_handshakes = 0
        self.received: asyncio.Queue[tuple[int, str | bytes]] = asyncio.Queue()

    @property
    def connection(self) -> websockets.ServerConnection:
        (connection,) = self.connections
        return connection

    async def _process_request(
        self, connection: websockets.ServerConnection, request: websockets.Request
    ) -> None:
        if self.connection_count:
            self.delayed_handshakes += 1
            await asyncio.sleep(self.handshake_delay)
            self.delayed_handshakes -= 1

    async def _serve(self, ws: websockets.ServerConnection) -> None:
        self.connection_count += 1
        number = self.connection_count
        limit = self.drop_first_connection_after if number == 1 else None
        received = 0
        while limit is None or received < limit:
            try:
                message = await ws.recv()
            except websockets.ConnectionClosed:
                return
            await self.received.put((number, message))
            received += 1
        # Returning closes the connection.
