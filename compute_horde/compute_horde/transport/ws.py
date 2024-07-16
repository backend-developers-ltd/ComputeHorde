import asyncio
import logging

import websockets

from .base import AbstractTransport, TransportConnectionError

logger = logging.getLogger(__name__)


class WSTransport(AbstractTransport):
    def __init__(self, url: str):
        self.url = url
        self.ws = None

    async def connect(self):
        try:
            self.ws = await websockets.connect(self.url, max_size=50 * (2**20))  # 50MB
        except (websockets.WebSocketException, OSError) as exc:
            raise TransportConnectionError(f"Failed to connect to {self.url}") from exc

    async def close(self):
        if self.ws is not None and not self.ws.closed:
            await self.ws.close()

    def is_connected(self) -> bool:
        return self.ws is not None and self.ws.open

    async def send(self, data: str | bytes) -> None:
        assert self.ws is not None

        try:
            await self.ws.send(data)
            await asyncio.sleep(0)
            # Summary: https://github.com/python-websockets/websockets/issues/867
            # Longer discussion: https://github.com/python-websockets/websockets/issues/865
        except (websockets.WebSocketException, OSError) as exc:
            raise TransportConnectionError(f"Failed to send data to {self.url}") from exc

        logger.debug(f"Sent message to {self.url}: {data}")

    async def receive(self) -> str | bytes:
        assert self.ws is not None

        try:
            msg = await self.ws.recv()
            logger.debug(f"Received message from {self.url}: {msg}")
            return msg
        except (websockets.WebSocketException, OSError) as exc:
            raise TransportConnectionError(f"Failed to receive data from {self.url}") from exc
