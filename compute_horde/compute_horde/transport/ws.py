import asyncio
import logging
import random

import websockets

from .base import AbstractTransport, TransportConnectionError

logger = logging.getLogger(__name__)


class WSTransport(AbstractTransport):
    def __init__(
        self,
        name: str,
        url: str,
        *,
        max_retries: int = 5,
        base_retry_delay: int = 1,
        retry_jitter: float = 1,
    ):
        super().__init__(name)
        self.url = url
        self.base_retry_delay = base_retry_delay
        self.retry_jitter = retry_jitter
        self.max_retries = max_retries
        self.connect_lock = asyncio.Lock()
        self._ws = None

    @property
    def ws(self) -> websockets.WebSocketClientProtocol:
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
            if self._ws and self._ws.open:
                await self._ws.close()

    async def connect(self):
        loop = asyncio.get_running_loop()
        start_time = loop.time()
        attempt = 0

        while self.max_retries == 0 or attempt < self.max_retries:
            try:
                self._ws = await websockets.connect(self.url, max_size=50 * (2**20))  # 50MB
                logger.info(f"Connected to {self.name} after {attempt} attempts")
                return
            except (websockets.WebSocketException, OSError):
                attempt += 1
                delay = self._get_retry_delay(attempt)
                logger.info(f"Retrying connection to {self.name} in {delay:0.2f}")
                await asyncio.sleep(delay)

        time_took = loop.time() - start_time
        raise TransportConnectionError(
            f"Could not connect to {self.name} after {attempt} attempts"
            f" in {time_took:0.2f} seconds"
        )

    async def send(self, data: str | bytes) -> None:
        while True:
            try:
                await self.ws.send(data)
                await asyncio.sleep(0)
                # Summary: https://github.com/python-websockets/websockets/issues/867
                # Longer discussion: https://github.com/python-websockets/websockets/issues/865
                logger.debug(f"Sent message to {self.name}: {data}")
                return
            except (websockets.WebSocketException, OSError):
                logger.info(f"Could not send msg to {self.name}. Reconnecting...")
                await self.connect()

    async def receive(self) -> str | bytes:
        while True:
            try:
                msg = await self.ws.recv()
                logger.debug(f"Received message from {self.name}: {msg}")
                return msg
            except (websockets.WebSocketException, OSError):
                logger.info(f"Could not receive msg from {self.name}. Reconnecting...")
                await self.connect()
