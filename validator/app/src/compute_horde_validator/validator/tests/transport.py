import asyncio
import logging
from collections import deque

from compute_horde.miner_client.base import AbstractTransport


class MinerSimulationTransport(AbstractTransport):
    def __init__(self, name: str, *args, **kwargs):
        super().__init__(name, *args, **kwargs)
        self.received: list[str] = []
        self.sent = []
        self.remaining = 0
        self.to_receive: deque[tuple[int, str]] = deque()
        self.ready_to_receive = asyncio.Event()
        self.ready_to_receive.set()
        self.logger = logging.getLogger(f"transport.{name}")

    async def start(self): ...

    async def stop(self): ...

    async def send(self, message):
        self.logger.debug(f"Sending message: {message}")

        self.sent.append(message)

        self.remaining = max(0, self.remaining - 1)
        if self.remaining == 0:
            self.ready_to_receive.set()

    async def receive(self):
        if not self.to_receive:
            self.logger.debug("No more messages to receive")
            await asyncio.Future()

        await self.ready_to_receive.wait()

        self.remaining, message = self.to_receive.popleft()
        self.ready_to_receive.clear()

        self.received.append(message)

        self.logger.debug(f"Received message: {message}")

        return message

    async def add_message(self, message, receive_before=0):
        if not self.to_receive:
            self.remaining = receive_before
            self.ready_to_receive.clear()

        self.to_receive.append((receive_before, message))
