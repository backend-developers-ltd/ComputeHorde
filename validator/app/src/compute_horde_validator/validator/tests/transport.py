import asyncio
from collections import deque

from compute_horde.miner_client.base import AbstractTransport


class MinerSimulationTransport(AbstractTransport):
    def __init__(self, name: str, *args, **kwargs):
        self.sent: list[str] = []
        self.remaining = 0
        self.ready_to_send = asyncio.Event()
        self.ready_to_send.set()
        self.received_messages = []
        self.to_send: deque[tuple[int, str]] = deque()

    async def start(self): ...

    async def stop(self): ...

    async def send(self, message):
        self.received_messages.append(message)

        self.remaining = max(0, self.remaining - 1)
        if self.remaining == 0:
            self.ready_to_send.set()

    async def receive(self):
        if not self.to_send:
            await asyncio.Future()

        await self.ready_to_send.wait()

        self.remaining, message = self.to_send.popleft()
        self.ready_to_send.clear()
        return message

    async def add_message(self, message, receive_before=0):
        if not self.to_send:
            self.remaining = receive_before
            self.ready_to_send.clear()

        self.to_send.append((receive_before, message))
