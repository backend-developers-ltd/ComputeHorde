import asyncio

from .base import AbstractTransport


class StubTransport(AbstractTransport):
    """
    Stub transport for testing purposes.
    Receives predefined list of messages and saves sent messages in the internal state
    """

    def __init__(self, name: str, messages: list[str], *args, **kwargs):
        self.sent: list[str] = []
        self.messages = iter(messages)
        self.sent_messages: list[str] = []

    async def start(self): ...

    async def stop(self): ...

    async def send(self, message):
        self.sent_messages.append(message)

    async def receive(self):
        try:
            return next(self.messages)
        except StopIteration:
            await asyncio.Future()
