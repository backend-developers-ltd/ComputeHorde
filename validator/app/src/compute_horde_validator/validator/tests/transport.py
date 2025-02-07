import asyncio
import logging
from collections import deque

from compute_horde.miner_client.base import AbstractTransport


class SimulationTransport(AbstractTransport):
    """
    A simulation transport layer mimicking the behavior of a WSTransport.
    Feed the messages to be received in a sequence and the transport
    will receive them in the same order. Each message can be set to be received after a
    specified number of sent messages replicating the real communication flow.
    """

    def __init__(self, name: str, *args, **kwargs):
        super().__init__(name, *args, **kwargs)
        self.received: list[str] = []
        self.sent: list[str] = []
        self.receive_at_counter: int = 0
        self.to_receive: deque[tuple[int, float, str]] = deque()
        self.receive_condition = asyncio.Condition()
        self.logger = logging.getLogger(f"transport.{name}")

    async def start(self): ...

    async def stop(self): ...

    async def send(self, message: str) -> None:
        async with self.receive_condition:
            self.sent.append(message)
            self.receive_condition.notify_all()

        self.logger.debug(f"Sent message: {message}")

    async def receive(self) -> str:
        try:
            receive_at, sleep_before, message = self.to_receive.popleft()
        except IndexError:
            self.logger.debug("No more messages to receive")
            await asyncio.Future()

        if len(self.sent) < receive_at:
            async with self.receive_condition:
                await self.receive_condition.wait_for(lambda: len(self.sent) >= receive_at)

        await asyncio.sleep(sleep_before)

        self.logger.debug(f"Received message: {message}")
        self.received.append(message)

        return message

    async def add_message(
        self, message: str, send_before: int = 0, sleep_before: float = 0
    ) -> None:
        """
        Add a message to be received after a certain number of sent messages.
        Receives the message immediately if send_before is 0.
        Optionally sleep before receiving the message.
        """

        self.receive_at_counter += send_before
        self.to_receive.append((self.receive_at_counter, sleep_before, message))
