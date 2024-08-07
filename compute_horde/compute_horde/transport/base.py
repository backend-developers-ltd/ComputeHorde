import abc


class TransportConnectionError(Exception):
    pass


class AbstractTransport(abc.ABC):
    """
    A base communication layer for sending and receiving messages.
    """

    def __init__(self, name: str, *args, **kwargs):
        self.name = name

    @abc.abstractmethod
    async def send(self, data: str | bytes) -> None:
        pass

    @abc.abstractmethod
    async def receive(self) -> str | bytes:
        pass

    @abc.abstractmethod
    async def start(self) -> None:
        pass

    @abc.abstractmethod
    async def stop(self) -> None:
        pass

    def __aiter__(self):
        return self

    async def __anext__(self):
        message = await self.receive()
        return message
