import abc


class TransportConnectionError(Exception):
    pass


class AbstractTransport(abc.ABC):
    @abc.abstractmethod
    async def connect(self):
        pass

    @abc.abstractmethod
    async def send(self, data: str | bytes):
        pass

    @abc.abstractmethod
    async def receive(self) -> str | bytes:
        pass

    @abc.abstractmethod
    async def close(self):
        pass
