import asyncio

import pytest
import pytest_asyncio
import websockets

from compute_horde.transport import WSTransport


class WSTestServer:
    host = "localhost"
    port = 8765

    def __init__(self):
        self.ws_server = None
        self.received: asyncio.Queue[str] = asyncio.Queue()

    async def srv(self, ws, path):
        async for message in ws:
            await self.received.put(message)

    async def start(self):
        self.ws_server = await websockets.serve(self.srv, self.host, self.port)

    async def stop(self):
        self.ws_server.close()
        await self.ws_server.wait_closed()

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.stop()

    @property
    def connection(self) -> websockets.WebSocketServerProtocol | None:
        assert len(self.ws_server.websockets) <= 1

        try:
            return next(iter(self.ws_server.websockets))
        except StopIteration:
            return None

    @property
    def is_connected(self) -> bool:
        return self.connection is not None and self.connection.open


@pytest_asyncio.fixture
async def server():
    async with WSTestServer() as _server:
        yield _server


@pytest.fixture
def ws_transport():
    return WSTransport(
        "test",
        f"ws://{WSTestServer.host}:{WSTestServer.port}",
        base_retry_delay=0.1,
        retry_jitter=0.1,
    )


@pytest.mark.asyncio
async def test_connects(server: WSTestServer, ws_transport: WSTransport):
    assert not server.is_connected

    await ws_transport.start()

    assert server.is_connected


@pytest.mark.asyncio
async def test_send(server: WSTestServer, ws_transport: WSTransport):
    await ws_transport.start()

    await ws_transport.send("foo")
    await ws_transport.send("bar")

    assert await asyncio.wait_for(server.received.get(), 0.2) == "foo"
    assert await asyncio.wait_for(server.received.get(), 0.2) == "bar"


@pytest.mark.asyncio
async def test_send_reconnect(server: WSTestServer, ws_transport: WSTransport):
    await ws_transport.start()
    await server.stop()

    send_task = asyncio.create_task(ws_transport.send("foo"))
    await asyncio.sleep(0.1)
    await server.start()
    await send_task

    assert await asyncio.wait_for(server.received.get(), 0.2) == "foo"


@pytest.mark.asyncio
async def test_receive(server: WSTestServer, ws_transport: WSTransport):
    await ws_transport.start()

    await server.connection.send("foo")
    assert await ws_transport.receive() == "foo"

    await server.connection.send("bar")
    assert await ws_transport.receive() == "bar"


@pytest.mark.asyncio
async def test_receive_reconnect(server: WSTestServer, ws_transport: WSTransport):
    await ws_transport.start()

    await server.stop()

    receive_task = asyncio.create_task(ws_transport.receive())
    await server.start()
    await asyncio.sleep(0.5)
    await server.connection.send("foo")

    assert await receive_task == "foo"
