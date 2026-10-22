import asyncio
import contextlib
import itertools

import pytest
import pytest_asyncio
import websockets

from compute_horde.transport import WSTransport
from tests.ws_server import AbstractWSTestServer, ReconnectingWSTestServer, wait_until


class WSTestServer(AbstractWSTestServer):
    def __init__(self, port: int) -> None:
        super().__init__(port)
        self.received: asyncio.Queue[str] = asyncio.Queue()

    async def _serve(self, ws: websockets.ServerConnection) -> None:
        async for message in ws:
            await self.received.put(message)

    @property
    def connection(self) -> websockets.ServerConnection | None:
        assert len(self.connections) <= 1
        return next(iter(self.connections), None)

    @property
    def is_connected(self) -> bool:
        return self.connection is not None and self.connection.state is websockets.State.OPEN


@pytest_asyncio.fixture
async def server(unused_tcp_port):
    async with WSTestServer(port=unused_tcp_port) as _server:
        yield _server


@pytest.fixture
def ws_transport(server):
    return WSTransport("test", server.url, base_retry_delay=0.1, retry_jitter=0.1)


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


@pytest.fixture
def greeting():
    numbers = itertools.count(1)
    return lambda: f"greeting {next(numbers)}"


def greeting_transport(server: ReconnectingWSTestServer, greeting) -> WSTransport:
    return WSTransport(
        "test", server.url, base_retry_delay=0.1, retry_jitter=0.1, reconnect_greeting=greeting
    )


@pytest.mark.asyncio
async def test_receive_reconnect_sends_greeting_first(unused_tcp_port, greeting):
    async with ReconnectingWSTestServer(port=unused_tcp_port) as server:
        ws_transport = greeting_transport(server, greeting)
        await ws_transport.start()

        receive_task = asyncio.create_task(ws_transport.receive())
        assert await asyncio.wait_for(server.received.get(), 1) == (2, "greeting 1")
        await server.connection.send("foo")

        assert await receive_task == "foo"
        await ws_transport.stop()


@pytest.mark.asyncio
async def test_send_reconnect_sends_greeting_first(unused_tcp_port, greeting):
    async with ReconnectingWSTestServer(port=unused_tcp_port) as server:
        ws_transport = greeting_transport(server, greeting)
        await ws_transport.start()
        await wait_until(lambda: ws_transport.ws.state is not websockets.State.OPEN)

        await ws_transport.send("foo")

        assert await asyncio.wait_for(server.received.get(), 1) == (2, "greeting 1")
        assert await asyncio.wait_for(server.received.get(), 1) == (2, "foo")
        await ws_transport.stop()


@pytest.mark.asyncio
async def test_failing_greeting_closes_reopened_connection(unused_tcp_port):
    def greeting() -> str:
        raise ValueError("cannot sign the greeting")

    async with ReconnectingWSTestServer(port=unused_tcp_port) as server:
        ws_transport = greeting_transport(server, greeting)
        await ws_transport.start()

        with pytest.raises(ValueError, match="cannot sign the greeting"):
            await ws_transport.receive()
        await ws_transport.stop()

        assert server.connection_count == 2
        await wait_until(lambda: not server.connections)


@pytest.mark.asyncio
async def test_concurrent_reconnects_open_one_connection(unused_tcp_port, greeting):
    async with ReconnectingWSTestServer(port=unused_tcp_port, handshake_delay=0.3) as server:
        ws_transport = greeting_transport(server, greeting)
        await ws_transport.start()
        await wait_until(lambda: ws_transport.ws.state is not websockets.State.OPEN)

        receive_task = asyncio.create_task(ws_transport.receive())
        await wait_until(lambda: server.delayed_handshakes == 1)
        await ws_transport.send("foo")

        receive_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await receive_task
        await ws_transport.stop()

        assert server.connection_count == 2
        assert await asyncio.wait_for(server.received.get(), 1) == (2, "greeting 1")
        assert await asyncio.wait_for(server.received.get(), 1) == (2, "foo")
        await wait_until(lambda: not server.connections)
