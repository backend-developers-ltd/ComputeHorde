from contextlib import asynccontextmanager

from channels.testing import WebsocketCommunicator
from compute_horde.test_wallet import get_test_validator_wallet

from compute_horde_miner import asgi


@asynccontextmanager
async def fake_validator() -> WebsocketCommunicator:
    validator_wallet = get_test_validator_wallet()
    communicator = WebsocketCommunicator(
        application=asgi.application,
        path=f"v0.1/validator_interface/{validator_wallet.hotkey.ss58_address}",
    )
    connected, _ = await communicator.connect()
    assert connected
    yield communicator
    await communicator.disconnect()
