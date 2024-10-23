from contextlib import asynccontextmanager

import bittensor
from channels.testing import WebsocketCommunicator

from compute_horde_miner import asgi


@asynccontextmanager
async def fake_validator(validator_wallet: bittensor.wallet) -> WebsocketCommunicator:
    communicator = WebsocketCommunicator(
        application=asgi.application,
        path=f"v0.1/validator_interface/{validator_wallet.hotkey.ss58_address}",
    )
    connected, _ = await communicator.connect()
    assert connected
    yield communicator
    await communicator.disconnect()
