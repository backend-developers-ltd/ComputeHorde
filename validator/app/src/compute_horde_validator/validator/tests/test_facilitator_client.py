"""
This test file is here always to indicate that everything was installed and the CI was able to run tests.
It should always pass as long as all dependencies are properly installed.
"""

import asyncio
import uuid

import bittensor
import pytest
import websockets
from django.conf import settings

from compute_horde_validator.validator.facilitator_client import FacilitatorClient, Response
from compute_horde_validator.validator.miner_driver import JobStatusUpdate

from .test_miner_driver import MockMinerClient, get_dummy_job_request

WEBSOCKET_TIMEOUT = 10

condition = asyncio.Condition()
facilitator_error = None


class MockFacilitatorClient(FacilitatorClient):
    async def get_miner_axon_info(self, hotkey: str) -> bittensor.AxonInfo:
        return bittensor.AxonInfo(
            version=4,
            ip="ignore",
            ip_type=4,
            port=9999,
            hotkey=hotkey,
            coldkey="ignore",
        )


async def mock_facilitator_ws(ws):
    global facilitator_error
    job_uuid = str(uuid.uuid4())

    # auth
    response = await ws.recv()
    await ws.send(Response(status="success").json())

    # send job request
    await ws.send(get_dummy_job_request(job_uuid).json())

    # get job status update
    response = await ws.recv()
    try:
        JobStatusUpdate.parse_raw(response)
    except Exception as e:
        facilitator_error = e

    response = await ws.recv()
    try:
        JobStatusUpdate.parse_raw(response)
    except Exception as e:
        facilitator_error = e

    async with condition:
        condition.notify()


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_facilitator_client():
    async with websockets.serve(mock_facilitator_ws, "127.0.0.1", 0) as server:
        host, port = server.sockets[0].getsockname()
        keypair = settings.BITTENSOR_WALLET().get_hotkey()
        facilitator_uri = f"ws://{host}:{port}/"
        facilitator_client = MockFacilitatorClient(keypair, facilitator_uri)

        facilitator_client.MINER_CLIENT_CLASS = MockMinerClient
        facilitator_client.MINER_CLIENT_CLASS.mock_job_state = True

        async with condition:
            task = asyncio.create_task(facilitator_client.run_forever())
            await condition.wait()

        facilitator_client.miner_driver_awaiter_task.cancel()
        facilitator_client.heartbeat_task.cancel()
        facilitator_client.specs_task.cancel()
        task.cancel()
        if facilitator_error:
            assert False, facilitator_error
