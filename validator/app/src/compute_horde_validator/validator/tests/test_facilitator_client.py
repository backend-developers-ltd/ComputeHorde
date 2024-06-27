import asyncio
import uuid
from unittest.mock import patch

import bittensor
import pytest
import websockets

from compute_horde_validator.validator.facilitator_client import (
    AuthenticationRequest,
    FacilitatorClient,
    Response,
)
from compute_horde_validator.validator.miner_driver import JobStatusUpdate
from compute_horde_validator.validator.models import OrganicJob

from .helpers import (
    MockJobStateMinerClient,
    MockMetagraph,
    MockSubtensor,
    get_dummy_job_request,
    get_keypair,
)


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


class FacilitatorWs:
    def __init__(self):
        self.condition = asyncio.Condition()
        self.facilitator_error = None

    async def wait(self):
        async with self.condition:
            await self.condition.wait()


class FacilitatorJobStatusUpdatesWs(FacilitatorWs):
    async def serve(self, ws):
        job_uuid = str(uuid.uuid4())

        # auth
        response = await ws.recv()
        try:
            AuthenticationRequest.model_validate_json(response)
        except Exception as e:
            self.facilitator_error = e

        await ws.send(Response(status="success").model_dump_json())

        # send job request
        await ws.send(get_dummy_job_request(job_uuid).model_dump_json())

        # get job status update
        response = await ws.recv()
        try:
            JobStatusUpdate.model_validate_json(response)
        except Exception as e:
            self.facilitator_error = e

        response = await ws.recv()
        try:
            JobStatusUpdate.model_validate_json(response)
        except Exception as e:
            self.facilitator_error = e

        organic_job = await OrganicJob.objects.aget(job_uuid=job_uuid)
        if organic_job.status != OrganicJob.Status.COMPLETED:
            self.facilitator_error = Exception(f"job not completed: {organic_job.status}")

        async with self.condition:
            self.condition.notify()


class FacilitatorBadMessageWs(FacilitatorWs):
    async def serve(self, ws):
        job_uuid = str(uuid.uuid4())

        # auth
        await ws.recv()
        await ws.send(Response(status="success").model_dump_json())

        # send bad job request
        await ws.send('{"job_request": "invalid"}')

        num_jobs = await OrganicJob.objects.filter(job_uuid=job_uuid).acount()
        if num_jobs != 0:
            self.facilitator_error = Exception("should not have created job")

        async with self.condition:
            self.condition.notify()


@pytest.mark.asyncio
@patch("bittensor.subtensor", lambda *args, **kwargs: MockSubtensor())
@patch("bittensor.metagraph", lambda *args, **kwargs: MockMetagraph())
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
@pytest.mark.parametrize(
    "ws_server_cls",
    [
        FacilitatorJobStatusUpdatesWs,
        FacilitatorBadMessageWs,
    ],
)
async def test_facilitator_client(ws_server_cls):
    ws_server = ws_server_cls()
    async with websockets.serve(ws_server.serve, "127.0.0.1", 0) as server:
        host, port = server.sockets[0].getsockname()
        facilitator_uri = f"ws://{host}:{port}/"
        facilitator_client = MockFacilitatorClient(get_keypair(), facilitator_uri)

        facilitator_client.MINER_CLIENT_CLASS = MockJobStateMinerClient

        async with ws_server.condition:
            task = asyncio.create_task(facilitator_client.run_forever())
            await ws_server.condition.wait()

        facilitator_client.miner_driver_awaiter_task.cancel()
        facilitator_client.heartbeat_task.cancel()
        facilitator_client.specs_task.cancel()
        task.cancel()
        if ws_server.facilitator_error:
            assert False, ws_server.facilitator_error
