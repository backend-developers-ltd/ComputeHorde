import asyncio
import uuid
from unittest.mock import patch

import bittensor
import pytest
import websockets
from channels.layers import get_channel_layer

from compute_horde_validator.validator.models import OrganicJob
from compute_horde_validator.validator.organic_jobs.facilitator_api import MachineSpecsUpdate
from compute_horde_validator.validator.organic_jobs.facilitator_client import (
    AuthenticationRequest,
    FacilitatorClient,
    Response,
)
from compute_horde_validator.validator.organic_jobs.miner_driver import JobStatusUpdate
from compute_horde_validator.validator.utils import MACHINE_SPEC_CHANNEL

from .helpers import (
    MockJobStateMinerClient,
    MockMetagraph,
    MockSubtensor,
    get_dummy_job_request_v0,
    get_dummy_job_request_v1,
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


class FacilitatorJobStatusUpdatesWsV0(FacilitatorWs):
    def get_dummy_job(self, job_uuid):
        return get_dummy_job_request_v0(job_uuid)

    async def serve(self, ws):
        try:
            job_uuid = str(uuid.uuid4())

            # auth
            response = await asyncio.wait_for(ws.recv(), timeout=5)
            try:
                AuthenticationRequest.model_validate_json(response)
            except Exception as e:
                self.facilitator_error = e

            await asyncio.wait_for(ws.send(Response(status="success").model_dump_json()), timeout=5)

            # send job request
            await asyncio.wait_for(
                ws.send(self.get_dummy_job(job_uuid).model_dump_json()), timeout=5
            )

            # get job status update
            response = await asyncio.wait_for(ws.recv(), timeout=5)
            try:
                JobStatusUpdate.model_validate_json(response)
            except Exception as e:
                self.facilitator_error = e

            response = await asyncio.wait_for(ws.recv(), timeout=5)
            try:
                JobStatusUpdate.model_validate_json(response)
            except Exception as e:
                self.facilitator_error = e

            organic_job = await asyncio.wait_for(
                OrganicJob.objects.aget(job_uuid=job_uuid), timeout=5
            )
            if organic_job.status != OrganicJob.Status.COMPLETED:
                self.facilitator_error = Exception(f"job not completed: {organic_job.status}")
        except Exception as e:
            self.facilitator_error = e
        finally:
            async with self.condition:
                self.condition.notify()


class FacilitatorJobStatusUpdatesWsV1(FacilitatorJobStatusUpdatesWsV0):
    def get_dummy_job(self, job_uuid):
        return get_dummy_job_request_v1(job_uuid)


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
        FacilitatorJobStatusUpdatesWsV0,
        FacilitatorJobStatusUpdatesWsV1,
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
            pytest.fail(str(ws_server.facilitator_error))


@pytest.fixture
def specs_msg():
    return {
        "type": "machine.specs",
        "batch_id": str(uuid.uuid4()),
        "miner_hotkey": "miner_hotkey",
        "specs": {
            "cpu": {"cores": 1, "threads": 2, "freq": 3},
            "gpu": {"name": "gpu_name", "memory": 4, "compute": 5},
        },
    }


class FacilitatorExpectMachineSpecsWs(FacilitatorWs):
    async def serve(self, ws, path):
        response = await asyncio.wait_for(ws.recv(), timeout=5)
        try:
            AuthenticationRequest.model_validate_json(response)
        except Exception as e:
            self.facilitator_error = e

        await asyncio.wait_for(ws.send(Response(status="success").model_dump_json()), timeout=5)

        async for message in ws:
            try:
                MachineSpecsUpdate.model_validate_json(message)
            except Exception:
                continue
            else:
                async with self.condition:
                    self.condition.notify()


# TODO: this test is flaky, needs proper investigation
@pytest.mark.skip
@pytest.mark.asyncio
@patch("bittensor.subtensor", lambda *args, **kwargs: MockSubtensor())
@patch("bittensor.metagraph", lambda *args, **kwargs: MockMetagraph())
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
async def test_wait_for_specs(specs_msg: dict):
    layer = get_channel_layer()
    await layer.send(MACHINE_SPEC_CHANNEL, specs_msg)

    ws_server = FacilitatorExpectMachineSpecsWs()

    async with websockets.serve(ws_server.serve, "127.0.0.1", 0) as server:
        host, port = server.sockets[0].getsockname()
        facilitator_uri = f"ws://{host}:{port}/"
        facilitator_client = MockFacilitatorClient(get_keypair(), facilitator_uri)

        facilitator_client.MINER_CLIENT_CLASS = MockJobStateMinerClient

        async with ws_server.condition:
            task = asyncio.create_task(facilitator_client.run_forever())
            await asyncio.wait_for(ws_server.condition.wait(), timeout=5)

        facilitator_client.miner_driver_awaiter_task.cancel()
        facilitator_client.heartbeat_task.cancel()
        facilitator_client.specs_task.cancel()
        task.cancel()
