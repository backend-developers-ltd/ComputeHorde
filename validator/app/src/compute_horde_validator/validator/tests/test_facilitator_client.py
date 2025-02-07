import asyncio

# from enum import pickle_by_enum_name
import uuid
from contextlib import asynccontextmanager
from datetime import timedelta
from unittest.mock import patch

import pytest
import websockets
from channels.layers import get_channel_layer
from compute_horde.executor_class import DEFAULT_EXECUTOR_CLASS
from compute_horde.fv_protocol.facilitator_requests import JobRequest, Response
from compute_horde.fv_protocol.validator_requests import (
    V0AuthenticationRequest,
    V0MachineSpecsUpdate,
)
from django.utils import timezone

from compute_horde_validator.validator.models import (
    Cycle,
    Miner,
    MinerManifest,
    OrganicJob,
    SyntheticJobBatch,
)
from compute_horde_validator.validator.organic_jobs.facilitator_client import FacilitatorClient
from compute_horde_validator.validator.organic_jobs.miner_driver import JobStatusUpdate
from compute_horde_validator.validator.utils import MACHINE_SPEC_CHANNEL

from .helpers import (
    MockFaillingMinerClient,
    MockMetagraph,
    MockSubtensor,
    MockSuccessfulMinerClient,
    get_dummy_job_request_v0,
    get_dummy_job_request_v1,
    get_dummy_job_request_v2,
    get_keypair,
    mock_get_miner_axon_info,
)

DYNAMIC_ORGANIC_JOB_MAX_RETRIES_OVERRIDE = 3


@asynccontextmanager
async def async_patch_all():
    with (
        patch(
            "compute_horde_validator.validator.organic_jobs.facilitator_client.verify_job_request",
            return_value=True,
        ),
        patch(
            "compute_horde_validator.validator.organic_jobs.facilitator_client.get_miner_axon_info",
            mock_get_miner_axon_info,
        ),
        patch("bittensor.subtensor", lambda *args, **kwargs: MockSubtensor()),
        patch("bittensor.metagraph", lambda *args, **kwargs: MockMetagraph()),
        patch(
            "compute_horde_validator.validator.organic_jobs.facilitator_client.create_metagraph_refresh_task",
            lambda *args, **kwargs: asyncio.create_task(asyncio.sleep(0)),
        ),
    ):
        yield


async def setup_db(n: int = 1):
    now = timezone.now()
    batch = await SyntheticJobBatch.objects.acreate(
        block=1,
        cycle=await Cycle.objects.acreate(start=-14, stop=708),
        created_at=now,
    )
    miners = [await Miner.objects.acreate(hotkey=f"miner_{i}") for i in range(0, n)]
    for i, miner in enumerate(miners):
        await MinerManifest.objects.acreate(
            miner=miner,
            batch=batch,
            created_at=now - timedelta(minutes=i * 2),
            executor_class=DEFAULT_EXECUTOR_CLASS,
            online_executor_count=5,
        )


async def cancel_facilitator_tasks(
    facilitator_client, run_forever_task: asyncio.Task | None = None
):
    tasks = [
        facilitator_client.miner_driver_awaiter_task,
        facilitator_client.refresh_metagraph_task,
        facilitator_client.heartbeat_task,
    ]
    if run_forever_task:
        tasks.append(run_forever_task)
    if facilitator_client.specs_task:
        tasks.append(facilitator_client.specs_task)

    for task in tasks:
        task.cancel()

    await asyncio.gather(
        *tasks,
        return_exceptions=True,
    )


class FacilitatorWs:
    def __init__(self):
        self.condition = asyncio.Condition()
        self.facilitator_error = None

    async def wait(self):
        async with self.condition:
            await self.condition.wait()

    def get_dummy_job(self, job_uuid) -> JobRequest:
        return get_dummy_job_request_v0(job_uuid)

    async def verify_auth(self, ws):
        response = await asyncio.wait_for(ws.recv(), timeout=5)
        V0AuthenticationRequest.model_validate_json(response)
        await asyncio.wait_for(ws.send(Response(status="success").model_dump_json()), timeout=5)

    async def verify_job_status_update(self, ws):
        # accept or decline
        response = await asyncio.wait_for(ws.recv(), timeout=5)
        JobStatusUpdate.model_validate_json(response)
        # finished or failed
        response = await asyncio.wait_for(ws.recv(), timeout=5)
        JobStatusUpdate.model_validate_json(response)

    async def serve(self, ws):
        try:
            await self.verify_auth(ws)

            # send job request
            job_uuid = str(uuid.uuid4())
            await asyncio.wait_for(
                ws.send(self.get_dummy_job(job_uuid).model_dump_json()), timeout=5
            )

            await self.verify_job_status_update(ws)

            organic_job = await asyncio.wait_for(
                OrganicJob.objects.aget(job_uuid=job_uuid), timeout=5
            )
            if organic_job.status != OrganicJob.Status.COMPLETED:
                self.facilitator_error = Exception(f"job not completed: {organic_job.status}")
        except TimeoutError:
            self.facilitator_error = Exception("timed out")
        except Exception as e:
            self.facilitator_error = e
        finally:
            async with self.condition:
                self.condition.notify()


class FacilitatorJobStatusUpdatesWsV0(FacilitatorWs):
    def get_dummy_job(self, job_uuid) -> JobRequest:
        return get_dummy_job_request_v0(job_uuid)


class FacilitatorJobStatusUpdatesWsV1(FacilitatorWs):
    def get_dummy_job(self, job_uuid):
        return get_dummy_job_request_v1(job_uuid)


class FacilitatorJobStatusUpdatesWsV2(FacilitatorWs):
    def get_dummy_job(self, job_uuid):
        return get_dummy_job_request_v2(job_uuid)


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


class FacilitatorJobStatusUpdatesWsV2Retries(FacilitatorJobStatusUpdatesWsV2):
    async def serve(self, ws):
        try:
            await self.verify_auth(ws)

            # send job request
            job_uuid = str(uuid.uuid4())
            await asyncio.wait_for(
                ws.send(self.get_dummy_job(job_uuid).model_dump_json()), timeout=5
            )

            for _ in range(DYNAMIC_ORGANIC_JOB_MAX_RETRIES_OVERRIDE):
                await self.verify_job_status_update(ws)

            organic_job = await asyncio.wait_for(
                OrganicJob.objects.aget(job_uuid=job_uuid), timeout=5
            )
            if organic_job.status != OrganicJob.Status.FAILED:
                self.facilitator_error = Exception("job should have failed after retries")
        except TimeoutError:
            self.facilitator_error = Exception("timed out")
        except Exception as e:
            self.facilitator_error = e
        finally:
            async with self.condition:
                self.condition.notify()


@pytest.mark.asyncio
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
@pytest.mark.parametrize(
    "ws_server_cls",
    [
        FacilitatorJobStatusUpdatesWsV0,
        FacilitatorJobStatusUpdatesWsV1,
        FacilitatorJobStatusUpdatesWsV2,
        FacilitatorBadMessageWs,
    ],
)
async def test_facilitator_client__job_completed(ws_server_cls):
    await setup_db()
    ws_server = ws_server_cls()
    async with async_patch_all():
        async with websockets.serve(ws_server.serve, "127.0.0.1", 0) as server:
            host, port = server.sockets[0].getsockname()
            facilitator_uri = f"ws://{host}:{port}/"
            facilitator_client = FacilitatorClient(get_keypair(), facilitator_uri)

            facilitator_client.MINER_CLIENT_CLASS = MockSuccessfulMinerClient

            async with ws_server.condition:
                task = asyncio.create_task(facilitator_client.run_forever())
                await ws_server.condition.wait()

            await cancel_facilitator_tasks(facilitator_client, task)

            if ws_server.facilitator_error:
                pytest.fail(f"Test failed due to: {ws_server.facilitator_error}")


@pytest.mark.override_config(
    DYNAMIC_ORGANIC_JOB_MAX_RETRIES=DYNAMIC_ORGANIC_JOB_MAX_RETRIES_OVERRIDE
)
@pytest.mark.asyncio
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
async def test_facilitator_client__failed_job_retries():
    await setup_db()
    ws_server = FacilitatorJobStatusUpdatesWsV2Retries()
    async with async_patch_all():
        async with websockets.serve(ws_server.serve, "127.0.0.1", 0) as server:
            host, port = server.sockets[0].getsockname()
            facilitator_uri = f"ws://{host}:{port}/"

            facilitator_client = FacilitatorClient(get_keypair(), facilitator_uri)
            facilitator_client.MINER_CLIENT_CLASS = MockFaillingMinerClient

            async with ws_server.condition:
                task = asyncio.create_task(facilitator_client.run_forever())
                await ws_server.condition.wait()

            await cancel_facilitator_tasks(facilitator_client, task)
            if ws_server.facilitator_error:
                pytest.fail(f"Test failed due to: {ws_server.facilitator_error}")


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
    async def serve(self, ws):
        response = await asyncio.wait_for(ws.recv(), timeout=5)
        try:
            V0AuthenticationRequest.model_validate_json(response)
        except Exception as e:
            self.facilitator_error = e

        await asyncio.wait_for(ws.send(Response(status="success").model_dump_json()), timeout=5)

        async for message in ws:
            try:
                V0MachineSpecsUpdate.model_validate_json(message)
            except Exception:
                continue
            else:
                async with self.condition:
                    self.condition.notify()


@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
@pytest.mark.asyncio
async def test_fetch_miner_for_cross_validation__wrap_around():
    await setup_db(n := 5)

    async with async_patch_all():
        client = FacilitatorClient(get_keypair(), "ws://127.0.0.1:0/")

        # loop twice to check wrap-around behavior
        for i in range(n * 2):
            miner = await client.fetch_miner_for_cross_validation(DEFAULT_EXECUTOR_CLASS)
            expected_hotkey = f"miner_{i % n}"
            assert miner.hotkey == expected_hotkey
            assert client.last_miner_cross_validated == expected_hotkey

        await cancel_facilitator_tasks(client)


@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
@pytest.mark.asyncio
async def test_fetch_miner_for_cross_validation__no_matching_executor_class():
    await setup_db()

    async with async_patch_all():
        client = FacilitatorClient(get_keypair(), "ws://127.0.0.1:0/")
        client.last_miner_cross_validated = "some_miner"

        miner = await client.fetch_miner_for_cross_validation("NonExistentExecutorClass")
        assert miner is None
        assert client.last_miner_cross_validated is None

        await cancel_facilitator_tasks(client)


@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
@pytest.mark.asyncio
async def test_fetch_miner_for_cross_validation__no_online_executors():
    await setup_db()
    await MinerManifest.objects.all().aupdate(online_executor_count=0)

    async with async_patch_all():
        client = FacilitatorClient(get_keypair(), "ws://127.0.0.1:0/")
        client.last_miner_cross_validated = "some_miner"

        miner = await client.fetch_miner_for_cross_validation(DEFAULT_EXECUTOR_CLASS)
        assert miner is None
        assert client.last_miner_cross_validated is None

        await cancel_facilitator_tasks(client)


# TODO: this test is flaky, needs proper investigation
@pytest.mark.skip
@pytest.mark.asyncio
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
async def test_wait_for_specs(specs_msg: dict):
    layer = get_channel_layer()
    await layer.send(MACHINE_SPEC_CHANNEL, specs_msg)
    ws_server = FacilitatorExpectMachineSpecsWs()

    async with async_patch_all():
        async with websockets.serve(ws_server.serve, "127.0.0.1", 0) as server:
            host, port = server.sockets[0].getsockname()
            facilitator_uri = f"ws://{host}:{port}/"
            facilitator_client = FacilitatorClient(get_keypair(), facilitator_uri)

            facilitator_client.MINER_CLIENT_CLASS = MockSuccessfulMinerClient

            async with ws_server.condition:
                task = asyncio.create_task(facilitator_client.run_forever())
                await asyncio.wait_for(ws_server.condition.wait(), timeout=5)

            await cancel_facilitator_tasks(facilitator_client, task)
