import asyncio
import uuid
from collections import namedtuple
from unittest.mock import AsyncMock, Mock, patch

import pytest
from compute_horde.executor_class import DEFAULT_EXECUTOR_CLASS
from compute_horde.fv_protocol import facilitator_requests
from compute_horde.miner_client.organic import OrganicMinerClient
from compute_horde.mv_protocol import miner_requests
from compute_horde.transport import AbstractTransport

from compute_horde_validator.validator.models import (
    Miner,
    MinerManifest,
    OrganicJob,
    SyntheticJobBatch,
)
from compute_horde_validator.validator.organic_jobs.facilitator_client import FacilitatorClient
from compute_horde_validator.validator.organic_jobs.miner_driver import JobStatusUpdate
from compute_horde_validator.validator.tests.transport import SimulationTransport


@pytest.fixture
def miner(miner_keypair):
    return Miner.objects.create(hotkey=miner_keypair.ss58_address)


class _SimulationTransportWsAdapter:
    """
    Dirty hack to adapt the SimulationTransport into FacilitatorClient, which doesn't abstract away its dependence
    on WS socket. Quack.
    TODO: Refactor FacilitatorClient.
    """

    def __init__(self, transport: AbstractTransport):
        self.transport = transport

    async def send(self, msg: str):
        await self.transport.send(msg)

    async def recv(self) -> str:
        return await self.transport.receive()

    def __aiter__(self):
        return self.transport


@pytest.fixture(autouse=True)
def seed_database(miner):
    batch = SyntheticJobBatch.objects.create()
    MinerManifest.objects.create(
        miner=miner,
        batch=batch,
        executor_class=DEFAULT_EXECUTOR_CLASS,
        executor_count=5,
        online_executor_count=5,
    )


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
@patch.object(FacilitatorClient, "heartbeat", AsyncMock())
@patch.object(FacilitatorClient, "create_metagraph_refresh_task", Mock())
@patch.object(FacilitatorClient, "get_current_block", AsyncMock(return_value=1))
@patch.object(
    FacilitatorClient,
    "get_miner_axon_info",
    AsyncMock(
        return_value=namedtuple("MockAxonInfo", "ip,port,ip_type")(
            ip="500.500.500.500", port=1, ip_type=4
        )
    ),
)
@patch(
    "compute_horde_validator.validator.organic_jobs.facilitator_client.verify_job_request",
    AsyncMock(),
)
async def test_miner_gets_blacklisted(miner, miner_keypair, validator_keypair):
    miner = SimulationTransport("miner")
    faci = SimulationTransport("facilitator")

    def fake_miner_client_factory(*args, **kwargs):
        return OrganicMinerClient(*args, **kwargs, transport=miner)

    faci_client = FacilitatorClient(validator_keypair, "")
    faci_client.MINER_CLIENT_CLASS = fake_miner_client_factory

    await faci.add_message('{"status":"success"}', send_before=1)
    job_request = facilitator_requests.V2JobRequest(
        uuid=str(uuid.uuid4()),
        executor_class=DEFAULT_EXECUTOR_CLASS,
        docker_image="doesntmatter",
        raw_script="doesntmatter",
        args=[],
        env={},
        use_gpu=False,
    )
    await faci.add_message(job_request.model_dump_json(), send_before=0)

    # vali should now select the test miner and offer it a job
    accept_job_msg = miner_requests.V0AcceptJobRequest(job_uuid=job_request.uuid)
    await miner.add_message(accept_job_msg.model_dump_json(), send_before=1)

    executor_ready_msg = miner_requests.V0ExecutorReadyRequest(job_uuid=job_request.uuid)
    await miner.add_message(executor_ready_msg.model_dump_json(), send_before=1)

    job_finished_msg = miner_requests.V0JobFinishedRequest(
        job_uuid=job_request.uuid, docker_process_stdout="", docker_process_stderr=""
    )
    await miner.add_message(job_finished_msg.model_dump_json(), send_before=1)

    async with faci_client, faci.receive_condition, asyncio.timeout(10):
        faci_loop = asyncio.create_task(
            faci_client.handle_connection(_SimulationTransportWsAdapter(faci))
        )
        await faci.receive_condition.wait_for(lambda: len(faci.sent) >= 3)

    faci_loop.cancel()
    try:
        await faci_loop
    except asyncio.CancelledError:
        pass

    assert JobStatusUpdate.model_validate_json(faci.sent[-1]).status == "completed"
    assert (
        await OrganicJob.objects.aget(job_uuid=job_request.uuid)
    ).status == OrganicJob.Status.COMPLETED
