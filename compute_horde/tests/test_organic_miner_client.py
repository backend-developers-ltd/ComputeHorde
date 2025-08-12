import uuid

import bittensor
import pytest
from pydantic import BaseModel

from compute_horde.miner_client.organic import (
    MinerRejectedJob,
    MinerReportedHordeFailed,
    MinerReportedJobFailed,
    OrganicMinerClient,
)
from compute_horde.protocol_consts import HordeFailureReason, JobParticipantType
from compute_horde.protocol_messages import (
    V0AcceptJobRequest,
    V0DeclineJobRequest,
    V0ExecutorReadyRequest,
    V0HordeFailedRequest,
    V0JobFailedRequest,
    V0JobFinishedRequest,
)
from compute_horde.transport import StubTransport

JOB_UUID = "b4793a02-33a2-4a49-b4e2-4a7b903847e7"


def get_miner_client(
    keypair: bittensor.Keypair,
    messages: list[BaseModel] | None = None,
) -> OrganicMinerClient:
    transport = StubTransport("stub", messages or [])
    client = OrganicMinerClient(
        miner_hotkey="mock",
        miner_address="0.0.0.0",
        miner_port=1234,
        job_uuid=JOB_UUID,
        my_keypair=keypair,
        transport=transport,
    )
    return client


@pytest.mark.asyncio
@pytest.mark.xfail(raises=MinerRejectedJob)
async def test_organic_miner_client__throws_when_miner_rejects_job(keypair):
    miner_client = get_miner_client(keypair)

    initial_msg = V0DeclineJobRequest(job_uuid=JOB_UUID)
    await miner_client.handle_message(initial_msg)
    await miner_client.job_accepted_future


@pytest.mark.asyncio
@pytest.mark.xfail(raises=MinerReportedHordeFailed)
async def test_organic_miner_client__throws_when_horde_failed(keypair):
    miner_client = get_miner_client(keypair)

    initial_msg = V0AcceptJobRequest(job_uuid=JOB_UUID)
    executor_msg = V0HordeFailedRequest(
        job_uuid=JOB_UUID,
        reported_by=JobParticipantType.EXECUTOR,
        message="Nope",
        reason=HordeFailureReason.GENERIC_ERROR,
    )

    await miner_client.handle_message(initial_msg)
    await miner_client.handle_message(executor_msg)
    await miner_client.executor_ready_future


@pytest.mark.asyncio
@pytest.mark.xfail(raises=MinerReportedJobFailed)
async def test_organic_miner_client__throws_when_job_failed(keypair):
    miner_client = get_miner_client(keypair)

    initial_msg = V0AcceptJobRequest(job_uuid=JOB_UUID)
    executor_msg = V0ExecutorReadyRequest(job_uuid=JOB_UUID)
    final_msg = V0JobFailedRequest(
        job_uuid=JOB_UUID,
        docker_process_exit_status=1,
        docker_process_stdout="stdout",
        docker_process_stderr="stderr",
    )

    await miner_client.handle_message(initial_msg)
    await miner_client.handle_message(executor_msg)
    await miner_client.handle_message(final_msg)
    await miner_client.job_finished_future


@pytest.mark.asyncio
async def test_organic_miner_client__happy_path_futures_set_and_idempotent(keypair):
    miner_client = get_miner_client(keypair)

    initial_msg = V0AcceptJobRequest(job_uuid=JOB_UUID)
    executor_msg = V0ExecutorReadyRequest(job_uuid=JOB_UUID)
    final_msg = V0JobFinishedRequest(
        job_uuid=JOB_UUID,
        docker_process_stdout="stdout",
        docker_process_stderr="stderr",
        artifacts={},
    )

    assert not miner_client.job_accepted_future.done()
    await miner_client.handle_message(initial_msg)
    assert miner_client.job_accepted_future.done()
    assert await miner_client.job_accepted_future == initial_msg

    # should set only once
    another_initial_message = initial_msg.model_copy()
    await miner_client.handle_message(another_initial_message)
    assert await miner_client.job_accepted_future == initial_msg

    assert not miner_client.executor_ready_future.done()
    await miner_client.handle_message(executor_msg)
    assert miner_client.executor_ready_future.done()
    assert await miner_client.executor_ready_future == executor_msg

    # should set only once
    another_executor_msg = executor_msg.model_copy()
    await miner_client.handle_message(another_executor_msg)
    assert await miner_client.executor_ready_future == executor_msg

    assert not miner_client.job_finished_future.done()
    await miner_client.handle_message(final_msg)
    assert miner_client.job_finished_future.done()
    assert await miner_client.job_finished_future == final_msg

    # should set only once
    another_final_msg = final_msg.model_copy()
    await miner_client.handle_message(another_final_msg)
    assert await miner_client.job_finished_future == final_msg


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "initial_msg",
    [
        V0AcceptJobRequest(job_uuid=str(uuid.uuid4())),
        V0DeclineJobRequest(job_uuid=str(uuid.uuid4())),
    ],
)
async def test_organic_miner_client__skip_different_job__initial_future(initial_msg, keypair):
    miner_client = get_miner_client(keypair)

    assert not miner_client.job_accepted_future.done()

    await miner_client.handle_message(initial_msg)

    assert not miner_client.job_accepted_future.done()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "final_msg",
    [
        V0JobFailedRequest(
            job_uuid=str(uuid.uuid4()),
            docker_process_exit_status=1,
            docker_process_stdout="stdout",
            docker_process_stderr="stderr",
        ),
        V0JobFinishedRequest(
            job_uuid=str(uuid.uuid4()),
            docker_process_stdout="stdout",
            docker_process_stderr="stderr",
            artifacts={},
        ),
    ],
)
async def test_organic_miner_client__skip_different_job__final_future(final_msg, keypair):
    miner_client = get_miner_client(keypair)

    assert not miner_client.job_finished_future.done()

    await miner_client.handle_message(final_msg)

    assert not miner_client.job_finished_future.done()
