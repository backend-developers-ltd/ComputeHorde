import uuid

import bittensor
import pytest

from compute_horde.base_requests import BaseRequest
from compute_horde.miner_client.organic import OrganicMinerClient
from compute_horde.mv_protocol.miner_requests import (
    V0AcceptJobRequest,
    V0DeclineJobRequest,
    V0ExecutorFailedRequest,
    V0ExecutorReadyRequest,
    V0JobFailedRequest,
    V0JobFinishedRequest,
)
from compute_horde.transport import StubTransport

JOB_UUID = "b4793a02-33a2-4a49-b4e2-4a7b903847e7"


def get_miner_client(
    keypair: bittensor.Keypair,
    messages: list[BaseRequest] | None = None,
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
@pytest.mark.parametrize(
    "initial_msg,executor_msg,final_msg",
    [
        (V0DeclineJobRequest(job_uuid=JOB_UUID), None, None),
        (V0AcceptJobRequest(job_uuid=JOB_UUID), V0ExecutorFailedRequest(job_uuid=JOB_UUID), None),
        (
            V0AcceptJobRequest(job_uuid=JOB_UUID),
            V0ExecutorReadyRequest(job_uuid=JOB_UUID),
            V0JobFailedRequest(
                job_uuid=JOB_UUID,
                docker_process_exit_status=1,
                docker_process_stdout="stdout",
                docker_process_stderr="stderr",
            ),
        ),
        (
            V0AcceptJobRequest(job_uuid=JOB_UUID),
            V0ExecutorReadyRequest(job_uuid=JOB_UUID),
            V0JobFinishedRequest(
                job_uuid=JOB_UUID,
                docker_process_stdout="stdout",
                docker_process_stderr="stderr",
            ),
        ),
    ],
)
async def test_organic_miner_client__futures__properly_set(
    initial_msg, executor_msg, final_msg, keypair
):
    miner_client = get_miner_client(keypair)

    assert not miner_client.miner_accepting_or_declining_future.done()
    assert miner_client.miner_accepting_or_declining_timestamp == 0

    await miner_client.handle_message(initial_msg)

    assert miner_client.miner_accepting_or_declining_future.done()
    assert miner_client.miner_accepting_or_declining_timestamp != 0
    assert await miner_client.miner_accepting_or_declining_future == initial_msg

    # should set only once
    set_time = miner_client.miner_accepting_or_declining_timestamp
    await miner_client.handle_message(initial_msg)
    assert miner_client.miner_accepting_or_declining_timestamp == set_time

    if executor_msg is None:
        return

    assert not miner_client.executor_ready_or_failed_future.done()
    assert miner_client.executor_ready_or_failed_timestamp == 0

    await miner_client.handle_message(executor_msg)

    assert miner_client.executor_ready_or_failed_future.done()
    assert miner_client.executor_ready_or_failed_future != 0
    assert await miner_client.executor_ready_or_failed_future == executor_msg

    # should set only once
    set_time = miner_client.executor_ready_or_failed_timestamp
    await miner_client.handle_message(executor_msg)
    assert miner_client.executor_ready_or_failed_timestamp == set_time

    if final_msg is None:
        return

    assert not miner_client.miner_finished_or_failed_future.done()
    assert miner_client.miner_finished_or_failed_timestamp == 0

    await miner_client.handle_message(final_msg)

    assert miner_client.miner_finished_or_failed_future.done()
    assert miner_client.miner_finished_or_failed_timestamp != 0
    assert await miner_client.miner_finished_or_failed_future == final_msg

    # should set only once
    set_time = miner_client.miner_finished_or_failed_timestamp
    await miner_client.handle_message(final_msg)
    assert miner_client.miner_finished_or_failed_timestamp == set_time


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

    assert not miner_client.miner_accepting_or_declining_future.done()
    assert miner_client.miner_accepting_or_declining_timestamp == 0

    await miner_client.handle_message(initial_msg)

    assert not miner_client.miner_accepting_or_declining_future.done()
    assert miner_client.miner_accepting_or_declining_timestamp == 0


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
        ),
    ],
)
async def test_organic_miner_client__skip_different_job__final_future(final_msg, keypair):
    miner_client = get_miner_client(keypair)

    assert not miner_client.miner_finished_or_failed_future.done()
    assert miner_client.miner_finished_or_failed_timestamp == 0

    await miner_client.handle_message(final_msg)

    assert not miner_client.miner_finished_or_failed_future.done()
    assert miner_client.miner_finished_or_failed_timestamp == 0
