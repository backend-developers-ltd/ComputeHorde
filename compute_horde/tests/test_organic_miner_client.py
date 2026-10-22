import asyncio
import datetime
import uuid
from unittest import mock

import bittensor
import pytest
from compute_horde_core.executor_class import ExecutorClass
from compute_horde_core.streaming import StreamingDetails
from compute_horde_core.volume import InlineVolume
from freezegun import freeze_time
from pydantic import BaseModel, TypeAdapter

from compute_horde.miner_client.organic import (
    MinerRejectedJob,
    MinerReportedHordeFailed,
    MinerReportedJobFailed,
    OrganicJobDetails,
    OrganicMinerClient,
)
from compute_horde.protocol_consts import HordeFailureReason, JobParticipantType
from compute_horde.protocol_messages import (
    V0AcceptJobRequest,
    V0DeclineJobRequest,
    V0ExecutorReadyRequest,
    V0HordeFailedRequest,
    V0InitialJobRequest,
    V0JobFailedRequest,
    V0JobFinishedRequest,
    ValidatorAuthForMiner,
    ValidatorToMinerMessage,
)
from compute_horde.receipts.schemas import JobFinishedReceiptPayload
from compute_horde.transport import StubTransport
from tests.ws_server import ReconnectingWSTestServer

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


@pytest.mark.asyncio
async def test_organic_miner_client__reauthenticates_after_reconnect(keypair, unused_tcp_port):
    # The reconnect handshake takes a second, so the renewed authentication message carries a
    # later timestamp than the first one.
    async with ReconnectingWSTestServer(
        port=unused_tcp_port, drop_first_connection_after=1, handshake_delay=1
    ) as miner:
        async with OrganicMinerClient(
            miner_hotkey="mock",
            miner_address=miner.host,
            miner_port=miner.port,
            job_uuid=JOB_UUID,
            my_keypair=keypair,
        ):
            received = [await asyncio.wait_for(miner.received.get(), 3) for _ in range(2)]

    messages = [
        (connection, TypeAdapter(ValidatorToMinerMessage).validate_json(raw))
        for connection, raw in received
    ]
    auth = ValidatorAuthForMiner.model_construct(
        validator_hotkey=keypair.ss58_address,
        miner_hotkey="mock",
        timestamp=mock.ANY,
        signature=mock.ANY,
    )
    assert messages == [(1, auth), (2, auth)]
    (_, first_auth), (_, second_auth) = messages
    assert second_auth.timestamp > first_auth.timestamp
    assert keypair.verify(second_auth.blob_for_signing(), second_auth.signature)


@pytest.mark.asyncio
async def test_organic_miner_client__signs_initial_job_request(keypair):
    miner_client = get_miner_client(keypair)
    volume = InlineVolume(contents="aW5wdXQ=", relative_path="input")
    job_details = OrganicJobDetails(
        job_uuid=JOB_UUID,
        executor_class=ExecutorClass.always_on__llm__a6000,
        docker_image="mock",
        job_timing=OrganicJobDetails.TimingDetails(1, 2, 3, 4, 5),
        volume=volume,
        streaming_details=StreamingDetails(public_key="certificate"),
    )
    receipt_payload, receipt_signature = miner_client.generate_job_started_receipt_message(
        ExecutorClass.always_on__llm__a6000, ttl=5
    )

    await miner_client.send_initial_job_request(job_details, receipt_payload, receipt_signature)

    [sent] = [
        TypeAdapter(ValidatorToMinerMessage).validate_json(message)
        for message in miner_client.transport.sent_messages
    ]
    assert sent == V0InitialJobRequest(
        job_uuid=JOB_UUID,
        executor_class=ExecutorClass.always_on__llm__a6000,
        docker_image="mock",
        timeout_seconds=300,
        volume=volume,
        job_started_receipt_payload=receipt_payload,
        job_started_receipt_signature=receipt_signature,
        streaming_details=StreamingDetails(public_key="certificate"),
        executor_timing=V0InitialJobRequest.ExecutorTimingDetails(
            allowed_leeway=1,
            download_time_limit=2,
            execution_time_limit=3,
            upload_time_limit=4,
            streaming_start_time_limit=5,
        ),
        validator_signature=sent.validator_signature,
    )
    assert sent.is_signed_by(keypair.ss58_address)


@pytest.mark.asyncio
async def test_organic_miner_client__job_finished_receipt_clamps_negative_duration(keypair):
    miner_client = get_miner_client(keypair)
    started_at = datetime.datetime(2025, 3, 4, 12, 0, tzinfo=datetime.UTC)

    with freeze_time(started_at):
        message = miner_client.generate_job_finished_receipt_message(
            started_timestamp=started_at.timestamp(),
            time_took_seconds=-0.5,
            score=1.5,
            allowance_blocks=[7, 8],
        )

    assert message.payload == JobFinishedReceiptPayload(
        job_uuid=JOB_UUID,
        miner_hotkey="mock",
        validator_hotkey=keypair.ss58_address,
        timestamp=started_at,
        time_started=started_at,
        time_took_us=0,
        block_numbers=[7, 8],
        score_str="1.500000",
    )
