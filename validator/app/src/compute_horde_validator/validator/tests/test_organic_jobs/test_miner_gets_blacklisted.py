import pytest
from compute_horde import mv_protocol
from compute_horde.mv_protocol import miner_requests

from compute_horde_validator.validator.organic_jobs.miner_driver import JobStatusUpdate

# NOTE: In case this test is taking unreasonable amount of time before timing out:
# Something during the job flow is causing the execute_scenario timeout to be ignored.
# Maybe some cancellation gets eaten somewhere, or a timeout exception, no idea.
# The test will actually time out after one of these overridden timeouts though:
pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.django_db(transaction=True),
    pytest.mark.override_config(
        DYNAMIC_ORGANIC_JOB_TIMEOUT=1,
        DYNAMIC_ORGANIC_JOB_INITIAL_RESPONSE_TIMEOUT=0.5,
        DYNAMIC_ORGANIC_JOB_EXECUTOR_READY_TIMEOUT=0.5,
    ),
]


@pytest.mark.parametrize("reason", [*miner_requests.V0DeclineJobRequest.Reason])
async def test_miner_is_blacklisted__after_rejecting_job(
    job_request, another_job_request, faci_transport, miner_transport, execute_scenario, reason
):
    # Faci -> vali: V2 job request
    await faci_transport.add_message(job_request, send_before=0)

    # Vali -> miner: initial job request

    # Miner -> vali: rejects job
    accept_job_msg = miner_requests.V0DeclineJobRequest(job_uuid=job_request.uuid, reason=reason)
    await miner_transport.add_message(accept_job_msg, send_before=1)

    # Vali -> faci: job failed

    # Faci: request another job
    await faci_transport.add_message(another_job_request, send_before=1)

    # Vali -> faci: job rejected (no miners to take it)
    await execute_scenario(until=lambda: len(faci_transport.sent) >= 3)

    assert len(faci_transport.sent) == 3

    miner_rejected = JobStatusUpdate.model_validate_json(faci_transport.sent[-2])
    vali_rejected = JobStatusUpdate.model_validate_json(faci_transport.sent[-1])

    assert miner_rejected.status == "rejected"
    assert miner_rejected.metadata.comment.startswith("Miner declined job")
    assert miner_rejected.metadata.miner_response is not None

    assert vali_rejected.status == "rejected"
    assert vali_rejected.metadata.comment.startswith("No executor for job request")
    assert vali_rejected.metadata.miner_response is None


@pytest.mark.parametrize(
    "timeout_stage,expected_status_updates",
    [
        (
            0,
            [
                ("failed", "timed out waiting for initial response"),
                ("rejected", "No executor for job request"),
            ],
        ),
        (
            1,
            [
                ("accepted", ""),
                ("failed", "timed out while preparing executor"),
                ("rejected", "No executor for job request"),
            ],
        ),
        (
            2,
            [
                ("accepted", ""),
                ("failed", "timed out after"),
                ("rejected", "No executor for job request"),
            ],
        ),
    ],
)
async def test_miner_is_blacklisted__after_timing_out(
    job_request,
    another_job_request,
    faci_transport,
    miner_transport,
    execute_scenario,
    timeout_stage,
    expected_status_updates,
):
    await faci_transport.add_message(job_request, send_before=0)
    # Miner: timeout here (stage==0)

    if timeout_stage > 0:
        accept_job_msg = miner_requests.V0AcceptJobRequest(job_uuid=job_request.uuid)
        await miner_transport.add_message(accept_job_msg, send_before=1)
        # Miner: timeout here (stage==1)

    if timeout_stage > 1:
        executor_ready_msg = miner_requests.V0ExecutorReadyRequest(job_uuid=job_request.uuid)
        await miner_transport.add_message(executor_ready_msg, send_before=1)
        # Miner: timeout here (stage==2)

    await faci_transport.add_message(
        another_job_request,
        send_before=1 if timeout_stage == 0 else 2,
    )

    await execute_scenario(
        until=lambda: len(faci_transport.sent) >= len(expected_status_updates) + 1,
        timeout_seconds=3,
    )

    for i, (status, comment) in enumerate(expected_status_updates, start=1):
        status_message = JobStatusUpdate.model_validate_json(faci_transport.sent[i])
        assert status_message.status == status
        assert comment in status_message.metadata.comment


async def test_miner_is_blacklisted__after_failing_to_start_executor(
    job_request,
    another_job_request,
    faci_transport,
    miner_transport,
    execute_scenario,
):
    await faci_transport.add_message(job_request, send_before=0)

    accept_job_msg = miner_requests.V0AcceptJobRequest(job_uuid=job_request.uuid)
    await miner_transport.add_message(accept_job_msg, send_before=1)

    await miner_transport.add_message(
        mv_protocol.miner_requests.V0ExecutorFailedRequest(job_uuid=job_request.uuid),
        send_before=1,
    )

    await faci_transport.add_message(
        another_job_request,
        send_before=2,  # job status=accepted, job status=failed
    )

    await execute_scenario(until=lambda: len(faci_transport.sent) >= 4, timeout_seconds=3)

    accepted_status_msg = JobStatusUpdate.model_validate_json(faci_transport.sent[1])
    failed_status_msg = JobStatusUpdate.model_validate_json(faci_transport.sent[2])
    rejected_status_msg = JobStatusUpdate.model_validate_json(faci_transport.sent[3])

    assert accepted_status_msg.status == "accepted"
    assert failed_status_msg.status == "failed"
    assert rejected_status_msg.status == "rejected"


async def test_miner_is_blacklisted__after_failing_job(
    job_request,
    another_job_request,
    faci_transport,
    miner_transport,
    execute_scenario,
):
    await faci_transport.add_message(job_request, send_before=0)

    accept_job_msg = miner_requests.V0AcceptJobRequest(job_uuid=job_request.uuid)
    await miner_transport.add_message(accept_job_msg, send_before=1)

    executor_ready_msg = miner_requests.V0ExecutorReadyRequest(job_uuid=job_request.uuid)
    await miner_transport.add_message(executor_ready_msg, send_before=1)

    await miner_transport.add_message(
        mv_protocol.miner_requests.V0JobFailedRequest(
            job_uuid=job_request.uuid,
            docker_process_exit_status=1,
            docker_process_stdout="stdout",
            docker_process_stderr="stderr",
        ),
        send_before=1,
    )

    await faci_transport.add_message(
        another_job_request,
        send_before=2,  # job status=accepted, job status=failed
    )

    await execute_scenario(until=lambda: len(faci_transport.sent) >= 4, timeout_seconds=3)

    accepted_status_msg = JobStatusUpdate.model_validate_json(faci_transport.sent[1])
    failed_status_msg = JobStatusUpdate.model_validate_json(faci_transport.sent[2])
    rejected_status_msg = JobStatusUpdate.model_validate_json(faci_transport.sent[3])

    assert accepted_status_msg.status == "accepted"
    assert failed_status_msg.status == "failed"
    assert rejected_status_msg.status == "rejected"
