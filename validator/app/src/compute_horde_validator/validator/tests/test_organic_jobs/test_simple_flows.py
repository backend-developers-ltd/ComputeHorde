import uuid

import pytest
from compute_horde.executor_class import DEFAULT_EXECUTOR_CLASS
from compute_horde.fv_protocol import facilitator_requests
from compute_horde.mv_protocol import miner_requests

from compute_horde_validator.validator.models import (
    OrganicJob,
)
from compute_horde_validator.validator.organic_jobs.miner_driver import JobStatusUpdate

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.django_db(transaction=True),
    pytest.mark.override_config(
        DYNAMIC_ORGANIC_JOB_TIMEOUT=2,
        DYNAMIC_ORGANIC_JOB_INITIAL_RESPONSE_TIMEOUT=1,
        DYNAMIC_ORGANIC_JOB_EXECUTOR_READY_TIMEOUT=1,
    ),
]

_JOB_REQUEST = facilitator_requests.V2JobRequest(
    uuid=str(uuid.uuid4()),
    executor_class=DEFAULT_EXECUTOR_CLASS,
    docker_image="doesntmatter",
    raw_script="doesntmatter",
    args=[],
    env={},
    use_gpu=False,
)

_ANOTHER_JOB_REQUEST = _JOB_REQUEST.__replace__(uuid=str(uuid.uuid4()))


async def test_basic_flow_works(faci_transport, miner_transport, let_it_rip):
    # Faci -> vali: V2 job request
    await faci_transport.add_message(_JOB_REQUEST.model_dump_json(), send_before=0)

    # Vali -> miner: initial job request

    # Miner -> vali: accept job
    accept_job_msg = miner_requests.V0AcceptJobRequest(job_uuid=_JOB_REQUEST.uuid)
    await miner_transport.add_message(accept_job_msg.model_dump_json(), send_before=1)

    # Vali -> Miner: receipt for accepting the job
    # Vali -> Faci: job status update

    # Miner -> vali: executor is ready
    executor_ready_msg = miner_requests.V0ExecutorReadyRequest(job_uuid=_JOB_REQUEST.uuid)
    await miner_transport.add_message(executor_ready_msg.model_dump_json(), send_before=1)

    # Vali -> Miner: actual job request

    # Miner -> vali: job finished
    job_finished_msg = miner_requests.V0JobFinishedRequest(
        job_uuid=_JOB_REQUEST.uuid, docker_process_stdout="", docker_process_stderr=""
    )
    await miner_transport.add_message(job_finished_msg.model_dump_json(), send_before=1)

    # Vali -> Miner: receipt for finishing job
    # Vali -> Faci: job status update

    await let_it_rip(until=lambda: len(faci_transport.sent) >= 3)

    assert JobStatusUpdate.model_validate_json(faci_transport.sent[-1]).status == "completed"
    assert (
        await OrganicJob.objects.aget(job_uuid=_JOB_REQUEST.uuid)
    ).status == OrganicJob.Status.COMPLETED


@pytest.mark.parametrize("reason", [*miner_requests.V0DeclineJobRequest.Reason])
async def test_miner_is_blacklisted__after_rejecting_job(
    faci_transport, miner_transport, let_it_rip, reason
):
    # Faci -> vali: V2 job request
    await faci_transport.add_message(_JOB_REQUEST.model_dump_json(), send_before=0)

    # Vali -> miner: initial job request

    # Miner -> vali: rejects job
    accept_job_msg = miner_requests.V0DeclineJobRequest(job_uuid=_JOB_REQUEST.uuid, reason=reason)
    await miner_transport.add_message(accept_job_msg.model_dump_json(), send_before=1)

    # Vali -> faci: job failed

    # Faci: request another job
    await faci_transport.add_message(_ANOTHER_JOB_REQUEST.model_dump_json(), send_before=1)

    # Vali -> faci: job rejected (no miners to take it)
    await let_it_rip(until=lambda: len(faci_transport.sent) >= 3)

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
    faci_transport,
    miner_transport,
    let_it_rip,
    timeout_stage,
    expected_status_updates,
):
    # Faci -> vali: V2 job request
    await faci_transport.add_message(_JOB_REQUEST.model_dump_json(), send_before=0)

    # Vali -> miner: initial job request
    # Miner: timeout here (stage==0)

    if timeout_stage > 0:
        # Miner -> vali: accept job
        accept_job_msg = miner_requests.V0AcceptJobRequest(job_uuid=_JOB_REQUEST.uuid)
        await miner_transport.add_message(accept_job_msg.model_dump_json(), send_before=1)

        # Vali -> Miner: receipt for accepting the job
        # Vali -> Faci: job status update

        # Miner: timeout here (stage==1)

    if timeout_stage > 1:
        # Miner -> vali: executor is ready
        executor_ready_msg = miner_requests.V0ExecutorReadyRequest(job_uuid=_JOB_REQUEST.uuid)
        await miner_transport.add_message(executor_ready_msg.model_dump_json(), send_before=1)

        # Vali -> Miner: actual job request
        # Miner: timeout here (stage==2)

    # Faci: request another job
    await faci_transport.add_message(
        _ANOTHER_JOB_REQUEST.model_dump_json(),
        send_before=1 if timeout_stage == 0 else 2,
    )

    await let_it_rip(
        until=lambda: len(faci_transport.sent) >= len(expected_status_updates) + 1,
        timeout_seconds=3,
    )

    for i, (status, comment) in enumerate(expected_status_updates, start=1):
        status_message = JobStatusUpdate.model_validate_json(faci_transport.sent[i])
        assert status_message.status == status
        assert comment in status_message.metadata.comment
