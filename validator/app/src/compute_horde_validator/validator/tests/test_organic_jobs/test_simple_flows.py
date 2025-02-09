import uuid

import pytest
from compute_horde.executor_class import DEFAULT_EXECUTOR_CLASS
from compute_horde.fv_protocol import facilitator_requests
from compute_horde.mv_protocol import miner_requests

from compute_horde_validator.validator.models import (
    OrganicJob,
)
from compute_horde_validator.validator.organic_jobs.miner_driver import JobStatusUpdate

job_request = facilitator_requests.V2JobRequest(
    uuid=str(uuid.uuid4()),
    executor_class=DEFAULT_EXECUTOR_CLASS,
    docker_image="doesntmatter",
    raw_script="doesntmatter",
    args=[],
    env={},
    use_gpu=False,
)


@pytest.mark.asyncio
@pytest.mark.django_db(transaction=True)
async def test_basic_flow_works(faci_transport, miner_transport, let_it_rip):
    # Faci: starts with sending a V2 job request to vali
    await faci_transport.add_message(job_request.model_dump_json(), send_before=0)

    # Vali: should now select the test miner and offer it a job

    # Miner: accepts job
    accept_job_msg = miner_requests.V0AcceptJobRequest(job_uuid=job_request.uuid)
    await miner_transport.add_message(accept_job_msg.model_dump_json(), send_before=1)

    # Vali -> Miner: receipt for accepting the job
    # Vali -> Faci: job status update

    # Miner: reports the executor is ready
    executor_ready_msg = miner_requests.V0ExecutorReadyRequest(job_uuid=job_request.uuid)
    await miner_transport.add_message(executor_ready_msg.model_dump_json(), send_before=1)

    # Vali -> Miner: actual job request

    # Miner: finishes the job
    job_finished_msg = miner_requests.V0JobFinishedRequest(
        job_uuid=job_request.uuid, docker_process_stdout="", docker_process_stderr=""
    )
    await miner_transport.add_message(job_finished_msg.model_dump_json(), send_before=1)

    # Vali -> Miner: receipt for finishing job
    # Vali -> Faci: job status update

    await let_it_rip(until=lambda: len(faci_transport.sent) >= 3)

    assert JobStatusUpdate.model_validate_json(faci_transport.sent[-1]).status == "completed"
    assert (
        await OrganicJob.objects.aget(job_uuid=job_request.uuid)
    ).status == OrganicJob.Status.COMPLETED


@pytest.mark.asyncio
@pytest.mark.django_db(transaction=True, databases=["default", "default_alias"])
@pytest.mark.parametrize("reason", [*miner_requests.V0DeclineJobRequest.Reason])
async def test_miner_is_blacklisted__after_rejecting_job(
    faci_transport, miner_transport, let_it_rip, reason
):
    # Faci: starts with sending a V2 job request to vali
    await faci_transport.add_message(job_request.model_dump_json(), send_before=0)

    # Vali: should now select the test miner and offer it a job

    # Miner: rejects job
    accept_job_msg = miner_requests.V0DeclineJobRequest(job_uuid=job_request.uuid, reason=reason)
    await miner_transport.add_message(accept_job_msg.model_dump_json(), send_before=1)

    # Vali -> faci: job failed

    # Faci: request another job
    await faci_transport.add_message(
        job_request.__replace__(uuid=str(uuid.uuid4())).model_dump_json(),
        send_before=1,
    )

    # Vali -> faci: job rejected (no miners to take it)
    try:
        await let_it_rip(until=lambda: len(faci_transport.sent) >= 3)
    except TimeoutError:
        pass

    miner_rejected = JobStatusUpdate.model_validate_json(faci_transport.sent[-2])
    vali_rejected = JobStatusUpdate.model_validate_json(faci_transport.sent[-1])

    assert miner_rejected.status == "rejected"
    assert miner_rejected.metadata.comment.startswith("Miner declined job")
    assert miner_rejected.metadata.miner_response is not None

    assert vali_rejected.status == "rejected"
    assert vali_rejected.metadata.comment.startswith("No executor for job request")
    assert vali_rejected.metadata.miner_response is None
