import pytest
from compute_horde.mv_protocol import miner_requests

from compute_horde_validator.validator.models import OrganicJob
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


async def test_basic_flow_works(job_request, faci_transport, miner_transport, execute_scenario):
    await faci_transport.add_message(job_request, send_before=0)

    # vali -> miner: initial job request

    accept_job_msg = miner_requests.V0AcceptJobRequest(job_uuid=job_request.uuid)
    await miner_transport.add_message(accept_job_msg, send_before=1)

    # vali -> miner: receipt for accepting the job
    # vali -> faci: job status update

    executor_ready_msg = miner_requests.V0ExecutorReadyRequest(job_uuid=job_request.uuid)
    await miner_transport.add_message(executor_ready_msg, send_before=1)

    # vali -> miner: actual job request

    job_finished_msg = miner_requests.V0JobFinishedRequest(
        job_uuid=job_request.uuid, docker_process_stdout="", docker_process_stderr=""
    )
    await miner_transport.add_message(job_finished_msg, send_before=1)

    # vali -> miner: receipt for finishing job
    # vali -> faci: job status update

    await execute_scenario(until=lambda: len(faci_transport.sent) >= 3)

    assert JobStatusUpdate.model_validate_json(faci_transport.sent[-1]).status == "completed"
    assert (
        await OrganicJob.objects.aget(job_uuid=job_request.uuid)
    ).status == OrganicJob.Status.COMPLETED
