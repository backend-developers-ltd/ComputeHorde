import pytest
from compute_horde.protocol_messages import (
    V0AcceptJobRequest,
    V0ExecutorReadyRequest,
    V0JobFinishedRequest,
)

from compute_horde_validator.validator.models import Miner, OrganicJob
from compute_horde_validator.validator.organic_jobs.miner_driver import JobStatusUpdate

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.django_db(transaction=True),
    pytest.mark.override_config(
        DYNAMIC_ORGANIC_JOB_TIMEOUT=1,
        DYNAMIC_ORGANIC_JOB_INITIAL_RESPONSE_TIMEOUT=0.5,
        DYNAMIC_ORGANIC_JOB_EXECUTOR_READY_TIMEOUT=0.5,
    ),
]


async def test_basic_flow_works(job_request, faci_transport, miner_transport, execute_scenario):
    await faci_transport.add_message(job_request, send_before=0)

    # vali -> miner: initial job request

    accept_job_msg = V0AcceptJobRequest(job_uuid=job_request.uuid)
    await miner_transport.add_message(accept_job_msg, send_before=1)

    # vali -> miner: receipt for accepting the job
    # vali -> faci: job status update

    executor_ready_msg = V0ExecutorReadyRequest(job_uuid=job_request.uuid)
    await miner_transport.add_message(executor_ready_msg, send_before=1)

    # vali -> miner: actual job request

    job_finished_msg = V0JobFinishedRequest(
        job_uuid=job_request.uuid, docker_process_stdout="", docker_process_stderr=""
    )
    await miner_transport.add_message(job_finished_msg, send_before=2)

    # vali -> miner: receipt for finishing job
    # vali -> faci: job status update

    await execute_scenario(until=lambda: len(faci_transport.sent) >= 3)

    assert JobStatusUpdate.model_validate_json(faci_transport.sent[2]).status == "completed"
    assert (
        await OrganicJob.objects.aget(job_uuid=job_request.uuid)
    ).status == OrganicJob.Status.COMPLETED


async def test_miner_can_be_selected_after_finishing_job(
    miner,
    job_request,
    another_job_request,
    faci_transport,
    miner_transport,
    miner_transports,
    execute_scenario,
):
    # we will re-select the same miner as soon as it "finishes" its first job
    assert await Miner.objects.acount() == 1

    # Job 1
    await faci_transport.add_message(job_request, send_before=0)

    accept_job_msg = V0AcceptJobRequest(job_uuid=job_request.uuid)
    await miner_transport.add_message(accept_job_msg, send_before=2)

    executor_ready_msg = V0ExecutorReadyRequest(job_uuid=job_request.uuid)
    await miner_transport.add_message(executor_ready_msg, send_before=1)

    job_finished_msg = V0JobFinishedRequest(
        job_uuid=job_request.uuid, docker_process_stdout="", docker_process_stderr=""
    )
    await miner_transport.add_message(job_finished_msg, send_before=1)

    # Job 2
    # Second transport will "connect" to the same "miner", but will have a clean state.
    miner_transport_2 = miner_transports[1]
    await faci_transport.add_message(another_job_request, send_before=2)

    await miner_transport_2.add_message(
        V0AcceptJobRequest(job_uuid=another_job_request.uuid),
        send_before=2,
    )

    await miner_transport_2.add_message(
        V0ExecutorReadyRequest(job_uuid=another_job_request.uuid),
        send_before=1,
    )

    await miner_transport_2.add_message(
        V0JobFinishedRequest(
            job_uuid=another_job_request.uuid, docker_process_stdout="", docker_process_stderr=""
        ),
        send_before=1,
    )

    # Expected messages: auth, job1 status=accepted, job1 status=finished, job2 status=accepted
    await execute_scenario(until=lambda: len(faci_transport.sent) >= 5)

    j1_accepted_msg = JobStatusUpdate.model_validate_json(faci_transport.sent[1])
    j1_finished_msg = JobStatusUpdate.model_validate_json(faci_transport.sent[2])
    j2_accepted_msg = JobStatusUpdate.model_validate_json(faci_transport.sent[3])
    j2_finished_msg = JobStatusUpdate.model_validate_json(faci_transport.sent[4])

    assert j1_accepted_msg.status == "accepted"
    assert j1_finished_msg.status == "completed"
    assert j2_accepted_msg.status == "accepted"
    assert j2_finished_msg.status == "completed"

    # Check that the same miner did both jobs
    assert miner.hotkey in j1_finished_msg.metadata.comment
    assert miner.hotkey in j2_finished_msg.metadata.comment
