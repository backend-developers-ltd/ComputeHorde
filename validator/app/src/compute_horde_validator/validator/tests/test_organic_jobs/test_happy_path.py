import pytest
from compute_horde.fv_protocol.validator_requests import JobStatusUpdate
from compute_horde.protocol_messages import (
    V0AcceptJobRequest,
    V0ExecutionDoneRequest,
    V0ExecutorReadyRequest,
    V0JobFinishedRequest,
    V0VolumesReadyRequest,
)

from compute_horde_validator.validator.models import Miner, OrganicJob

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.django_db(transaction=True),
    pytest.mark.override_config(
        DYNAMIC_EXECUTOR_RESERVATION_TIME_LIMIT=1,
        DYNAMIC_EXECUTOR_STARTUP_TIME_LIMIT=1,
    ),
]


async def test_basic_flow_works(job_request, faci_transport, miner_transport, execute_scenario):
    await faci_transport.add_message(job_request, send_before=0)
    # vali -> faci: received
    # vali -> miner: initial job request
    await miner_transport.add_message(V0AcceptJobRequest(job_uuid=job_request.uuid), send_before=1)
    # vali -> faci: accepted
    # vali -> miner: receipt for accepting the job
    await miner_transport.add_message(
        V0ExecutorReadyRequest(job_uuid=job_request.uuid), send_before=1
    )
    # vali -> faci: executor ready
    # vali -> miner: actual job request
    await miner_transport.add_message(
        V0VolumesReadyRequest(job_uuid=job_request.uuid), send_before=1
    )
    # vali -> faci: volumes ready
    await miner_transport.add_message(
        V0ExecutionDoneRequest(job_uuid=job_request.uuid), send_before=0
    )
    # vali -> faci: execution done
    await miner_transport.add_message(
        V0JobFinishedRequest(
            job_uuid=job_request.uuid, docker_process_stdout="", docker_process_stderr=""
        ),
        send_before=0,
    )
    # vali -> miner: receipt for finishing job
    # vali -> faci: completed

    await execute_scenario(until=lambda: len(faci_transport.sent) >= 7)

    assert JobStatusUpdate.model_validate_json(faci_transport.sent[1]).status == "received"
    assert JobStatusUpdate.model_validate_json(faci_transport.sent[2]).status == "accepted"
    assert JobStatusUpdate.model_validate_json(faci_transport.sent[3]).status == "executor_ready"
    assert JobStatusUpdate.model_validate_json(faci_transport.sent[4]).status == "volumes_ready"
    assert JobStatusUpdate.model_validate_json(faci_transport.sent[5]).status == "execution_done"
    assert JobStatusUpdate.model_validate_json(faci_transport.sent[6]).status == "completed"
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
    assert await Miner.objects.acount() == 2
    # 1 miner + self validator (Miner tables holds both miners and validators now, should be renamed)

    # Job 1
    await faci_transport.add_message(job_request, send_before=0)

    await miner_transport.add_message(V0AcceptJobRequest(job_uuid=job_request.uuid), send_before=2)
    await miner_transport.add_message(
        V0ExecutorReadyRequest(job_uuid=job_request.uuid), send_before=1
    )
    await miner_transport.add_message(
        V0VolumesReadyRequest(job_uuid=job_request.uuid), send_before=1
    )
    await miner_transport.add_message(
        V0ExecutionDoneRequest(job_uuid=job_request.uuid), send_before=0
    )
    await miner_transport.add_message(
        V0JobFinishedRequest(
            job_uuid=job_request.uuid, docker_process_stdout="", docker_process_stderr=""
        ),
        send_before=0,
    )

    # Job 2
    # Second transport will "connect" to the same "miner", but will have a clean state.
    miner_transport_2 = miner_transports[1]
    await faci_transport.add_message(another_job_request, send_before=2)

    await miner_transport_2.add_message(
        V0AcceptJobRequest(job_uuid=another_job_request.uuid), send_before=2
    )
    await miner_transport_2.add_message(
        V0ExecutorReadyRequest(job_uuid=another_job_request.uuid), send_before=1
    )
    await miner_transport_2.add_message(
        V0VolumesReadyRequest(job_uuid=another_job_request.uuid), send_before=1
    )
    await miner_transport_2.add_message(
        V0ExecutionDoneRequest(job_uuid=another_job_request.uuid), send_before=0
    )
    await miner_transport_2.add_message(
        V0JobFinishedRequest(
            job_uuid=another_job_request.uuid, docker_process_stdout="", docker_process_stderr=""
        ),
        send_before=0,
    )

    # Expected messages: auth, job1 status=accepted, job1 status=finished, job2 status=accepted
    await execute_scenario(until=lambda: len(faci_transport.sent) >= 13, timeout_seconds=3)

    j1_accepted_msg = JobStatusUpdate.model_validate_json(faci_transport.sent[2])
    j1_finished_msg = JobStatusUpdate.model_validate_json(faci_transport.sent[6])
    j2_accepted_msg = JobStatusUpdate.model_validate_json(faci_transport.sent[8])
    j2_finished_msg = JobStatusUpdate.model_validate_json(faci_transport.sent[12])

    assert j1_accepted_msg.status == "accepted"
    assert j1_finished_msg.status == "completed"
    assert j2_accepted_msg.status == "accepted"
    assert j2_finished_msg.status == "completed"

    # Check that the same miner did both jobs
    assert miner.hotkey in j1_finished_msg.metadata.comment
    assert miner.hotkey in j2_finished_msg.metadata.comment
