import uuid

import pytest
from compute_horde.executor_class import DEFAULT_EXECUTOR_CLASS
from compute_horde.protocol_messages import (
    V0AcceptJobRequest,
    V0DeclineJobRequest,
    V0ExecutorFailedRequest,
    V0ExecutorReadyRequest,
    V0JobAcceptedReceiptRequest,
    V0JobFailedRequest,
    V0JobFinishedReceiptRequest,
    V0JobFinishedRequest,
)

from compute_horde_validator.validator.models import Miner
from compute_horde_validator.validator.organic_jobs.facilitator_client import OrganicJob
from compute_horde_validator.validator.organic_jobs.miner_driver import drive_organic_job

from .helpers import (
    MockMinerClient,
    get_dummy_job_request_v2,
    get_miner_client,
)

WEBSOCKET_TIMEOUT = 10


@pytest.mark.asyncio
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
@pytest.mark.parametrize(
    (
        "futures_result",
        "expected_job_status_updates",
        "organic_job_status",
        "dummy_job_factory",
        "expected_job_accepted_receipt",
        "expected_job_finished_receipt",
    ),
    [
        (
            (None, None, None),
            ["failed"],
            OrganicJob.Status.FAILED,
            get_dummy_job_request_v2,
            False,
            False,
        ),
        (
            (V0DeclineJobRequest, None, None),
            ["rejected"],
            OrganicJob.Status.FAILED,
            get_dummy_job_request_v2,
            False,
            False,
        ),
        (
            (V0AcceptJobRequest, V0ExecutorFailedRequest, None),
            ["accepted", "failed"],
            OrganicJob.Status.FAILED,
            get_dummy_job_request_v2,
            True,
            False,
        ),
        (
            (V0AcceptJobRequest, V0ExecutorReadyRequest, None),
            ["accepted", "failed"],
            OrganicJob.Status.FAILED,
            get_dummy_job_request_v2,
            True,
            False,
        ),
        (
            (V0AcceptJobRequest, V0ExecutorReadyRequest, V0JobFailedRequest),
            ["accepted", "failed"],
            OrganicJob.Status.FAILED,
            get_dummy_job_request_v2,
            True,
            False,
        ),
        (
            (V0AcceptJobRequest, V0ExecutorReadyRequest, V0JobFinishedRequest),
            ["accepted", "completed"],
            OrganicJob.Status.COMPLETED,
            get_dummy_job_request_v2,
            True,
            True,
        ),
        (
            (V0AcceptJobRequest, V0ExecutorReadyRequest, V0JobFinishedRequest),
            ["accepted", "completed"],
            OrganicJob.Status.COMPLETED,
            get_dummy_job_request_v2,
            True,
            True,
        ),
    ],
)
async def test_miner_driver(
    futures_result,
    expected_job_status_updates,
    organic_job_status,
    dummy_job_factory,
    expected_job_accepted_receipt,
    expected_job_finished_receipt,
):
    miner, _ = await Miner.objects.aget_or_create(hotkey="miner_client")
    job_uuid = str(uuid.uuid4())
    job_request = dummy_job_factory(job_uuid)

    job = await OrganicJob.objects.acreate(
        job_uuid=job_uuid,
        miner=miner,
        miner_address="irrelevant",
        miner_address_ip_version=4,
        miner_port=9999,
        executor_class=DEFAULT_EXECUTOR_CLASS,
        job_description="User job from facilitator",
        block=42,
    )
    miner_client = get_miner_client(MockMinerClient, job_uuid)
    f0, f1, f2 = futures_result
    if f0:
        miner_client.miner_accepting_or_declining_future.set_result(f0(job_uuid=job_uuid))
    if f1:
        miner_client.executor_ready_or_failed_future.set_result(f1(job_uuid=job_uuid))
    if f2:
        miner_client.miner_finished_or_failed_future.set_result(
            f2(
                job_uuid=job_uuid,
                docker_process_stdout="mocked stdout",
                docker_process_stderr="mocked stderr",
                artifacts={},
            )
        )

    job_status_updates = []

    async def track_job_status_updates(x):
        job_status_updates.append(x)

    await drive_organic_job(
        miner_client,
        job,
        job_request,
        total_job_timeout=1,
        executor_ready_timeout=1,
        notify_callback=track_job_status_updates,
    )

    assert len(job_status_updates) == len(expected_job_status_updates)
    for job_status, expected_status in zip(job_status_updates, expected_job_status_updates):
        assert job_status.status == expected_status
        assert job_status.uuid == job_uuid

    job = await OrganicJob.objects.aget(job_uuid=job_uuid)
    assert job.status == organic_job_status
    if organic_job_status == OrganicJob.Status.COMPLETED:
        assert job.stdout == "mocked stdout"
        assert job.stderr == "mocked stderr"

    def condition(_):
        return True

    if expected_job_accepted_receipt:
        assert miner_client._query_sent_models(condition, V0JobAcceptedReceiptRequest)
    if expected_job_finished_receipt:
        assert miner_client._query_sent_models(condition, V0JobFinishedReceiptRequest)
