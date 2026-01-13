import uuid
from collections.abc import Callable
from functools import partial
from unittest import mock

import pytest
from compute_horde.executor_class import DEFAULT_EXECUTOR_CLASS
from compute_horde.fv_protocol.facilitator_requests import OrganicJobRequest
from compute_horde.fv_protocol.validator_requests import JobStatusUpdate
from compute_horde.protocol_consts import HordeFailureReason, JobParticipantType, JobStatus
from compute_horde.protocol_messages import (
    GenericError,
    MinerToValidatorMessage,
    V0AcceptJobRequest,
    V0DeclineJobRequest,
    V0ExecutionDoneRequest,
    V0ExecutorManifestRequest,
    V0ExecutorReadyRequest,
    V0HordeFailedRequest,
    V0JobAcceptedReceiptRequest,
    V0JobFailedRequest,
    V0JobFinishedReceiptRequest,
    V0JobFinishedRequest,
    V0VolumesReadyRequest,
    ValidatorToMinerMessage,
)

from compute_horde_validator.validator.models import Miner
from compute_horde_validator.validator.organic_jobs.facilitator_client import OrganicJob
from compute_horde_validator.validator.organic_jobs.miner_driver_sync import (
    MinerClient,
    SyncOrganicJobDriver,
)

from ..helpers import get_dummy_job_request_v2


class MockSyncMinerClient(MinerClient):
    """Mock sync MinerClient that returns pre-configured messages."""

    def __init__(
        self,
        *args,
        messages_to_return: list[MinerToValidatorMessage],
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self._messages_to_return = list(messages_to_return)
        self._sent_models: list[ValidatorToMinerMessage] = []
        self._recv_index = 0

    def connect(self) -> None:
        # Mock connection - set ws to a truthy value so send works
        self.ws = mock.MagicMock()  # type: ignore[assignment]

    def close(self) -> None:
        # Mock close - do nothing
        pass

    def send(self, msg: ValidatorToMinerMessage) -> None:
        self._sent_models.append(msg)

    def recv(self, timeout: float) -> MinerToValidatorMessage:
        if self._recv_index >= len(self._messages_to_return):
            raise TimeoutError("No more messages to return")
        msg = self._messages_to_return[self._recv_index]
        self._recv_index += 1
        return msg

    def query_sent_models(
        self,
        condition: Callable[[ValidatorToMinerMessage], bool] | None = None,
        model_class: type | None = None,
    ) -> list[ValidatorToMinerMessage]:
        result = []
        for model in self._sent_models:
            if model_class is not None and not isinstance(model, model_class):
                continue
            if condition is not None and not condition(model):
                continue
            result.append(model)
        return result


def create_job_request(job_uuid: str) -> OrganicJobRequest:
    """Convert V2JobRequest to OrganicJobRequest for the sync driver."""
    v2_request = get_dummy_job_request_v2(job_uuid)
    return OrganicJobRequest(
        type="job.new",
        uuid=job_uuid,
        docker_image=v2_request.docker_image,
        executor_class=v2_request.executor_class,
        args=v2_request.args,
        env=v2_request.env,
        volume=v2_request.volume,
        output_upload=v2_request.output_upload,
        download_time_limit=v2_request.download_time_limit,
        execution_time_limit=v2_request.execution_time_limit,
        streaming_start_time_limit=v2_request.streaming_start_time_limit,
        upload_time_limit=v2_request.upload_time_limit,
    )


@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
@pytest.mark.parametrize(
    (
        "messages_from_miner",
        "expected_job_status_updates",
        "organic_job_status",
        "expected_job_accepted_receipt",
        "expected_job_finished_receipt",
    ),
    [
        # Timeout - no messages from miner
        (
            [],
            [JobStatus.FAILED],
            OrganicJob.Status.FAILED,
            False,
            True,  # Failure receipt is sent
        ),
        # Miner declines job
        (
            [V0DeclineJobRequest],
            [JobStatus.REJECTED],
            OrganicJob.Status.FAILED,
            False,
            True,  # Failure receipt is sent
        ),
        # Miner accepts but executor fails
        (
            [
                V0AcceptJobRequest,
                partial(
                    V0HordeFailedRequest,
                    reported_by=JobParticipantType.MINER,
                    reason=HordeFailureReason.GENERIC_ERROR,
                    message="Executor failed",
                ),
            ],
            [JobStatus.ACCEPTED, JobStatus.HORDE_FAILED],
            OrganicJob.Status.FAILED,
            True,
            True,  # Failure receipt is sent
        ),
        # Miner accepts, executor ready, then timeout
        (
            [
                V0AcceptJobRequest,
                V0ExecutorReadyRequest,
            ],
            [JobStatus.ACCEPTED, JobStatus.EXECUTOR_READY, JobStatus.FAILED],
            OrganicJob.Status.FAILED,
            True,
            True,  # Failure receipt is sent
        ),
        # Full flow but job fails at execution
        (
            [
                V0AcceptJobRequest,
                V0ExecutorReadyRequest,
                V0VolumesReadyRequest,
                V0ExecutionDoneRequest,
                V0JobFailedRequest,
            ],
            [
                JobStatus.ACCEPTED,
                JobStatus.EXECUTOR_READY,
                JobStatus.VOLUMES_READY,
                JobStatus.EXECUTION_DONE,
                JobStatus.FAILED,
            ],
            OrganicJob.Status.FAILED,
            True,
            True,  # Failure receipt is sent
        ),
        # Full successful flow
        (
            [
                V0AcceptJobRequest,
                V0ExecutorReadyRequest,
                V0VolumesReadyRequest,
                V0ExecutionDoneRequest,
                partial(
                    V0JobFinishedRequest,
                    docker_process_stdout="mocked stdout",
                    docker_process_stderr="mocked stderr",
                    artifacts={},
                ),
            ],
            [
                JobStatus.ACCEPTED,
                JobStatus.EXECUTOR_READY,
                JobStatus.VOLUMES_READY,
                JobStatus.EXECUTION_DONE,
                JobStatus.COMPLETED,
            ],
            OrganicJob.Status.COMPLETED,
            True,
            True,  # Success receipt is sent
        ),
    ],
)
def test_sync_miner_driver(
    messages_from_miner,
    expected_job_status_updates,
    organic_job_status,
    expected_job_accepted_receipt,
    expected_job_finished_receipt,
    settings,
):
    miner, _ = Miner.objects.get_or_create(hotkey="miner_client")
    job_uuid = str(uuid.uuid4())
    job_request = create_job_request(job_uuid)
    job = OrganicJob.objects.create(
        job_uuid=job_uuid,
        miner=miner,
        miner_address="irrelevant",
        miner_address_ip_version=4,
        miner_port=9999,
        executor_class=DEFAULT_EXECUTOR_CLASS,
        job_description="User job from facilitator",
        block=42,
    )

    # Build messages with job_uuid
    messages = []
    for msg_factory in messages_from_miner:
        if callable(msg_factory) and not isinstance(msg_factory, type):
            # It's a partial or other callable
            messages.append(msg_factory(job_uuid=job_uuid))
        else:
            # It's a class
            messages.append(msg_factory(job_uuid=job_uuid))

    job_status_updates: list[JobStatusUpdate] = []

    def track_job_status_updates(x: JobStatusUpdate):
        job_status_updates.append(x)

    my_keypair = settings.BITTENSOR_WALLET().hotkey

    miner_client = MockSyncMinerClient(
        url="ws://miner:9999/v0.1/validator_interface/test",
        miner_hotkey="miner_hotkey",
        validator_keypair=my_keypair,
        messages_to_return=messages,
    )

    with mock.patch(
        "compute_horde_validator.validator.organic_jobs.miner_driver_sync.supertensor"
    ) as mock_supertensor:
        mock_supertensor.return_value.list_validators.return_value = []

        driver = SyncOrganicJobDriver(
            miner_client,
            job,
            job_request,
            miner_hotkey="miner_hotkey",
            my_keypair=my_keypair,
            allowed_leeway=5,
            reservation_time_limit=10,
            executor_startup_time_limit=10,
            max_overall_time_limit=60,
            status_callback=track_job_status_updates,
        )
        driver.run()

    assert len(job_status_updates) == len(expected_job_status_updates), (
        f"Got {[u.status.value for u in job_status_updates]}, expected {[s.value for s in expected_job_status_updates]}"
    )
    for job_status, expected_status in zip(job_status_updates, expected_job_status_updates):
        assert job_status.status == expected_status
        assert job_status.uuid == job_uuid

    job.refresh_from_db()
    assert job.status == organic_job_status

    if organic_job_status == OrganicJob.Status.COMPLETED:
        assert job.stdout == "mocked stdout"
        assert job.stderr == "mocked stderr"

    if expected_job_accepted_receipt:
        assert miner_client.query_sent_models(model_class=V0JobAcceptedReceiptRequest)

    if expected_job_finished_receipt:
        assert miner_client.query_sent_models(model_class=V0JobFinishedReceiptRequest)


@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
def test_sync_miner_driver_connection_failure(settings):
    """Test that connection failures are handled properly."""
    miner, _ = Miner.objects.get_or_create(hotkey="miner_client")
    job_uuid = str(uuid.uuid4())
    job_request = create_job_request(job_uuid)
    job = OrganicJob.objects.create(
        job_uuid=job_uuid,
        miner=miner,
        miner_address="irrelevant",
        miner_address_ip_version=4,
        miner_port=9999,
        executor_class=DEFAULT_EXECUTOR_CLASS,
        job_description="User job from facilitator",
        block=42,
    )

    my_keypair = settings.BITTENSOR_WALLET().hotkey

    miner_client = MockSyncMinerClient(
        url="ws://miner:9999/v0.1/validator_interface/test",
        miner_hotkey="miner_hotkey",
        validator_keypair=my_keypair,
        messages_to_return=[],
    )

    # Make connect raise an exception
    def failing_connect():
        raise ConnectionRefusedError("Connection refused")

    miner_client.connect = failing_connect

    job_status_updates: list[JobStatusUpdate] = []

    def track_job_status_updates(x: JobStatusUpdate):
        job_status_updates.append(x)

    driver = SyncOrganicJobDriver(
        miner_client,
        job,
        job_request,
        miner_hotkey="miner_hotkey",
        my_keypair=my_keypair,
        allowed_leeway=5,
        reservation_time_limit=10,
        executor_startup_time_limit=10,
        max_overall_time_limit=60,
        status_callback=track_job_status_updates,
    )
    driver.run()

    assert len(job_status_updates) == 1
    assert job_status_updates[0].status == JobStatus.FAILED

    job.refresh_from_db()
    assert job.status == OrganicJob.Status.FAILED
    assert "connection failed" in job.comment.lower()


@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
def test_sync_miner_driver_ignores_messages_for_different_job(settings):
    """Messages with a different job_uuid should be ignored."""
    miner, _ = Miner.objects.get_or_create(hotkey="miner_client")
    job_uuid = str(uuid.uuid4())
    other_job_uuid = str(uuid.uuid4())
    job_request = create_job_request(job_uuid)
    job = OrganicJob.objects.create(
        job_uuid=job_uuid,
        miner=miner,
        miner_address="irrelevant",
        miner_address_ip_version=4,
        miner_port=9999,
        executor_class=DEFAULT_EXECUTOR_CLASS,
        job_description="User job from facilitator",
        block=42,
    )

    messages = [
        # Decline for a different job - should be ignored (if processed, would fail our job)
        V0DeclineJobRequest(job_uuid=other_job_uuid),
        # Message for our job - should be processed
        V0AcceptJobRequest(job_uuid=job_uuid),
        V0ExecutorReadyRequest(job_uuid=job_uuid),
        V0VolumesReadyRequest(job_uuid=job_uuid),
        V0ExecutionDoneRequest(job_uuid=job_uuid),
        V0JobFinishedRequest(
            job_uuid=job_uuid,
            docker_process_stdout="stdout",
            docker_process_stderr="stderr",
            artifacts={},
        ),
    ]

    job_status_updates: list[JobStatusUpdate] = []
    my_keypair = settings.BITTENSOR_WALLET().hotkey

    miner_client = MockSyncMinerClient(
        url="ws://miner:9999/v0.1/validator_interface/test",
        miner_hotkey="miner_hotkey",
        validator_keypair=my_keypair,
        messages_to_return=messages,
    )

    driver = SyncOrganicJobDriver(
        miner_client,
        job,
        job_request,
        miner_hotkey="miner_hotkey",
        my_keypair=my_keypair,
        allowed_leeway=5,
        reservation_time_limit=10,
        executor_startup_time_limit=10,
        max_overall_time_limit=60,
        status_callback=job_status_updates.append,
    )
    driver.run()

    job.refresh_from_db()
    assert job.status == OrganicJob.Status.COMPLETED
    assert [u.status for u in job_status_updates] == [
        JobStatus.ACCEPTED,
        JobStatus.EXECUTOR_READY,
        JobStatus.VOLUMES_READY,
        JobStatus.EXECUTION_DONE,
        JobStatus.COMPLETED,
    ]


@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
def test_sync_miner_driver_ignores_manifest_requests(settings):
    """V0ExecutorManifestRequest messages should be ignored during job flow."""
    miner, _ = Miner.objects.get_or_create(hotkey="miner_client")
    job_uuid = str(uuid.uuid4())
    job_request = create_job_request(job_uuid)
    job = OrganicJob.objects.create(
        job_uuid=job_uuid,
        miner=miner,
        miner_address="irrelevant",
        miner_address_ip_version=4,
        miner_port=9999,
        executor_class=DEFAULT_EXECUTOR_CLASS,
        job_description="User job from facilitator",
        block=42,
    )

    # TODO: maybe disallow multiple manifest requests to prevent DOS condition?
    messages = [
        # Manifest requests interspersed - should all be ignored
        V0ExecutorManifestRequest(manifest={}),
        V0AcceptJobRequest(job_uuid=job_uuid),
        V0ExecutorManifestRequest(manifest={}),
        V0ExecutorReadyRequest(job_uuid=job_uuid),
        V0ExecutorManifestRequest(manifest={}),
        V0VolumesReadyRequest(job_uuid=job_uuid),
        V0ExecutionDoneRequest(job_uuid=job_uuid),
        V0JobFinishedRequest(
            job_uuid=job_uuid,
            docker_process_stdout="stdout",
            docker_process_stderr="stderr",
            artifacts={},
        ),
    ]

    job_status_updates: list[JobStatusUpdate] = []
    my_keypair = settings.BITTENSOR_WALLET().hotkey

    miner_client = MockSyncMinerClient(
        url="ws://miner:9999/v0.1/validator_interface/test",
        miner_hotkey="miner_hotkey",
        validator_keypair=my_keypair,
        messages_to_return=messages,
    )

    driver = SyncOrganicJobDriver(
        miner_client,
        job,
        job_request,
        miner_hotkey="miner_hotkey",
        my_keypair=my_keypair,
        allowed_leeway=5,
        reservation_time_limit=10,
        executor_startup_time_limit=10,
        max_overall_time_limit=60,
        status_callback=job_status_updates.append,
    )
    driver.run()

    job.refresh_from_db()
    assert job.status == OrganicJob.Status.COMPLETED


@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
def test_sync_miner_driver_out_of_order_executor_ready_request(settings):
    """V0ExecutorReadyRequest should be ignored while waiting for V0AcceptJobRequest."""
    miner, _ = Miner.objects.get_or_create(hotkey="miner_client")
    job_uuid = str(uuid.uuid4())
    job_request = create_job_request(job_uuid)
    job = OrganicJob.objects.create(
        job_uuid=job_uuid,
        miner=miner,
        miner_address="irrelevant",
        miner_address_ip_version=4,
        miner_port=9999,
        executor_class=DEFAULT_EXECUTOR_CLASS,
        job_description="User job from facilitator",
        block=42,
    )

    messages = [
        # Out of order - should be ignored while waiting for V0AcceptJobRequest
        V0ExecutorReadyRequest(job_uuid=job_uuid),
        V0AcceptJobRequest(job_uuid=job_uuid),
        # Rest of happy path - should be ignored while waiting for V0ExecutorReadyRequest
        V0VolumesReadyRequest(job_uuid=job_uuid),
        V0ExecutionDoneRequest(job_uuid=job_uuid),
        V0JobFinishedRequest(
            job_uuid=job_uuid,
            docker_process_stdout="stdout",
            docker_process_stderr="stderr",
            artifacts={},
        ),
    ]

    job_status_updates: list[JobStatusUpdate] = []
    my_keypair = settings.BITTENSOR_WALLET().hotkey

    miner_client = MockSyncMinerClient(
        url="ws://miner:9999/v0.1/validator_interface/test",
        miner_hotkey="miner_hotkey",
        validator_keypair=my_keypair,
        messages_to_return=messages,
    )

    driver = SyncOrganicJobDriver(
        miner_client,
        job,
        job_request,
        miner_hotkey="miner_hotkey",
        my_keypair=my_keypair,
        allowed_leeway=1,
        reservation_time_limit=1,
        executor_startup_time_limit=1,
        max_overall_time_limit=5,
        status_callback=job_status_updates.append,
    )
    driver.run()

    job.refresh_from_db()
    assert job.status == OrganicJob.Status.FAILED
    assert "executor_readiness_response_timed_out" in job.comment


@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
def test_sync_miner_driver_ignores_job_finished_while_waiting_for_execution_done(settings):
    """V0JobFinishedRequest should be ignored while waiting for V0ExecutionDoneRequest."""
    miner, _ = Miner.objects.get_or_create(hotkey="miner_client")
    job_uuid = str(uuid.uuid4())
    job_request = create_job_request(job_uuid)
    job = OrganicJob.objects.create(
        job_uuid=job_uuid,
        miner=miner,
        miner_address="irrelevant",
        miner_address_ip_version=4,
        miner_port=9999,
        executor_class=DEFAULT_EXECUTOR_CLASS,
        job_description="User job from facilitator",
        block=42,
    )

    messages = [
        V0AcceptJobRequest(job_uuid=job_uuid),
        V0ExecutorReadyRequest(job_uuid=job_uuid),
        V0VolumesReadyRequest(job_uuid=job_uuid),
        # Out of order - should be ignored while waiting for V0ExecutionDoneRequest
        V0JobFinishedRequest(
            job_uuid=job_uuid,
            docker_process_stdout="should be ignored",
            docker_process_stderr="",
            artifacts={},
        ),
        # Rest of happy path (never reached due to timeout waiting for V0ExecutionDoneRequest)
        V0ExecutionDoneRequest(job_uuid=job_uuid),
        V0JobFinishedRequest(
            job_uuid=job_uuid,
            docker_process_stdout="stdout",
            docker_process_stderr="stderr",
            artifacts={},
        ),
    ]

    job_status_updates: list[JobStatusUpdate] = []
    my_keypair = settings.BITTENSOR_WALLET().hotkey

    miner_client = MockSyncMinerClient(
        url="ws://miner:9999/v0.1/validator_interface/test",
        miner_hotkey="miner_hotkey",
        validator_keypair=my_keypair,
        messages_to_return=messages,
    )

    driver = SyncOrganicJobDriver(
        miner_client,
        job,
        job_request,
        miner_hotkey="miner_hotkey",
        my_keypair=my_keypair,
        allowed_leeway=1,
        reservation_time_limit=1,
        executor_startup_time_limit=1,
        max_overall_time_limit=5,
        status_callback=job_status_updates.append,
    )
    driver.run()

    job.refresh_from_db()
    assert job.status == OrganicJob.Status.COMPLETED
    assert job.stdout == "stdout"  # check the correct stdout was received
    assert [u.status for u in job_status_updates] == [
        JobStatus.ACCEPTED,
        JobStatus.EXECUTOR_READY,
        JobStatus.VOLUMES_READY,
        JobStatus.EXECUTION_DONE,
        JobStatus.COMPLETED,
    ]


@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
def test_sync_miner_driver_continues_after_non_fatal_generic_error(settings):
    """Non-fatal GenericError messages should be logged but not fail the job."""
    miner, _ = Miner.objects.get_or_create(hotkey="miner_client")
    job_uuid = str(uuid.uuid4())
    job_request = create_job_request(job_uuid)
    job = OrganicJob.objects.create(
        job_uuid=job_uuid,
        miner=miner,
        miner_address="irrelevant",
        miner_address_ip_version=4,
        miner_port=9999,
        executor_class=DEFAULT_EXECUTOR_CLASS,
        job_description="User job from facilitator",
        block=42,
    )

    messages = [
        V0AcceptJobRequest(job_uuid=job_uuid),
        # Non-fatal generic error - should be logged and ignored
        GenericError(details="Some transient error"),
        V0ExecutorReadyRequest(job_uuid=job_uuid),
        V0VolumesReadyRequest(job_uuid=job_uuid),
        V0ExecutionDoneRequest(job_uuid=job_uuid),
        V0JobFinishedRequest(
            job_uuid=job_uuid,
            docker_process_stdout="stdout",
            docker_process_stderr="stderr",
            artifacts={},
        ),
    ]

    job_status_updates: list[JobStatusUpdate] = []
    my_keypair = settings.BITTENSOR_WALLET().hotkey

    miner_client = MockSyncMinerClient(
        url="ws://miner:9999/v0.1/validator_interface/test",
        miner_hotkey="miner_hotkey",
        validator_keypair=my_keypair,
        messages_to_return=messages,
    )

    driver = SyncOrganicJobDriver(
        miner_client,
        job,
        job_request,
        miner_hotkey="miner_hotkey",
        my_keypair=my_keypair,
        allowed_leeway=5,
        reservation_time_limit=10,
        executor_startup_time_limit=10,
        max_overall_time_limit=60,
        status_callback=job_status_updates.append,
    )
    driver.run()

    job.refresh_from_db()
    assert job.status == OrganicJob.Status.COMPLETED


@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
def test_sync_miner_driver_fails_on_fatal_generic_error(settings):
    """GenericError with 'Unknown validator' or similar should fail the job."""
    miner, _ = Miner.objects.get_or_create(hotkey="miner_client")
    job_uuid = str(uuid.uuid4())
    job_request = create_job_request(job_uuid)
    job = OrganicJob.objects.create(
        job_uuid=job_uuid,
        miner=miner,
        miner_address="irrelevant",
        miner_address_ip_version=4,
        miner_port=9999,
        executor_class=DEFAULT_EXECUTOR_CLASS,
        job_description="User job from facilitator",
        block=42,
    )

    messages = [
        V0AcceptJobRequest(job_uuid=job_uuid),
        # Fatal generic error - should fail the job
        GenericError(details="Unknown validator: some_hotkey"),
    ]

    job_status_updates: list[JobStatusUpdate] = []
    my_keypair = settings.BITTENSOR_WALLET().hotkey

    miner_client = MockSyncMinerClient(
        url="ws://miner:9999/v0.1/validator_interface/test",
        miner_hotkey="miner_hotkey",
        validator_keypair=my_keypair,
        messages_to_return=messages,
    )

    driver = SyncOrganicJobDriver(
        miner_client,
        job,
        job_request,
        miner_hotkey="miner_hotkey",
        my_keypair=my_keypair,
        allowed_leeway=5,
        reservation_time_limit=10,
        executor_startup_time_limit=10,
        max_overall_time_limit=60,
        status_callback=job_status_updates.append,
    )
    driver.run()

    job.refresh_from_db()
    assert job.status == OrganicJob.Status.FAILED
    assert [u.status for u in job_status_updates] == [JobStatus.ACCEPTED, JobStatus.FAILED]
