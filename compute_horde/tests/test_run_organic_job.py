import datetime

import pytest
from compute_horde_core.executor_class import ExecutorClass
from compute_horde_core.streaming import StreamingDetails
from freezegun import freeze_time
from pydantic import TypeAdapter

from compute_horde.executor_class import EXECUTOR_CLASS, ExecutorClassSpec
from compute_horde.miner_client.organic import (
    MinerConnectionFailed,
    MinerRejectedJob,
    MinerReportedJobFailed,
    MinerTimedOut,
    OrganicJobDetails,
    OrganicMinerClient,
    execute_organic_job_on_miner,
)
from compute_horde.protocol_consts import HordeFailureReason, JobFailureReason, JobRejectionReason
from compute_horde.protocol_messages import (
    V0AcceptJobRequest,
    V0DeclineJobRequest,
    V0ExecutionDoneRequest,
    V0ExecutorReadyRequest,
    V0InitialJobRequest,
    V0JobAcceptedReceiptRequest,
    V0JobFailedRequest,
    V0JobFinishedReceiptRequest,
    V0JobFinishedRequest,
    V0JobRequest,
    V0StreamingJobReadyRequest,
    V0VolumesReadyRequest,
    ValidatorAuthForMiner,
    ValidatorToMinerMessage,
)
from compute_horde.receipts.schemas import MAX_TTL_SECONDS, JobAcceptedReceiptPayload
from compute_horde.transport import StubTransport
from compute_horde.transport.base import TransportConnectionError

JOB_UUID = "b4793a02-33a2-4a49-b4e2-4a7b903847e7"


class MinerStubTransport(StubTransport):
    def __init__(self, name: str, messages: list[str], *args, **kwargs):
        super().__init__(name, messages, *args, **kwargs)
        self.sent_models = []

    async def send(self, message):
        await super().send(message)
        self.sent_models.append(TypeAdapter(ValidatorToMinerMessage).validate_json(message))


class FailingTransport(StubTransport):
    def __init__(self, name: str, exception_to_raise: Exception, *args, **kwargs):
        super().__init__(name, [], *args, **kwargs)
        self.exception_to_raise = exception_to_raise

    async def start(self):
        raise self.exception_to_raise


@pytest.fixture
def successful_job_flow() -> list[str]:
    return [
        V0AcceptJobRequest(job_uuid=JOB_UUID).model_dump_json(),
        V0ExecutorReadyRequest(job_uuid=JOB_UUID).model_dump_json(),
        V0VolumesReadyRequest(job_uuid=JOB_UUID).model_dump_json(),
        V0ExecutionDoneRequest(job_uuid=JOB_UUID).model_dump_json(),
        V0JobFinishedRequest(
            job_uuid=JOB_UUID,
            docker_process_stdout="stdout",
            docker_process_stderr="stderr",
            artifacts={},
        ).model_dump_json(),
    ]


@pytest.mark.asyncio
@pytest.mark.django_db(transaction=True)
async def test_run_organic_job__success(keypair, successful_job_flow):
    mock_transport = MinerStubTransport("mock", successful_job_flow)
    client = OrganicMinerClient(
        miner_hotkey="mock",
        miner_address="0.0.0.0",
        miner_port=1234,
        job_uuid=JOB_UUID,
        my_keypair=keypair,
        transport=mock_transport,
    )
    job_details = OrganicJobDetails(
        job_uuid=JOB_UUID,
        executor_class=ExecutorClass.always_on__llm__a6000,
        docker_image="mock",
    )
    stdout, stderr, artifacts, upload_results = await execute_organic_job_on_miner(
        client, job_details, reservation_time_limit=2, executor_startup_time_limit=2
    )

    assert stdout == "stdout"
    assert stderr == "stderr"

    sent_models_types = [type(model) for model in mock_transport.sent_models]
    assert sent_models_types == [
        ValidatorAuthForMiner,
        V0InitialJobRequest,
        V0JobAcceptedReceiptRequest,
        V0JobRequest,
        V0JobFinishedReceiptRequest,
    ]
    _, initial_job_request, _, job_request, _ = mock_transport.sent_models
    assert initial_job_request.is_signed_by(keypair.ss58_address)
    assert job_request == V0JobRequest(
        job_uuid=JOB_UUID,
        executor_class=ExecutorClass.always_on__llm__a6000,
        docker_image="mock",
        docker_run_options_preset="nvidia_all",
        docker_run_cmd=[],
        env={},
        volume=None,
        output_upload=None,
        artifacts_dir=None,
        validator_signature=job_request.validator_signature,
    )
    assert job_request.is_signed_by(keypair.ss58_address)


@pytest.mark.asyncio
@pytest.mark.django_db(transaction=True)
async def test_run_organic_job__clamps_job_accepted_receipt_ttl(keypair, successful_job_flow):
    accepted_at = datetime.datetime(2025, 3, 4, 12, 0, tzinfo=datetime.UTC)
    mock_transport = MinerStubTransport("mock", successful_job_flow)
    client = OrganicMinerClient(
        miner_hotkey="mock",
        miner_address="0.0.0.0",
        miner_port=1234,
        job_uuid=JOB_UUID,
        my_keypair=keypair,
        transport=mock_transport,
    )
    job_details = OrganicJobDetails(
        job_uuid=JOB_UUID,
        executor_class=ExecutorClass.always_on__llm__a6000,
        docker_image="mock",
        job_timing=OrganicJobDetails.TimingDetails(
            allowed_leeway=5,
            download_time_limit=5,
            execution_time_limit=MAX_TTL_SECONDS,
            upload_time_limit=5,
            streaming_start_time_limit=0,
        ),
    )
    with freeze_time(accepted_at, real_asyncio=True):
        await execute_organic_job_on_miner(
            client, job_details, reservation_time_limit=2, executor_startup_time_limit=2
        )

    [receipt] = [
        model
        for model in mock_transport.sent_models
        if isinstance(model, V0JobAcceptedReceiptRequest)
    ]
    assert receipt.payload == JobAcceptedReceiptPayload(
        job_uuid=JOB_UUID,
        miner_hotkey="mock",
        validator_hotkey=keypair.ss58_address,
        timestamp=accepted_at,
        time_accepted=accepted_at,
        ttl=MAX_TTL_SECONDS,
    )


@pytest.mark.asyncio
@pytest.mark.django_db(transaction=True)
async def test_run_organic_job_streaming__success(keypair):
    mock_transport = MinerStubTransport(
        "mock",
        [
            V0AcceptJobRequest(job_uuid=JOB_UUID).model_dump_json(),
            V0ExecutorReadyRequest(job_uuid=JOB_UUID).model_dump_json(),
            V0VolumesReadyRequest(job_uuid=JOB_UUID).model_dump_json(),
            V0StreamingJobReadyRequest(
                job_uuid=JOB_UUID,
                public_key="dummy-cert",
                port=12345,
            ).model_dump_json(),
            V0ExecutionDoneRequest(job_uuid=JOB_UUID).model_dump_json(),
            V0JobFinishedRequest(
                job_uuid=JOB_UUID,
                docker_process_stdout="streaming-stdout",
                docker_process_stderr="streaming-stderr",
                artifacts={},
            ).model_dump_json(),
        ],
    )
    client = OrganicMinerClient(
        miner_hotkey="mock",
        miner_address="0.0.0.0",
        miner_port=1234,
        job_uuid=JOB_UUID,
        my_keypair=keypair,
        transport=mock_transport,
    )
    job_details = OrganicJobDetails(
        job_uuid=JOB_UUID,
        executor_class=ExecutorClass.always_on__llm__a6000,
        docker_image="mock",
        streaming_details=StreamingDetails(public_key="dummy-cert"),
    )
    await execute_organic_job_on_miner(
        client, job_details, reservation_time_limit=2, executor_startup_time_limit=2
    )

    # Find the initial job request sent by the client
    initial_job_request = next(
        (m for m in mock_transport.sent_models if isinstance(m, V0InitialJobRequest)), None
    )
    assert initial_job_request is not None, "Initial job request was not sent"
    assert initial_job_request.streaming_details is not None, (
        "Streaming details not sent in initial job request"
    )
    assert initial_job_request.streaming_details.public_key == "dummy-cert"


@pytest.mark.asyncio
@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize(
    "reason,sent_types,received",
    [
        (
            HordeFailureReason.INITIAL_RESPONSE_TIMED_OUT,
            [
                ValidatorAuthForMiner,
                V0InitialJobRequest,
                V0JobFinishedReceiptRequest,
            ],
            [
                # Nothing received from miner
            ],
        ),
        (
            HordeFailureReason.EXECUTOR_READINESS_RESPONSE_TIMED_OUT,
            [
                ValidatorAuthForMiner,
                V0InitialJobRequest,
                V0JobAcceptedReceiptRequest,
                V0JobFinishedReceiptRequest,
            ],
            [
                V0AcceptJobRequest(job_uuid=JOB_UUID).model_dump_json(),
                # Executor never reported ready
            ],
        ),
        (
            HordeFailureReason.VOLUMES_TIMED_OUT,
            [
                ValidatorAuthForMiner,
                V0InitialJobRequest,
                V0JobAcceptedReceiptRequest,
                V0JobRequest,
                V0JobFinishedReceiptRequest,
            ],
            [
                V0AcceptJobRequest(job_uuid=JOB_UUID).model_dump_json(),
                V0ExecutorReadyRequest(job_uuid=JOB_UUID).model_dump_json(),
                # Volumes never reported ready
            ],
        ),
        (
            HordeFailureReason.EXECUTION_TIMED_OUT,
            [
                ValidatorAuthForMiner,
                V0InitialJobRequest,
                V0JobAcceptedReceiptRequest,
                V0JobRequest,
                V0JobFinishedReceiptRequest,
            ],
            [
                V0AcceptJobRequest(job_uuid=JOB_UUID).model_dump_json(),
                V0ExecutorReadyRequest(job_uuid=JOB_UUID).model_dump_json(),
                V0VolumesReadyRequest(job_uuid=JOB_UUID).model_dump_json(),
                # Execution never finished
            ],
        ),
        (
            HordeFailureReason.FINAL_RESPONSE_TIMED_OUT,
            [
                ValidatorAuthForMiner,
                V0InitialJobRequest,
                V0JobAcceptedReceiptRequest,
                V0JobRequest,
                V0JobFinishedReceiptRequest,
            ],
            [
                V0AcceptJobRequest(job_uuid=JOB_UUID).model_dump_json(),
                V0ExecutorReadyRequest(job_uuid=JOB_UUID).model_dump_json(),
                V0VolumesReadyRequest(job_uuid=JOB_UUID).model_dump_json(),
                V0ExecutionDoneRequest(job_uuid=JOB_UUID).model_dump_json(),
                # Job finished never reported
            ],
        ),
    ],
)
@pytest.mark.parametrize(
    "job_details",
    [
        OrganicJobDetails(
            job_uuid=JOB_UUID,
            executor_class=ExecutorClass.always_on__llm__a6000,
            docker_image="mock",
            # Old single-timeout timing
            total_job_timeout=1,
        ),
        OrganicJobDetails(
            job_uuid=JOB_UUID,
            executor_class=ExecutorClass.always_on__llm__a6000,
            docker_image="mock",
            # Current per-stage timing
            job_timing=OrganicJobDetails.TimingDetails(
                allowed_leeway=1,
                download_time_limit=1,
                execution_time_limit=1,
                upload_time_limit=1,
                streaming_start_time_limit=0,
            ),
        ),
        OrganicJobDetails(
            job_uuid=JOB_UUID,
            executor_class=ExecutorClass.always_on__llm__a6000,
            docker_image="mock",
            # Even with all time limits=0, the leeway should be respected.
            # Repeating the tests with these details should still yield timeouts at each stage, not just the first one.
            job_timing=OrganicJobDetails.TimingDetails(
                allowed_leeway=2,
                download_time_limit=0,
                execution_time_limit=0,
                upload_time_limit=0,
                streaming_start_time_limit=0,
            ),
        ),
    ],
)
async def test_run_organic_job__raises_on_timeout(
    keypair, reason, received, sent_types, job_details, monkeypatch
):
    monkeypatch.setitem(
        EXECUTOR_CLASS,
        ExecutorClass.always_on__llm__a6000,
        ExecutorClassSpec(description="test", has_gpu=True, gpu_vram_gb=24, spin_up_time=1),
    )
    mock_transport = MinerStubTransport("mock", received)
    client = OrganicMinerClient(
        miner_hotkey="mock",
        miner_address="0.0.0.0",
        miner_port=1234,
        job_uuid=JOB_UUID,
        my_keypair=keypair,
        transport=mock_transport,
    )

    with pytest.raises(MinerTimedOut) as exc:
        await execute_organic_job_on_miner(
            client, job_details, reservation_time_limit=2, executor_startup_time_limit=2
        )

    assert exc.value.reason == reason

    sent_models_types = [type(model) for model in mock_transport.sent_models]
    assert sent_models_types == sent_types


@pytest.mark.asyncio
@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize(
    "exception_type,received,sent_types",
    [
        (
            MinerRejectedJob,
            [
                V0DeclineJobRequest(
                    job_uuid=JOB_UUID,
                    reason=JobRejectionReason.BUSY,
                    message="Executor busy",
                ).model_dump_json(),
            ],
            [
                ValidatorAuthForMiner,
                V0InitialJobRequest,
                V0JobFinishedReceiptRequest,
            ],
        ),
        (
            MinerReportedJobFailed,
            [
                V0AcceptJobRequest(job_uuid=JOB_UUID).model_dump_json(),
                V0ExecutorReadyRequest(job_uuid=JOB_UUID).model_dump_json(),
                V0VolumesReadyRequest(job_uuid=JOB_UUID).model_dump_json(),
                V0JobFailedRequest(
                    job_uuid=JOB_UUID,
                    reason=JobFailureReason.TIMEOUT,
                    message="Job execution timed out",
                ).model_dump_json(),
            ],
            [
                ValidatorAuthForMiner,
                V0InitialJobRequest,
                V0JobAcceptedReceiptRequest,
                V0JobRequest,
                V0JobFinishedReceiptRequest,
            ],
        ),
    ],
)
async def test_run_organic_job__raises_on_miner_reported_errors(
    keypair, exception_type, received, sent_types
):
    mock_transport = MinerStubTransport("mock", received)
    client = OrganicMinerClient(
        miner_hotkey="mock",
        miner_address="0.0.0.0",
        miner_port=1234,
        job_uuid=JOB_UUID,
        my_keypair=keypair,
        transport=mock_transport,
    )
    job_details = OrganicJobDetails(
        job_uuid=JOB_UUID,
        executor_class=ExecutorClass.always_on__llm__a6000,
        docker_image="mock",
        total_job_timeout=3,
    )

    with pytest.raises(exception_type):
        await execute_organic_job_on_miner(
            client, job_details, reservation_time_limit=1, executor_startup_time_limit=2
        )

    sent_models_types = [type(model) for model in mock_transport.sent_models]
    assert sent_models_types == sent_types


@pytest.mark.asyncio
@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize(
    "exception",
    [
        TransportConnectionError(),  # Thrown by WS transports
        Exception(),  # Just to check we are wrapping anything else that may happen there
    ],
)
async def test_run_organic_job__raises_on_connection_error(keypair, exception):
    failing_transport = FailingTransport("mock", exception)
    client = OrganicMinerClient(
        miner_hotkey="mock",
        miner_address="0.0.0.0",
        miner_port=1234,
        job_uuid=JOB_UUID,
        my_keypair=keypair,
        transport=failing_transport,
    )
    job_details = OrganicJobDetails(
        job_uuid=JOB_UUID,
        executor_class=ExecutorClass.always_on__llm__a6000,
        docker_image="mock",
        total_job_timeout=3,
    )

    with pytest.raises(MinerConnectionFailed) as exc:
        await execute_organic_job_on_miner(
            client, job_details, reservation_time_limit=1, executor_startup_time_limit=1
        )

    assert exc.value.reason == HordeFailureReason.MINER_CONNECTION_FAILED
    assert exc.value.__context__ == exception


# TODO:
#   - unhappy path
#       - send error event callback
