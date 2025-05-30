import pytest
from compute_horde_core.executor_class import ExecutorClass
from compute_horde_core.streaming import StreamingDetails
from pydantic import TypeAdapter

from compute_horde.miner_client.organic import (
    OrganicJobDetails,
    OrganicMinerClient,
    execute_organic_job_on_miner,
)
from compute_horde.protocol_messages import (
    V0AcceptJobRequest,
    V0ExecutionDoneRequest,
    V0ExecutorReadyRequest,
    V0InitialJobRequest,
    V0JobAcceptedReceiptRequest,
    V0JobFinishedReceiptRequest,
    V0JobFinishedRequest,
    V0JobRequest,
    V0StreamingJobReadyRequest,
    V0VolumesReadyRequest,
    ValidatorAuthForMiner,
    ValidatorToMinerMessage,
)
from compute_horde.transport import StubTransport

JOB_UUID = "b4793a02-33a2-4a49-b4e2-4a7b903847e7"


class MinerStubTransport(StubTransport):
    def __init__(self, name: str, messages: list[str], *args, **kwargs):
        super().__init__(name, messages, *args, **kwargs)
        self.sent_models = []

    async def send(self, message):
        await super().send(message)
        self.sent_models.append(TypeAdapter(ValidatorToMinerMessage).validate_json(message))


@pytest.mark.asyncio
@pytest.mark.django_db(transaction=True)
async def test_run_organic_job__success(keypair):
    mock_transport = MinerStubTransport(
        "mock",
        [
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


# TODO:
#   - unhappy path
#       - connection error
#       - initial_response timeout
#       - send error event callback
#       - declined
#       - executor failed
#       - full_response timeout
#       - job failed
