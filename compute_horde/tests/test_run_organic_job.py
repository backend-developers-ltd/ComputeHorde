import pytest
from compute_horde_core.executor_class import ExecutorClass
from pydantic import TypeAdapter

from compute_horde.miner_client.organic import (
    OrganicJobDetails,
    OrganicMinerClient,
    run_organic_job,
)
from compute_horde.protocol_messages import (
    V0AcceptJobRequest,
    V0ExecutorReadyRequest,
    V0InitialJobRequest,
    V0JobAcceptedReceiptRequest,
    V0JobFinishedReceiptRequest,
    V0JobFinishedRequest,
    V0JobRequest,
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
    stdout, stderr, artifacts = await run_organic_job(
        client, job_details, executor_ready_timeout=2, initial_response_timeout=2
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


# TODO:
#   - unhappy path
#       - connection error
#       - initial_response timeout
#       - send error event callback
#       - declined
#       - executor failed
#       - full_response timeout
#       - job failed
