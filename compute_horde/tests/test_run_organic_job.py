import pytest

from compute_horde.miner_client.organic import (
    OrganicJobDetails,
    OrganicMinerClient,
    run_organic_job,
)
from compute_horde.mv_protocol.miner_requests import (
    V0AcceptJobRequest,
    V0ExecutorReadyRequest,
    V0JobFinishedRequest,
)
from compute_horde.mv_protocol.validator_requests import (
    BaseValidatorRequest,
    V0AuthenticateRequest,
    V0InitialJobRequest,
    V0JobFinishedReceiptRequest,
    V0JobRequest,
    V0JobStartedReceiptRequest,
)
from compute_horde.transport import StubTransport

JOB_UUID = "b4793a02-33a2-4a49-b4e2-4a7b903847e7"


class MinerStubTransport(StubTransport):
    def __init__(self, name: str, messages: list[str], *args, **kwargs):
        super().__init__(name, messages, *args, **kwargs)
        self.sent_models = []

    async def send(self, message):
        await super().send(message)
        self.sent_models.append(BaseValidatorRequest.parse(message))


@pytest.mark.asyncio
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
    job_details = OrganicJobDetails(job_uuid=JOB_UUID, docker_image="mock")
    stdout, stderr = await run_organic_job(client, job_details, wait_timeout=2)

    assert stdout == "stdout"
    assert stderr == "stderr"

    sent_models_types = [type(model) for model in mock_transport.sent_models]
    assert sent_models_types == [
        V0AuthenticateRequest,
        V0InitialJobRequest,
        V0JobStartedReceiptRequest,
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
