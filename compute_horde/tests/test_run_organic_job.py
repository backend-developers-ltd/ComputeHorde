from unittest.mock import patch

import pytest

from compute_horde.miner_client.organic import (
    OrganicJobDetails,
    OrganicMinerClient,
    run_organic_job,
)
from compute_horde.mv_protocol.miner_requests import V0ExecutorReadyRequest, V0JobFinishedRequest
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


class MockOrganicMinerClient(OrganicMinerClient):
    def __init__(self, *args, **kwargs):
        kwargs["transport"] = MinerStubTransport(
            "mock",
            [
                V0ExecutorReadyRequest(job_uuid=JOB_UUID).model_dump_json(),
                V0JobFinishedRequest(
                    job_uuid=JOB_UUID,
                    docker_process_stdout="stdout",
                    docker_process_stderr="stderr",
                ).model_dump_json(),
            ],
        )
        super().__init__(*args, **kwargs)


@pytest.fixture
def mocked_organic_miner_client():
    with patch("compute_horde.miner_client.organic.OrganicMinerClient") as MockedClient:
        MockedClient.instance = None

        def side_effect(*args, **kwargs):
            if MockedClient.instance is not None:
                raise RuntimeError("You can create only single instance of mocked MinerClient")
            MockedClient.instance = MockOrganicMinerClient(*args, **kwargs)
            return MockedClient.instance

        MockedClient.side_effect = side_effect
        yield MockedClient


@pytest.mark.asyncio
async def test_run_organic_job__success(mocked_organic_miner_client, keypair):
    job_details = OrganicJobDetails(
        job_uuid=JOB_UUID,
        miner_hotkey="mock",
        miner_address="0.0.0.0",
        miner_port=1234,
        docker_image="mock",
    )
    stdout, stderr = await run_organic_job(job_details, keypair, wait_timeout=2)

    assert stdout == "stdout"
    assert stderr == "stderr"

    sent_models = mocked_organic_miner_client.instance.transport.sent_models
    sent_models_types = [type(model) for model in sent_models]
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
