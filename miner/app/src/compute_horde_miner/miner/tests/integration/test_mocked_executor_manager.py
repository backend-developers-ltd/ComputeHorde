import time
import uuid
from unittest.mock import MagicMock

import pytest
from compute_horde.protocol_consts import TRUSTED_MINER_FAKE_KEY
from compute_horde_core.streaming import StreamingDetails
from pytest_mock import MockerFixture

from compute_horde_miner.miner.models import Validator
from compute_horde_miner.miner.tests.executor_manager import (
    StubStreamingExecutorManager,
    fake_executor,
)
from compute_horde_miner.miner.tests.validator import (
    MANIFEST_MESSAGE,
    WEBSOCKET_TIMEOUT,
    authenticate,
    fake_validator,
    initial_job_request,
    job_request,
)

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.django_db(transaction=True),
    pytest.mark.usefixtures("authentication_on"),
]


@pytest.fixture
def mock_executor_ip(mocker: MockerFixture):
    return mocker.patch(
        "compute_horde_miner.miner.miner_consumer.executor_interface.MinerExecutorConsumer.get_executor_ip",
        lambda self: "0.0.0.0",
    )


@pytest.fixture
def mock_streaming_executor_manager_class(mocker: MockerFixture):
    mocker.patch(
        "compute_horde_miner.miner.executor_manager.current.executor_manager",
        StubStreamingExecutorManager(),
    )


@pytest.fixture
def job_uuid():
    _job_uuid = str(uuid.uuid4())
    fake_executor.job_uuid = _job_uuid
    yield _job_uuid
    fake_executor.job_uuid = None


async def run_regular_flow_test(
    job_uuid: str, streaming: bool = False, to_miner: str | None = None
):
    streaming_details = StreamingDetails(public_key="some_public_key") if streaming else None
    async with fake_validator() as communicator:
        await authenticate(communicator, to_miner)
        response = await communicator.receive_json_from(timeout=WEBSOCKET_TIMEOUT)
        assert response == MANIFEST_MESSAGE
        await communicator.send_to(
            initial_job_request(
                job_uuid, to_miner, streaming_details=streaming_details
            ).model_dump_json()
        )

        response = await communicator.receive_json_from(timeout=WEBSOCKET_TIMEOUT)
        assert response == {
            "message_type": "V0AcceptJobRequest",
            "job_uuid": job_uuid,
        }
        response = await communicator.receive_json_from(timeout=WEBSOCKET_TIMEOUT)
        assert response == {
            "message_type": "V0ExecutorReadyRequest",
            "job_uuid": job_uuid,
            "executor_token": None,
        }
        await communicator.send_to(job_request(job_uuid).model_dump_json())
        if streaming:
            response = await communicator.receive_json_from(timeout=WEBSOCKET_TIMEOUT)
            response.pop("miner_signature")  # ignore miner_signature field
            assert response == {
                "executor_token": None,
                "message_type": "V0StreamingJobReadyRequest",
                "job_uuid": job_uuid,
                "public_key": "some_public_key",
                "ip": "0.0.0.0",
                "port": 1234,
            }

        response = await communicator.receive_json_from(timeout=WEBSOCKET_TIMEOUT)
        assert response == {
            "message_type": "V0JobFinishedRequest",
            "job_uuid": job_uuid,
            "docker_process_stdout": "some stdout",
            "docker_process_stderr": "some stderr",
            "artifacts": {},
            "upload_results": {
                "output.zip": '{"headers": {"Content-Length": "123", "ETag": "abc123"}, "body": "response body content"}'
            },
        }


async def test_main_loop(validator: Validator, job_uuid: str, mock_executor_manager_class):
    await run_regular_flow_test(job_uuid)


async def test_streaming_main_loop(
    validator: Validator,
    job_uuid: str,
    mock_streaming_executor_manager_class,
    mock_executor_ip: MagicMock,
):
    await run_regular_flow_test(job_uuid, streaming=True)


@pytest.fixture
def trusted_miner_receipts():
    fake_executor.miner_hotkey = TRUSTED_MINER_FAKE_KEY
    yield
    fake_executor.miner_hotkey = None


async def test_local_miner(
    validator: Validator,
    job_uuid: str,
    mock_executor_manager_class,
    trusted_miner_receipts,
    settings,
):
    settings.IS_LOCAL_MINER = True

    await run_regular_flow_test(job_uuid, to_miner=TRUSTED_MINER_FAKE_KEY)


async def test_local_miner_unknown_validator(mock_executor_manager_class, settings):
    settings.IS_LOCAL_MINER = True

    validator_key = "unknown_validator"

    async with fake_validator(validator_key) as communicator:
        await communicator.send_json_to(
            {
                "message_type": "ValidatorAuthForMiner",
                "validator_hotkey": validator_key,
                "miner_hotkey": "some key",
                "timestamp": int(time.time()),
                "signature": "gibberish",
            }
        )
        response = await communicator.receive_json_from(timeout=WEBSOCKET_TIMEOUT)

        assert response == {
            "message_type": "GenericError",
            "details": f"Unknown validator: {validator_key}",
        }
