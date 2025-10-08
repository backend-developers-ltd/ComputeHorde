import contextlib
import time
import uuid
from unittest.mock import MagicMock

import pytest
import pytest_asyncio
from channels.testing import WebsocketCommunicator
from compute_horde.executor_class import DEFAULT_EXECUTOR_CLASS
from pytest_mock import MockerFixture

from compute_horde_miner import asgi
from compute_horde_miner.miner.models import Validator
from compute_horde_miner.miner.tests.executor_manager import (
    StubExecutorManager,
    StubStreamingExecutorManager,
    fake_executor,
)

pytestmark = [pytest.mark.asyncio, pytest.mark.django_db(transaction=True)]

WEBSOCKET_TIMEOUT = 3


@pytest.fixture
def mock_keypair(mocker: MockerFixture):
    return mocker.patch(
        "compute_horde_miner.miner.miner_consumer.validator_interface.bittensor.Keypair"
    )


@pytest.fixture
def mock_executor_ip(mocker: MockerFixture):
    return mocker.patch(
        "compute_horde_miner.miner.miner_consumer.executor_interface.MinerExecutorConsumer.get_executor_ip",
        lambda self: "0.0.0.0",
    )


# Somehow the regular dependency mechanism doesn't work with multiple test cases
# Explicit patching with a new instance each time is a temporary workaround
@pytest.fixture
def mock_executor_manager_class(mocker: MockerFixture):
    mocker.patch(
        "compute_horde_miner.miner.executor_manager.current.executor_manager",
        StubExecutorManager(),
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


@pytest.fixture
def validator_hotkey():
    return "some_public_key"


@pytest_asyncio.fixture
async def validator(validator_hotkey: str):
    return await Validator.objects.acreate(public_key=validator_hotkey, active=True)


@contextlib.asynccontextmanager
async def make_communicator(validator_key: str):
    communicator = WebsocketCommunicator(
        asgi.application, f"v0.1/validator_interface/{validator_key}"
    )
    connected, _ = await communicator.connect()
    assert connected
    yield communicator
    await communicator.disconnect()


async def run_regular_flow_test(validator_key: str, job_uuid: str, streaming: bool = False):
    async with make_communicator(validator_key) as communicator:
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
            "message_type": "V0ExecutorManifestRequest",
            "manifest": {DEFAULT_EXECUTOR_CLASS: 1},
        }
        msg = {
            "message_type": "V0InitialJobRequest",
            "job_uuid": job_uuid,
            "executor_class": DEFAULT_EXECUTOR_CLASS,
            "docker_image": "it's teeeeests",
            "timeout_seconds": 60,
            "volume": None,
            "job_started_receipt_payload": {
                "receipt_type": "JobStartedReceipt",
                "job_uuid": job_uuid,
                "miner_hotkey": "miner_hotkey",
                "validator_hotkey": validator_key,
                "timestamp": "2020-01-01T00:00:00Z",
                "executor_class": DEFAULT_EXECUTOR_CLASS,
                "is_organic": True,
                "ttl": 5,
            },
            "job_started_receipt_signature": "gibberish",
        }
        if streaming:
            msg["streaming_details"] = {
                "public_key": "some_public_key",
                "executor_ip": None,
            }
        await communicator.send_json_to(msg)

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
        await communicator.send_json_to(
            {
                "message_type": "V0JobRequest",
                "job_uuid": job_uuid,
                "executor_class": DEFAULT_EXECUTOR_CLASS,
                "docker_image": "it's teeeeests again",
                "docker_run_cmd": [],
                "env": {"SOME_ENV_VAR": "some value"},
                "docker_run_options_preset": "none",
                "volume": {"volume_type": "inline", "contents": "nonsense"},
                "output_upload": {
                    "output_upload_type": "zip_and_http_post",
                    "url": "http://localhost/output/upload",
                    "form_fields": {},
                },
            }
        )
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


async def test_main_loop(
    validator: Validator, job_uuid: str, mock_keypair: MagicMock, mock_executor_manager_class
):
    await run_regular_flow_test(validator.public_key, job_uuid)


async def test_streaming_main_loop(
    validator: Validator,
    job_uuid: str,
    mock_keypair: MagicMock,
    mock_streaming_executor_manager_class,
    mock_executor_ip: MagicMock,
):
    await run_regular_flow_test(validator.public_key, job_uuid, streaming=True)


async def test_local_miner(
    validator: Validator,
    job_uuid: str,
    mock_keypair: MagicMock,
    mock_executor_manager_class,
    settings,
):
    settings.IS_LOCAL_MINER = True
    settings.DEBUG_TURN_AUTHENTICATION_OFF = False

    await run_regular_flow_test(validator.public_key, job_uuid)

    mock_keypair.assert_called_once_with(ss58_address=validator.public_key)
    mock_keypair.return_value.verify.assert_called_once()


async def test_local_miner_unknown_validator(
    mock_keypair: MagicMock, mock_executor_manager_class, settings
):
    settings.IS_LOCAL_MINER = True
    settings.DEBUG_TURN_AUTHENTICATION_OFF = False

    validator_key = "unknown_validator"

    async with make_communicator(validator_key) as communicator:
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
