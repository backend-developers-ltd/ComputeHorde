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
from compute_horde_miner.miner.tests.executor_manager import StubExecutorManager, fake_executor

pytestmark = [pytest.mark.asyncio, pytest.mark.django_db(transaction=True)]

WEBSOCKET_TIMEOUT = 3


@pytest.fixture
def mock_keypair(mocker: MockerFixture):
    return mocker.patch(
        "compute_horde_miner.miner.miner_consumer.validator_interface.bittensor.Keypair"
    )


# Somehow the regular dependency mechanism doesn't work with multiple test cases
# Explicit patching with a new instance each time is a temporary workaround
@pytest.fixture(autouse=True)
def _patch_executor_manager_class(mocker: MockerFixture):
    mocker.patch(
        "compute_horde_miner.miner.executor_manager.current.executor_manager",
        StubExecutorManager(),
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


async def run_regular_flow_test(validator_key: str, job_uuid: str):
    async with make_communicator(validator_key) as communicator:
        await communicator.send_json_to(
            {
                "message_type": "V0AuthenticateRequest",
                "payload": {
                    "validator_hotkey": validator_key,
                    "miner_hotkey": "some key",
                    "timestamp": int(time.time()),
                },
                "signature": "gibberish",
            }
        )
        response = await communicator.receive_json_from(timeout=WEBSOCKET_TIMEOUT)
        assert response == {
            "message_type": "V0ExecutorManifestRequest",
            "manifest": {
                "executor_classes": [{"count": 1, "executor_class": DEFAULT_EXECUTOR_CLASS}]
            },
        }
        await communicator.send_json_to(
            {
                "message_type": "V0InitialJobRequest",
                "job_uuid": job_uuid,
                "executor_class": DEFAULT_EXECUTOR_CLASS,
                "base_docker_image_name": "it's teeeeests",
                "timeout_seconds": 60,
                "volume_type": "inline",
                "job_started_receipt_payload": {
                    "receipt_type": "JobStartedReceipt",
                    "job_uuid": job_uuid,
                    "miner_hotkey": "miner_hotkey",
                    "validator_hotkey": validator_key,
                    "timestamp": "2020-01-01T00:00Z",
                    "executor_class": DEFAULT_EXECUTOR_CLASS,
                    "max_timeout": 60,
                    "ttl": 5,
                },
                "job_started_receipt_signature": "gibberish",
            }
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
        }

        await communicator.send_json_to(
            {
                "message_type": "V0JobRequest",
                "job_uuid": job_uuid,
                "executor_class": DEFAULT_EXECUTOR_CLASS,
                "docker_image_name": "it's teeeeests again",
                "docker_run_cmd": [],
                "docker_run_options_preset": "none",
                "volume": {"volume_type": "inline", "contents": "nonsense"},
            }
        )
        response = await communicator.receive_json_from(timeout=WEBSOCKET_TIMEOUT)
        assert response == {
            "message_type": "V0JobFinishedRequest",
            "job_uuid": job_uuid,
            "docker_process_stdout": "some stdout",
            "docker_process_stderr": "some stderr",
        }


async def test_main_loop(validator: Validator, job_uuid: str, mock_keypair: MagicMock):
    await run_regular_flow_test(validator.public_key, job_uuid)


async def test_local_miner(validator: Validator, job_uuid: str, mock_keypair: MagicMock, settings):
    settings.IS_LOCAL_MINER = True
    settings.DEBUG_TURN_AUTHENTICATION_OFF = False

    await run_regular_flow_test(validator.public_key, job_uuid)

    mock_keypair.assert_called_once_with(ss58_address=validator.public_key)
    mock_keypair.return_value.verify.assert_called_once()


async def test_local_miner_unknown_validator(mock_keypair: MagicMock, settings):
    settings.IS_LOCAL_MINER = True
    settings.DEBUG_TURN_AUTHENTICATION_OFF = False

    validator_key = "unknown_validator"

    async with make_communicator(validator_key) as communicator:
        await communicator.send_json_to(
            {
                "message_type": "V0AuthenticateRequest",
                "payload": {
                    "validator_hotkey": validator_key,
                    "miner_hotkey": "some key",
                    "timestamp": int(time.time()),
                },
                "signature": "gibberish",
            }
        )
        response = await communicator.receive_json_from(timeout=WEBSOCKET_TIMEOUT)

        assert response == {
            "message_type": "GenericError",
            "details": f"Unknown validator: {validator_key}",
        }
