import time
import uuid

import pytest
import pytest_asyncio
from channels.testing import WebsocketCommunicator
from compute_horde.executor_class import DEFAULT_EXECUTOR_CLASS
from pytest_mock import MockerFixture

from compute_horde_miner import asgi
from compute_horde_miner.miner.executor_manager.base import ExecutorUnavailable
from compute_horde_miner.miner.models import Validator
from compute_horde_miner.miner.tests.executor_manager import fake_executor

WEBSOCKET_TIMEOUT = 10


@pytest.fixture
def validator_key():
    return "some_public_key"


@pytest_asyncio.fixture(autouse=True)
async def validator(validator_key: str):
    return await Validator.objects.acreate(public_key=validator_key, active=True)


@pytest_asyncio.fixture
async def communicator(validator_key: str):
    _communicator = WebsocketCommunicator(
        asgi.application, f"v0.1/validator_interface/{validator_key}"
    )
    connected, _ = await _communicator.connect()
    assert connected

    yield _communicator

    await _communicator.disconnect()


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_main_loop(validator_key: str, communicator: WebsocketCommunicator):
    job_uuid = str(uuid.uuid4())
    fake_executor.job_uuid = job_uuid

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
        "manifest": {"executor_classes": [{"count": 1, "executor_class": DEFAULT_EXECUTOR_CLASS}]},
    }
    await communicator.send_json_to(
        {
            "message_type": "V0InitialJobRequest",
            "job_uuid": job_uuid,
            "executor_class": DEFAULT_EXECUTOR_CLASS,
            "base_docker_image_name": "it's teeeeests",
            "timeout_seconds": 60,
            "volume_type": "inline",
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


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_decline_job(
    validator_key: str, communicator: WebsocketCommunicator, mocker: MockerFixture
):
    mocker.patch(
        "compute_horde_miner.miner.tests.executor_manager.TestExecutorManager.reserve_executor",
        side_effect=ExecutorUnavailable,
    )

    job_uuid = str(uuid.uuid4())
    fake_executor.job_uuid = job_uuid

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
    await communicator.receive_json_from(timeout=WEBSOCKET_TIMEOUT)

    await communicator.send_json_to(
        {
            "message_type": "V0InitialJobRequest",
            "job_uuid": job_uuid,
            "executor_class": DEFAULT_EXECUTOR_CLASS,
            "base_docker_image_name": "it's teeeeests",
            "timeout_seconds": 60,
            "volume_type": "inline",
        }
    )
    response = await communicator.receive_json_from(timeout=WEBSOCKET_TIMEOUT)
    assert response == {
        "message_type": "V0DeclineJobRequest",
        "job_uuid": job_uuid,
    }
