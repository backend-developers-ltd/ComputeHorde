import time
import uuid

import pytest
from channels.testing import WebsocketCommunicator
from compute_horde.executor_class import DEFAULT_EXECUTOR_CLASS

from compute_horde_miner import asgi
from compute_horde_miner.miner.models import Validator
from compute_horde_miner.miner.tests.executor_manager import fake_executor

WEBSOCKET_TIMEOUT = 10


@pytest.mark.asyncio
@pytest.mark.django_db(transaction=True)
async def test_main_loop():
    validator_key = "some_public_key"
    await Validator.objects.acreate(public_key=validator_key, active=True)

    job_uuid = str(uuid.uuid4())
    fake_executor.job_uuid = job_uuid
    communicator = WebsocketCommunicator(
        asgi.application, f"v0.1/validator_interface/{validator_key}"
    )
    connected, _ = await communicator.connect()
    assert connected
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
    await communicator.disconnect()
