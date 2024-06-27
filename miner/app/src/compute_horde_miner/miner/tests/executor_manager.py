import asyncio
from unittest import mock

from channels.testing import WebsocketCommunicator
from compute_horde.executor_class import DEFAULT_EXECUTOR_CLASS

from compute_horde_miner import asgi
from compute_horde_miner.miner.executor_manager import v1

WEBSOCKET_TIMEOUT = 100000


async def fake_executor(token):
    communicator = WebsocketCommunicator(asgi.application, f"v0.1/executor_interface/{token}")
    connected, _ = await communicator.connect()
    assert connected
    response = await communicator.receive_json_from(timeout=WEBSOCKET_TIMEOUT)
    assert response == {
        "job_uuid": fake_executor.job_uuid,
        "message_type": "V0PrepareJobRequest",
        "base_docker_image_name": "it's teeeeests",
        "timeout_seconds": 60,
        "volume_type": "inline",
    }, response
    await communicator.send_json_to(
        {
            "message_type": "V0ReadyRequest",
            "job_uuid": fake_executor.job_uuid,
        }
    )
    response = await communicator.receive_json_from(timeout=WEBSOCKET_TIMEOUT)
    assert response == {
        "job_uuid": fake_executor.job_uuid,
        "message_type": "V0RunJobRequest",
        "docker_image_name": "it's teeeeests again",
        "raw_script": None,
        "docker_run_options_preset": "none",
        "docker_run_cmd": [],
        "volume": {"volume_type": "inline", "contents": "nonsense", "relative_path": None},
        "output_upload": mock.ANY,
    }, response
    await communicator.send_json_to(
        {
            "message_type": "V0FinishedRequest",
            "job_uuid": fake_executor.job_uuid,
            "docker_process_stdout": "some stdout",
            "docker_process_stderr": "some stderr",
        }
    )
    await communicator.disconnect()


fake_executor.job_uuid = None


class TestExecutorManager(v1.BaseExecutorManager):
    async def start_new_executor(self, token, executor_class, timeout):
        asyncio.get_running_loop().create_task(fake_executor(token))
        return object()

    async def wait_for_executor(self, executor, timeout):
        pass

    async def kill_executor(self, executor):
        pass

    async def get_manifest(self):
        return {DEFAULT_EXECUTOR_CLASS: 1}
