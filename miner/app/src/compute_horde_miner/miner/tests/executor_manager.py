import asyncio
import logging
from unittest import mock

from channels.testing import WebsocketCommunicator
from compute_horde.executor_class import DEFAULT_EXECUTOR_CLASS
from compute_horde.protocol_messages import V0ExecutionDoneRequest, V0VolumesReadyRequest
from compute_horde.test_wallet import get_test_miner_wallet, get_test_validator_wallet

from compute_horde_miner.miner.executor_manager import v1

WEBSOCKET_TIMEOUT = 100000


async def fake_executor(token, streaming=False):
    # The test settings make the executor manager load this module while the consumers that
    # make up the ASGI application are still being imported, so asgi cannot be imported above.
    from compute_horde_miner import asgi

    communicator = WebsocketCommunicator(asgi.application, f"v0.1/executor_interface/{token}")
    connected, _ = await communicator.connect()
    assert connected
    response = await communicator.receive_json_from(timeout=WEBSOCKET_TIMEOUT)
    assert response == {
        "message_type": "V0InitialJobRequest",
        "job_uuid": fake_executor.job_uuid,
        "executor_class": DEFAULT_EXECUTOR_CLASS,
        "docker_image": "it's teeeeests",
        "timeout_seconds": 60,
        "volume": None,
        "job_started_receipt_payload": {
            "receipt_type": "JobStartedReceipt",
            "job_uuid": fake_executor.job_uuid,
            "miner_hotkey": fake_executor.miner_hotkey
            or get_test_miner_wallet().hotkey.ss58_address,
            "validator_hotkey": get_test_validator_wallet().hotkey.ss58_address,
            "timestamp": mock.ANY,
            "executor_class": DEFAULT_EXECUTOR_CLASS,
            "is_organic": True,
            "ttl": 60,
        },
        "job_started_receipt_signature": mock.ANY,
        "validator_signature": mock.ANY,
        "executor_timing": None,
        "streaming_details": None
        if not streaming
        else {
            "public_key": "some_public_key",
            "executor_ip": "0.0.0.0",
        },
    }, response
    await communicator.send_json_to(
        {
            "message_type": "V0ExecutorReadyRequest",
            "job_uuid": fake_executor.job_uuid,
            "executor_token": None,
        }
    )
    response = await communicator.receive_json_from(timeout=WEBSOCKET_TIMEOUT)
    assert response == {
        "job_uuid": fake_executor.job_uuid,
        "message_type": "V0JobRequest",
        "executor_class": DEFAULT_EXECUTOR_CLASS,
        "docker_image": "it's teeeeests again",
        "env": {"SOME_ENV_VAR": "some value"},
        "raw_script": None,
        "docker_run_options_preset": "none",
        "docker_run_cmd": [],
        "volume": {"volume_type": "inline", "contents": "nonsense", "relative_path": None},
        "output_upload": mock.ANY,
        "artifacts_dir": None,
        "validator_signature": mock.ANY,
    }, response
    if streaming:
        await communicator.send_json_to(
            {
                "message_type": "V0StreamingJobReadyRequest",
                "job_uuid": fake_executor.job_uuid,
                "public_key": "some_public_key",
                "port": 1234,
            }
        )
    await communicator.send_json_to(
        V0VolumesReadyRequest(job_uuid=fake_executor.job_uuid).model_dump_json()
    )
    await communicator.send_json_to(
        V0ExecutionDoneRequest(job_uuid=fake_executor.job_uuid).model_dump_json()
    )
    await communicator.send_json_to(
        {
            "message_type": "V0JobFinishedRequest",
            "job_uuid": fake_executor.job_uuid,
            "docker_process_stdout": "some stdout",
            "docker_process_stderr": "some stderr",
            "artifacts": {},
            "upload_results": {
                # This is the raw HTTP response data from the uploader.
                "output.zip": '{"headers": {"Content-Length": "123", "ETag": "abc123"}, "body": "response body content"}'
            },
        }
    )
    await communicator.disconnect()


fake_executor.job_uuid = None
# Miner hotkey the receipt names, when the validator does not address the test miner.
fake_executor.miner_hotkey = None


class StubExecutorManager(v1.BaseExecutorManager):
    async def start_new_executor(self, token, executor_class, timeout):
        task = asyncio.get_running_loop().create_task(fake_executor(token))
        task.add_done_callback(self._listen_for_exceptions)

        return object()

    def _listen_for_exceptions(self, task: asyncio.Task):
        try:
            task.result()
        except BaseException as e:
            logging.exception(f"Fake executor failed: {e}", exc_info=e)
            raise

    async def wait_for_executor(self, executor, timeout):
        pass

    async def kill_executor(self, executor):
        pass

    async def get_manifest(self):
        return {DEFAULT_EXECUTOR_CLASS: 1}


class StubStreamingExecutorManager(StubExecutorManager):
    async def start_new_executor(self, token, executor_class, timeout):
        asyncio.get_running_loop().create_task(fake_executor(token, streaming=True))
        return object()
