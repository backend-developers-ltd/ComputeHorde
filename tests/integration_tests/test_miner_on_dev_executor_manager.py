import asyncio
import base64
import io
import json
import os
import random
import string
import subprocess
import sys
import time
import uuid
import zipfile
from unittest import mock

import pytest
import requests
import websockets

from compute_horde.executor_class import DEFAULT_EXECUTOR_CLASS
from compute_horde.test_base import ActiveSubnetworkBaseTest

MINER_PORT = 8045
WEBSOCKET_TIMEOUT = 10
validator_key = str(uuid.uuid4())


class Test(ActiveSubnetworkBaseTest):
    @classmethod
    def check_if_validator_is_up(cls):
        return True

    @classmethod
    def check_if_miner_is_up(cls):
        try:
            requests.get(f"http://localhost:{MINER_PORT}/admin/login", timeout=1)
        except IOError:
            return False
        return True

    @classmethod
    def miner_path_and_args(cls) -> list[str]:
        return [sys.executable, "miner/app/src/manage.py", "runserver", str(MINER_PORT)]

    @classmethod
    def miner_preparation_tasks(cls):
        db_shell_cmd = f"{sys.executable} miner/app/src/manage.py dbshell"
        for cmd in [
            f'echo "DROP DATABASE IF EXISTS compute_horde_miner_integration_test" | {db_shell_cmd}',
            f'echo "CREATE DATABASE compute_horde_miner_integration_test" | {db_shell_cmd}',
        ]:
            subprocess.check_call(cmd, shell=True)
        for args in [
            [sys.executable, "miner/app/src/manage.py", "migrate"],
            [sys.executable, "miner/app/src/manage.py", "debug_add_validator", validator_key],
        ]:
            subprocess.check_call(
                args, env={**os.environ, "DATABASE_SUFFIX": "_integration_test"}
            )

    @classmethod
    def miner_environ(cls) -> dict[str, str]:
        return {
            "ADDRESS_FOR_EXECUTORS": "localhost",
            "PORT_FOR_EXECUTORS": str(MINER_PORT),
            "DATABASE_SUFFIX": "_integration_test",
            "DEBUG_TURN_AUTHENTICATION_OFF": "1",
        }

    @classmethod
    def validator_path_and_args(cls) -> list[str]:
        return ["sleep", "100000"]

    @classmethod
    def validator_environ(cls) -> dict[str, str]:
        return {}

    @pytest.mark.asyncio
    async def test_echo_image(self):
        job_uuid = str(uuid.uuid4())

        payload = "".join(
            random.choice(string.ascii_uppercase + string.digits) for _ in range(32)
        )
        in_memory_output = io.BytesIO()
        zipf = zipfile.ZipFile(in_memory_output, "w")
        zipf.writestr("payload.txt", payload)
        zipf.close()
        in_memory_output.seek(0)
        zip_contents = in_memory_output.read()
        base64_zipfile = base64.b64encode(zip_contents).decode()

        async with websockets.connect(
            f"ws://localhost:8045/v0.1/validator_interface/{validator_key}"
        ) as ws:
            await ws.send(
                json.dumps(
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
            )
            response = json.loads(
                await asyncio.wait_for(ws.recv(), timeout=WEBSOCKET_TIMEOUT)
            )
            assert response == {
                "message_type": "V0ExecutorManifestRequest",
                "manifest": {
                    "executor_classes": [
                        {"count": 1, "executor_class": DEFAULT_EXECUTOR_CLASS}
                    ]
                },
            }
            await ws.send(
                json.dumps(
                    {
                        "message_type": "V0InitialJobRequest",
                        "job_uuid": job_uuid,
                        "executor_class": DEFAULT_EXECUTOR_CLASS,
                        "base_docker_image_name": "alpine",
                        "timeout_seconds": 60,
                        "volume_type": "inline",
                    }
                )
            )
            response = json.loads(
                await asyncio.wait_for(ws.recv(), timeout=WEBSOCKET_TIMEOUT)
            )
            assert response == {
                "message_type": "V0AcceptJobRequest",
                "job_uuid": job_uuid,
            }
            response = json.loads(
                await asyncio.wait_for(ws.recv(), timeout=WEBSOCKET_TIMEOUT)
            )
            assert response == {
                "message_type": "V0ExecutorReadyRequest",
                "job_uuid": job_uuid,
            }

            await ws.send(
                json.dumps(
                    {
                        "message_type": "V0JobRequest",
                        "job_uuid": job_uuid,
                        "executor_class": DEFAULT_EXECUTOR_CLASS,
                        "docker_image_name": "backenddevelopersltd/compute-horde-job-echo:v0-latest",
                        "docker_run_cmd": [],
                        "docker_run_options_preset": "none",
                        "timeout_seconds": 60,
                        "volume": {
                            "volume_type": "inline",
                            "contents": base64_zipfile,
                        },
                    }
                )
            )
            response = json.loads(
                await asyncio.wait_for(ws.recv(), timeout=WEBSOCKET_TIMEOUT)
            )
            assert response == {
                "message_type": "V0MachineSpecsRequest",
                "job_uuid": job_uuid,
                "specs": mock.ANY,
            }
            response = json.loads(
                await asyncio.wait_for(ws.recv(), timeout=WEBSOCKET_TIMEOUT)
            )
            assert response == {
                "message_type": "V0JobFinishedRequest",
                "job_uuid": job_uuid,
                "docker_process_stdout": payload,
                "docker_process_stderr": mock.ANY,
            }
