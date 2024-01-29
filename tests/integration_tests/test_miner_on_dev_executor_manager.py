import asyncio
import base64
import io
import json
import os
import random
import string
import subprocess
import uuid
import zipfile
from unittest import mock

import pytest
import requests
import websockets

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
            requests.get(f'http://localhost:{MINER_PORT}/admin/login', timeout=1)
        except IOError:
            return False
        return True

    @classmethod
    def miner_path_and_args(cls) -> list[str]:
        return ['python', 'miner/app/src/manage.py', 'runserver', str(MINER_PORT)]

    @classmethod
    def miner_preparation_tasks(cls):
        db_shell_cmd = 'python miner/app/src/manage.py dbshell'
        for cmd in [
            f'echo "DROP DATABASE IF EXISTS compute_horde_miner_integration_test" | {db_shell_cmd}',
            f'echo "CREATE DATABASE compute_horde_miner_integration_test" | {db_shell_cmd}',
        ]:
            subprocess.check_call(cmd, shell=True)
        for args in [
            ['python', 'miner/app/src/manage.py', 'migrate'],
            ['python', 'miner/app/src/manage.py', 'debug_add_validator', validator_key],
        ]:
            subprocess.check_call(args, env={**os.environ, 'DATABASE_SUFFIX': '_integration_test'})

    @classmethod
    def miner_environ(cls) -> dict[str, str]:
        return {
            'ADDRESS_FOR_EXECUTORS': f'ws://localhost:{MINER_PORT}',
            'DATABASE_SUFFIX': '_integration_test'
        }

    @classmethod
    def validator_path_and_args(cls) -> list[str]:
        return ['sleep', '100000']

    @classmethod
    def validator_environ(cls) -> dict[str, str]:
        return {}

    @pytest.mark.asyncio
    async def test_echo_image(self):
        job_uuid = str(uuid.uuid4())

        payload = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(32))
        in_memory_output = io.BytesIO()
        zipf = zipfile.ZipFile(in_memory_output, 'w')
        zipf.writestr('payload.txt', payload)
        zipf.close()
        in_memory_output.seek(0)
        zip_contents = in_memory_output.read()
        base64_zipfile = base64.b64encode(zip_contents).decode()

        async with websockets.connect(f'ws://localhost:8045/v0/validator_interface/{validator_key}') as ws:
            await ws.send(json.dumps({
                "message_type": "V0InitialJobRequest",
                "job_uuid": job_uuid,
                "base_docker_image_name": "alpine",
                "timeout_seconds": 60,
                "volume_type": "inline"
            }))
            response = json.loads(await asyncio.wait_for(ws.recv(), timeout=WEBSOCKET_TIMEOUT))
            assert response == {
                "message_type": "V0AcceptJobRequest",
                "job_uuid": job_uuid,
            }
            response = json.loads(await asyncio.wait_for(ws.recv(), timeout=WEBSOCKET_TIMEOUT))
            assert response == {
                "message_type": "V0ExecutorReadyRequest",
                "job_uuid": job_uuid,
            }

            await ws.send(json.dumps({
                "message_type": "V0JobRequest",
                "job_uuid": job_uuid,
                "docker_image_name": "ghcr.io/reef-technologies/computehorde/echo:latest",
                "timeout_seconds": 60,
                "volume": {
                    "volume_type": "inline",
                    "contents": base64_zipfile,
                },
            }))
            response = json.loads(await asyncio.wait_for(ws.recv(), timeout=WEBSOCKET_TIMEOUT))
            assert response == {
                "message_type": "V0JobFinishedRequest",
                "job_uuid": job_uuid,
                "docker_process_stdout": payload,
                "docker_process_stderr": mock.ANY,
            }
