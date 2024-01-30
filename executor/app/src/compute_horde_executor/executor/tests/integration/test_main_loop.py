import asyncio
import base64
import io
import json
import random
import string
import uuid
import zipfile
from unittest import mock

from compute_horde_executor.executor.management.commands.run_executor import Command, MinerClient

payload = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(32))

in_memory_output = io.BytesIO()
zipf = zipfile.ZipFile(in_memory_output, 'w')
zipf.writestr('payload.txt', payload)
zipf.close()
in_memory_output.seek(0)
zip_contents = in_memory_output.read()
base64_zipfile = base64.b64encode(zip_contents).decode()

job_uuid = str(uuid.uuid4())


class MockWebsocket:
    def __init__(self):
        self.closed = False
        self.sent: list[str] = []
        self.messages = iter([
            json.dumps({
                "message_type": "V0PrepareJobRequest",
                "base_docker_image_name": "alpine",
                "timeout_seconds": None,
                "volume_type": "inline",
                "job_uuid": job_uuid,
            }),
            json.dumps({
                "message_type": "V0RunJobRequest",
                "docker_image_name": "ghcr.io/backend-developers-ltd/computehorde/echo:latest",
                "docker_run_cmd": [],
                "docker_run_options": [],
                "volume": {
                    "volume_type": "inline",
                    "contents": base64_zipfile,
                },
                "job_uuid": job_uuid,
            }),
        ])
        self.sent_messages: list[str] = []

    async def send(self, message):
        self.sent_messages.append(message)

    async def recv(self):
        try:
            return next(self.messages)
        except StopIteration:
            await asyncio.Future()

    async def close(self):
        ...


class TestMinerClient(MinerClient):
    async def _connect(self):
        return MockWebsocket()


class TestCommand(Command):
    MINER_CLIENT_CLASS = TestMinerClient


def test_main_loop():
    command = TestCommand()
    command.handle()
    assert [json.loads(msg) for msg in command.miner_client.ws.sent_messages] == [
        {
            "message_type": "V0ReadyRequest",
            "job_uuid": job_uuid,
        },
        {
            "message_type": "V0FinishedRequest",
            "docker_process_stdout": payload,
            "docker_process_stderr": mock.ANY,
            "job_uuid": job_uuid,
        }
    ]

