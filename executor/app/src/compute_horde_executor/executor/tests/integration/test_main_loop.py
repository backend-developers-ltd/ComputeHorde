import asyncio
import base64
import io
import json
import random
import string
import uuid
import zipfile
from functools import partial
from unittest import mock

from pytest_httpx import HTTPXMock

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


class ContainsStr:
    def __init__(self, contained: str) -> None:
        self.contained = contained

    def __eq__(self, other):
        return self.contained in other


class MockWebsocket:
    def __init__(self, messages):
        self.closed = False
        self.sent: list[str] = []
        self.messages = messages
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
    def __init__(self, *args, messages, **kwargs):
        super().__init__(*args, **kwargs)
        self.__messages = messages

    async def _connect(self):
        return MockWebsocket(self.__messages)


class TestCommand(Command):
    def __init__(self, messages, *args, **kwargs):
        self.MINER_CLIENT_CLASS = partial(TestMinerClient, messages=messages)
        super().__init__(*args, **kwargs)


def test_main_loop():
    command = TestCommand(iter([
        json.dumps({
            "message_type": "V0PrepareJobRequest",
            "base_docker_image_name": "alpine",
            "timeout_seconds": None,
            "volume_type": "inline",
            "job_uuid": job_uuid,
        }),
        json.dumps({
            "message_type": "V0RunJobRequest",
            "docker_image_name": "backenddevelopersltd/compute-horde-job-echo:v0-latest",
            "docker_run_cmd": [],
            "docker_run_options_preset": 'none',
            "volume": {
                "volume_type": "inline",
                "contents": base64_zipfile,
            },
            "job_uuid": job_uuid,
        }),
    ]))
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


def test_zip_url_volume(httpx_mock: HTTPXMock):
    zip_url = 'https://localhost/payload.txt'
    httpx_mock.add_response(url=zip_url, content=zip_contents)

    command = TestCommand(iter([
        json.dumps({
            "message_type": "V0PrepareJobRequest",
            "base_docker_image_name": "alpine",
            "timeout_seconds": None,
            "volume_type": "zip_url",
            "job_uuid": job_uuid,
        }),
        json.dumps({
            "message_type": "V0RunJobRequest",
            "docker_image_name": "backenddevelopersltd/compute-horde-job-echo:v0-latest",
            "docker_run_cmd": [],
            "docker_run_options_preset": 'none',
            "volume": {
                "volume_type": "zip_url",
                "contents": zip_url,
            },
            "job_uuid": job_uuid,
        }),
    ]))
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


def test_zip_url_too_big_volume_should_fail(httpx_mock: HTTPXMock, settings):
    settings.VOLUME_MAX_SIZE_BYTES = 1

    zip_url = 'https://localhost/payload.txt'
    httpx_mock.add_response(url=zip_url, content=zip_contents)

    command = TestCommand(iter([
        json.dumps({
            "message_type": "V0PrepareJobRequest",
            "base_docker_image_name": "alpine",
            "timeout_seconds": None,
            "volume_type": "zip_url",
            "job_uuid": job_uuid,
        }),
        json.dumps({
            "message_type": "V0RunJobRequest",
            "docker_image_name": "backenddevelopersltd/compute-horde-job-echo:v0-latest",
            "docker_run_cmd": [],
            "docker_run_options_preset": 'none',
            "volume": {
                "volume_type": "zip_url",
                "contents": zip_url,
            },
            "job_uuid": job_uuid,
        }),
    ]))
    command.handle()
    assert [json.loads(msg) for msg in command.miner_client.ws.sent_messages] == [
        {
            "message_type": "V0ReadyRequest",
            "job_uuid": job_uuid,
        },
        {
            "message_type": "V0FailedRequest",
            "docker_process_exit_status": None,
            "timeout": False,
            "docker_process_stdout": "Input volume too large",
            "docker_process_stderr": "",
            "job_uuid": job_uuid,
        }
    ]


def test_zip_and_http_post_output_uploader(httpx_mock: HTTPXMock, tmp_path):
    # Arrange
    httpx_mock.add_response()
    url = 'http://localhost/bucket/file.zip?hash=blabla'
    form_fields = {'a': 'b', 'c': 'd'}

    command = TestCommand(iter([
        json.dumps({
            "message_type": "V0PrepareJobRequest",
            "base_docker_image_name": "alpine",
            "timeout_seconds": None,
            "volume_type": "inline",
            "job_uuid": job_uuid,
        }),
        json.dumps({
            "message_type": "V0RunJobRequest",
            "docker_image_name": "backenddevelopersltd/compute-horde-job-echo:v0-latest",
            "docker_run_cmd": [],
            "docker_run_options_preset": 'none',
            "volume": {
                "volume_type": "inline",
                "contents": base64_zipfile,
            },
            "output_upload": {
                "output_upload_type": "zip_and_http_post",
                "url": url,
                "form_fields": form_fields,
            },
            "job_uuid": job_uuid,
        }),
    ]))

    # Act
    command.handle()

    # Assert
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
        },
    ]

    request = httpx_mock.get_request()
    assert request is not None
    assert request.url == url
    assert request.method == 'POST'


def test_zip_and_http_put_output_uploader(httpx_mock: HTTPXMock, tmp_path):
    # Arrange
    httpx_mock.add_response()
    url = 'http://localhost/bucket/file.zip?hash=blabla'

    command = TestCommand(iter([
        json.dumps({
            "message_type": "V0PrepareJobRequest",
            "base_docker_image_name": "alpine",
            "timeout_seconds": None,
            "volume_type": "inline",
            "job_uuid": job_uuid,
        }),
        json.dumps({
            "message_type": "V0RunJobRequest",
            "docker_image_name": "backenddevelopersltd/compute-horde-job-echo:v0-latest",
            "docker_run_cmd": [],
            "docker_run_options_preset": 'none',
            "volume": {
                "volume_type": "inline",
                "contents": base64_zipfile,
            },
            "output_upload": {
                "output_upload_type": "zip_and_http_put",
                "url": url,
            },
            "job_uuid": job_uuid,
        }),
    ]))

    # Act
    command.handle()

    # Assert
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
        },
    ]

    request = httpx_mock.get_request()
    assert request is not None
    assert request.url == url
    assert request.method == 'PUT'


def test_output_upload_failed(httpx_mock: HTTPXMock, tmp_path):
    # Arrange
    httpx_mock.add_response(status_code=400)
    command = TestCommand(iter([
        json.dumps({
            "message_type": "V0PrepareJobRequest",
            "base_docker_image_name": "alpine",
            "timeout_seconds": None,
            "volume_type": "inline",
            "job_uuid": job_uuid,
        }),
        json.dumps({
            "message_type": "V0RunJobRequest",
            "docker_image_name": "backenddevelopersltd/compute-horde-job-echo:v0-latest",
            "docker_run_cmd": [],
            "docker_run_options_preset": 'none',
            "volume": {
                "volume_type": "inline",
                "contents": base64_zipfile,
            },
            "output_upload": {
                "output_upload_type": "zip_and_http_post",
                "url": "http://localhost",
                "form_fields": {},
            },
            "job_uuid": job_uuid,
        }),
    ]))

    # Act
    command.handle()

    # Assert
    assert [json.loads(msg) for msg in command.miner_client.ws.sent_messages] == [
        {
            "message_type": "V0ReadyRequest",
            "job_uuid": job_uuid,
        },
        {
            "message_type": "V0FailedRequest",
            "docker_process_exit_status": mock.ANY,
            "timeout": mock.ANY,
            "docker_process_stdout": ContainsStr("Uploading output failed"),
            "docker_process_stderr": "",
            "job_uuid": job_uuid,
        },
    ]


def test_raw_script_job():
    command = TestCommand(iter([
        json.dumps({
            "message_type": "V0PrepareJobRequest",
            "base_docker_image_name": None,
            "timeout_seconds": None,
            "volume_type": "inline",
            "job_uuid": job_uuid,
        }),
        json.dumps({
            "message_type": "V0RunJobRequest",
            "docker_image_name": None,
            "raw_script": f"print('{payload}')",
            "docker_run_cmd": [],
            "docker_run_options_preset": 'none',
            "volume": {
                "volume_type": "inline",
                "contents": base64_zipfile,
            },
            "job_uuid": job_uuid,
        }),
    ]))
    command.handle()
    assert [json.loads(msg) for msg in command.miner_client.ws.sent_messages] == [
        {
            "message_type": "V0ReadyRequest",
            "job_uuid": job_uuid,
        },
        {
            "message_type": "V0FinishedRequest",
            "docker_process_stdout": f"{payload}\n",
            "docker_process_stderr": mock.ANY,
            "job_uuid": job_uuid,
        }
    ]
