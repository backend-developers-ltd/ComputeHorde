import base64
import io
import json
import logging
import random
import string
import subprocess
import uuid
import zipfile
from functools import partial
from unittest import mock
from unittest.mock import patch

import httpx
from compute_horde.certificate import generate_certificate_at
from compute_horde.transport import StubTransport
from pytest_httpx import HTTPXMock
from requests_toolbelt.multipart import decoder

from compute_horde_executor.executor.management.commands.run_executor import Command, MinerClient

payload = "".join(random.choice(string.ascii_uppercase + string.digits) for _ in range(32))


def mock_download(local_dir, **kwargs):
    with open(local_dir / "payload.txt", "w") as file:
        file.write(payload)


in_memory_output = io.BytesIO()
zipf = zipfile.ZipFile(in_memory_output, "w")
zipf.writestr("payload.txt", payload)
zipf.close()
in_memory_output.seek(0)
zip_contents = in_memory_output.read()
base64_zipfile = base64.b64encode(zip_contents).decode()

job_uuid = str(uuid.uuid4())
logger = logging.getLogger(__name__)


class ContainsStr:
    def __init__(self, contained: str) -> None:
        self.contained = contained

    def __eq__(self, other):
        return self.contained in other


def get_file_from_request(request):
    multipart_data = decoder.MultipartDecoder(request.content, request.headers["Content-Type"])
    parsed_data = {}

    for part in multipart_data.parts:
        header_disposition = part.headers.get(b"Content-Disposition", b"").decode()
        if 'name="file"' in header_disposition:
            file_content = part.content
            parsed_data["file"] = file_content
        else:
            header_content_type = part.headers.get(b"Content-Type", b"").decode()
            parsed_data[header_content_type] = part.text

    return parsed_data


class CommandTested(Command):
    def __init__(self, messages, *args, **kwargs):
        transport = StubTransport("test", messages)
        self.MINER_CLIENT_CLASS = partial(MinerClient, transport=transport)
        super().__init__(*args, **kwargs)


def test_main_loop():
    job_container_name = f'ch-{uuid.uuid4()}-job'
    nginx_container_name = f'ch-{uuid.uuid4()}-nginx'
    for container_name in [job_container_name, nginx_container_name]:
        subprocess.check_output([
            'docker',
            'run',
            '-d',
            '--name',
            container_name,
            'busybox',
            'sleep',
            '1000',
        ])
    for container_name in [job_container_name, nginx_container_name]:
        output = subprocess.check_output(['docker', 'ps', '--filter', f'name={container_name}'])
        assert container_name.encode() in output

    command = CommandTested(
        iter(
            [
                json.dumps(
                    {
                        "message_type": "V0PrepareJobRequest",
                        "base_docker_image_name": "backenddevelopersltd/compute-horde-job-echo:v0-latest",
                        "timeout_seconds": None,
                        "volume_type": "inline",
                        "job_uuid": job_uuid,
                    }
                ),
                json.dumps(
                    {
                        "message_type": "V0RunJobRequest",
                        "docker_image_name": "backenddevelopersltd/compute-horde-job-echo:v0-latest",
                        "docker_run_cmd": [],
                        "docker_run_options_preset": "none",
                        "volume": {
                            "volume_type": "inline",
                            "contents": base64_zipfile,
                        },
                        "job_uuid": job_uuid,
                    }
                ),
            ]
        )
    )
    command.handle()
    assert [json.loads(msg) for msg in command.miner_client_for_tests.transport.sent_messages] == [
        {
            "message_type": "V0ReadyRequest",
            "job_uuid": job_uuid,
        },
        {
            "message_type": "V0MachineSpecsRequest",
            "specs": mock.ANY,
            "job_uuid": job_uuid,
        },
        {
            "message_type": "V0FinishedRequest",
            "docker_process_stdout": payload,
            "docker_process_stderr": mock.ANY,
            "job_uuid": job_uuid,
        },
    ]

    for container_name in [job_container_name, nginx_container_name]:
        output = subprocess.check_output(['docker', 'ps', '--filter', f'name={container_name}'])
        assert container_name.encode() not in output


def test_main_loop_streaming_job():
    _, public_key, _ = generate_certificate_at()
    command = CommandTested(
        iter(
            [
                json.dumps(
                    {
                        "message_type": "V1PrepareJobRequest",
                        "docker_image_name": "backenddevelopersltd/compute-horde-streaming-job-test:v0-latest",
                        "timeout_seconds": 10,
                        "job_uuid": job_uuid,
                        "public_key": public_key,
                        "executor_ip": "127.0.0.1",
                    }
                ),
                json.dumps(
                    {
                        "message_type": "V0RunJobRequest",
                        "docker_image_name": "backenddevelopersltd/compute-horde-streaming-job-test:v0-latest",
                        "docker_run_cmd": ["python", "./mock_streaming_job.py", "autostart"],
                        "docker_run_options_preset": "none",
                        "job_uuid": job_uuid,
                    }
                ),
            ]
        )
    )
    command.handle()
    assert [json.loads(msg) for msg in command.miner_client_for_tests.transport.sent_messages] == [
        {
            "message_type": "V0ReadyRequest",
            "job_uuid": job_uuid,
        },
        {
            "message_type": "V0StreamingJobReadyRequest",
            "job_uuid": job_uuid,
            "public_key": mock.ANY,
            "port": mock.ANY,
        },
        {
            "message_type": "V0MachineSpecsRequest",
            "specs": mock.ANY,
            "job_uuid": job_uuid,
        },
        {
            "message_type": "V0FinishedRequest",
            "docker_process_stdout": mock.ANY,
            "docker_process_stderr": mock.ANY,
            "job_uuid": job_uuid,
        },
    ]


def test_huggingface_volume():
    # Arrange
    repo_id = "huggingface/model"
    revision = "main"

    with patch(
        "compute_horde_executor.executor.management.commands.run_executor.snapshot_download",
        mock_download,
    ):
        command = CommandTested(
            iter(
                [
                    json.dumps(
                        {
                            "message_type": "V0PrepareJobRequest",
                            "base_docker_image_name": "backenddevelopersltd/compute-horde-job-echo:v0-latest",
                            "timeout_seconds": None,
                            "volume_type": "huggingface_volume",
                            "job_uuid": job_uuid,
                        }
                    ),
                    json.dumps(
                        {
                            "message_type": "V0RunJobRequest",
                            "docker_image_name": "backenddevelopersltd/compute-horde-job-echo:v0-latest",
                            "docker_run_cmd": [],
                            "docker_run_options_preset": "none",
                            "volume": {
                                "volume_type": "huggingface_volume",
                                "repo_id": repo_id,
                                "revision": revision,
                            },
                            "job_uuid": job_uuid,
                        }
                    ),
                ]
            )
        )

        # Act
        command.handle()

    # Assert
    assert [json.loads(msg) for msg in command.miner_client_for_tests.transport.sent_messages] == [
        {
            "message_type": "V0ReadyRequest",
            "job_uuid": job_uuid,
        },
        {
            "message_type": "V0MachineSpecsRequest",
            "specs": mock.ANY,
            "job_uuid": job_uuid,
        },
        {
            "message_type": "V0FinishedRequest",
            "docker_process_stdout": payload,
            "docker_process_stderr": mock.ANY,
            "job_uuid": job_uuid,
        },
    ]


def test_huggingface_volume_dataset():
    # Arrange
    repo_id = "huggingface/dataset"
    revision = "main"
    repo_type = "dataset"
    file_patterns = [
        "default/train/001/01JJK16EFPA7HWY3Z7MWZ4A6N9.parquet",
        "default/train/003/01JJK12V8K1A65RD75NSWRGECK.parquet",
        "default/train/003/01JJKJ49N4NBSS3YJG35XQ9XPB.parquet",
        "default/train/004/01JJKB151AFCB1TGJDCXCTBZPW.parquet",
        "default/train/004/01JJKCK5DPEH61SBY8MC2NXTRM.parquet",
        "default/train/004/01JJKNQADWRJYBPKKZGJHSKRSC.parquet",
        "default/train/005/01JJKQ9QGPBV5KW18ZSM3VBTSX.parquet",
        "default/train/008/01JJK3G51DHYK1N0JHPJQS3GFR.parquet",
    ]

    with patch(
        "compute_horde_executor.executor.management.commands.run_executor.snapshot_download",
        side_effect=mock_download,
    ) as mock_snapshot_download:
        command = CommandTested(
            iter(
                [
                    json.dumps(
                        {
                            "message_type": "V0PrepareJobRequest",
                            "base_docker_image_name": "backenddevelopersltd/compute-horde-job-echo:v0-latest",
                            "timeout_seconds": None,
                            "volume_type": "huggingface_volume",
                            "job_uuid": job_uuid,
                        }
                    ),
                    json.dumps(
                        {
                            "message_type": "V0RunJobRequest",
                            "docker_image_name": "backenddevelopersltd/compute-horde-job-echo:v0-latest",
                            "docker_run_cmd": [],
                            "docker_run_options_preset": "none",
                            "volume": {
                                "volume_type": "huggingface_volume",
                                "repo_id": repo_id,
                                "repo_type": repo_type,
                                "revision": revision,
                                "allow_patterns": file_patterns,
                            },
                            "job_uuid": job_uuid,
                        }
                    ),
                ]
            )
        )

        # Act
        command.handle()

    # Assert
    assert [json.loads(msg) for msg in command.miner_client_for_tests.transport.sent_messages] == [
        {
            "message_type": "V0ReadyRequest",
            "job_uuid": job_uuid,
        },
        {
            "message_type": "V0MachineSpecsRequest",
            "specs": mock.ANY,
            "job_uuid": job_uuid,
        },
        {
            "message_type": "V0FinishedRequest",
            "docker_process_stdout": payload,
            "docker_process_stderr": mock.ANY,
            "job_uuid": job_uuid,
        },
    ]

    _, kwargs = mock_snapshot_download.call_args
    assert kwargs["repo_id"] == repo_id
    assert kwargs["revision"] == revision
    assert kwargs["repo_type"] == repo_type
    assert kwargs["allow_patterns"] == file_patterns


def test_zip_url_volume(httpx_mock: HTTPXMock):
    zip_url = "https://localhost/payload.txt"
    httpx_mock.add_response(url=zip_url, content=zip_contents)

    command = CommandTested(
        iter(
            [
                json.dumps(
                    {
                        "message_type": "V0PrepareJobRequest",
                        "base_docker_image_name": "backenddevelopersltd/compute-horde-job-echo:v0-latest",
                        "timeout_seconds": None,
                        "volume_type": "zip_url",
                        "job_uuid": job_uuid,
                    }
                ),
                json.dumps(
                    {
                        "message_type": "V0RunJobRequest",
                        "docker_image_name": "backenddevelopersltd/compute-horde-job-echo:v0-latest",
                        "docker_run_cmd": [],
                        "docker_run_options_preset": "none",
                        "volume": {
                            "volume_type": "zip_url",
                            "contents": zip_url,
                        },
                        "job_uuid": job_uuid,
                    }
                ),
            ]
        )
    )
    command.handle()
    assert [json.loads(msg) for msg in command.miner_client_for_tests.transport.sent_messages] == [
        {
            "message_type": "V0ReadyRequest",
            "job_uuid": job_uuid,
        },
        {
            "message_type": "V0MachineSpecsRequest",
            "specs": mock.ANY,
            "job_uuid": job_uuid,
        },
        {
            "message_type": "V0FinishedRequest",
            "docker_process_stdout": payload,
            "docker_process_stderr": mock.ANY,
            "job_uuid": job_uuid,
        },
    ]


def test_zip_url_too_big_volume_should_fail(httpx_mock: HTTPXMock, settings):
    settings.VOLUME_MAX_SIZE_BYTES = 1

    zip_url = "https://localhost/payload.txt"
    httpx_mock.add_response(url=zip_url, content=zip_contents)

    command = CommandTested(
        iter(
            [
                json.dumps(
                    {
                        "message_type": "V0PrepareJobRequest",
                        "base_docker_image_name": "backenddevelopersltd/compute-horde-job-echo:v0-latest",
                        "timeout_seconds": None,
                        "volume_type": "zip_url",
                        "job_uuid": job_uuid,
                    }
                ),
                json.dumps(
                    {
                        "message_type": "V0RunJobRequest",
                        "docker_image_name": "backenddevelopersltd/compute-horde-job-echo:v0-latest",
                        "docker_run_cmd": [],
                        "docker_run_options_preset": "none",
                        "volume": {
                            "volume_type": "zip_url",
                            "contents": zip_url,
                        },
                        "job_uuid": job_uuid,
                    }
                ),
            ]
        )
    )
    command.handle()
    assert [json.loads(msg) for msg in command.miner_client_for_tests.transport.sent_messages] == [
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
        },
    ]


def test_zip_url_volume_without_content_length(httpx_mock: HTTPXMock):
    zip_url = "https://localhost/payload.txt"

    def response_callback(request: httpx.Request) -> httpx.Response:
        response = httpx.Response(
            status_code=200,
            extensions={"http_version": b"HTTP/1.1"},
            content=zip_contents,
        )
        del response.headers["Content-Length"]
        return response

    httpx_mock.add_callback(response_callback, url=zip_url)

    command = CommandTested(
        iter(
            [
                json.dumps(
                    {
                        "message_type": "V0PrepareJobRequest",
                        "base_docker_image_name": "backenddevelopersltd/compute-horde-job-echo:v0-latest",
                        "timeout_seconds": None,
                        "volume_type": "zip_url",
                        "job_uuid": job_uuid,
                    }
                ),
                json.dumps(
                    {
                        "message_type": "V0RunJobRequest",
                        "docker_image_name": "backenddevelopersltd/compute-horde-job-echo:v0-latest",
                        "docker_run_cmd": [],
                        "docker_run_options_preset": "none",
                        "volume": {
                            "volume_type": "zip_url",
                            "contents": zip_url,
                        },
                        "job_uuid": job_uuid,
                    }
                ),
            ]
        )
    )
    command.handle()
    assert [json.loads(msg) for msg in command.miner_client_for_tests.transport.sent_messages] == [
        {
            "message_type": "V0ReadyRequest",
            "job_uuid": job_uuid,
        },
        {
            "message_type": "V0MachineSpecsRequest",
            "specs": mock.ANY,
            "job_uuid": job_uuid,
        },
        {
            "message_type": "V0FinishedRequest",
            "docker_process_stdout": payload,
            "docker_process_stderr": mock.ANY,
            "job_uuid": job_uuid,
        },
    ]


def test_zip_url_too_big_volume_without_content_length_should_fail(httpx_mock: HTTPXMock, settings):
    settings.VOLUME_MAX_SIZE_BYTES = 1

    zip_url = "https://localhost/payload.txt"

    def response_callback(request: httpx.Request) -> httpx.Response:
        response = httpx.Response(
            status_code=200,
            extensions={"http_version": "HTTP/1.1".encode("ascii")},
            content=zip_contents,
        )
        del response.headers["Content-Length"]
        return response

    httpx_mock.add_callback(response_callback, url=zip_url)

    command = CommandTested(
        iter(
            [
                json.dumps(
                    {
                        "message_type": "V0PrepareJobRequest",
                        "base_docker_image_name": "backenddevelopersltd/compute-horde-job-echo:v0-latest",
                        "timeout_seconds": None,
                        "volume_type": "zip_url",
                        "job_uuid": job_uuid,
                    }
                ),
                json.dumps(
                    {
                        "message_type": "V0RunJobRequest",
                        "docker_image_name": "backenddevelopersltd/compute-horde-job-echo:v0-latest",
                        "docker_run_cmd": [],
                        "docker_run_options_preset": "none",
                        "volume": {
                            "volume_type": "zip_url",
                            "contents": zip_url,
                        },
                        "job_uuid": job_uuid,
                    }
                ),
            ]
        )
    )
    command.handle()
    assert [json.loads(msg) for msg in command.miner_client_for_tests.transport.sent_messages] == [
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
        },
    ]


def test_zip_and_http_post_output_uploader(httpx_mock: HTTPXMock, tmp_path):
    # Arrange
    httpx_mock.add_response()
    url = "http://localhost/bucket/file.zip?hash=blabla"
    form_fields = {"a": "b", "c": "d"}

    command = CommandTested(
        iter(
            [
                json.dumps(
                    {
                        "message_type": "V0PrepareJobRequest",
                        "base_docker_image_name": "backenddevelopersltd/compute-horde-job-echo:v0-latest",
                        "timeout_seconds": None,
                        "volume_type": "inline",
                        "job_uuid": job_uuid,
                    }
                ),
                json.dumps(
                    {
                        "message_type": "V0RunJobRequest",
                        "docker_image_name": "backenddevelopersltd/compute-horde-job-echo:v0-latest",
                        "docker_run_cmd": [],
                        "docker_run_options_preset": "none",
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
                    }
                ),
            ]
        )
    )

    # Act
    command.handle()

    # Assert
    assert [json.loads(msg) for msg in command.miner_client_for_tests.transport.sent_messages] == [
        {
            "message_type": "V0ReadyRequest",
            "job_uuid": job_uuid,
        },
        {
            "message_type": "V0MachineSpecsRequest",
            "specs": mock.ANY,
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
    assert request.method == "POST"


def test_zip_and_http_put_output_uploader(httpx_mock: HTTPXMock, tmp_path):
    # Arrange
    httpx_mock.add_response()
    url = "http://localhost/bucket/file.zip?hash=blabla"

    command = CommandTested(
        iter(
            [
                json.dumps(
                    {
                        "message_type": "V0PrepareJobRequest",
                        "base_docker_image_name": "backenddevelopersltd/compute-horde-job-echo:v0-latest",
                        "timeout_seconds": None,
                        "volume_type": "inline",
                        "job_uuid": job_uuid,
                    }
                ),
                json.dumps(
                    {
                        "message_type": "V0RunJobRequest",
                        "docker_image_name": "backenddevelopersltd/compute-horde-job-echo:v0-latest",
                        "docker_run_cmd": [],
                        "docker_run_options_preset": "none",
                        "volume": {
                            "volume_type": "inline",
                            "contents": base64_zipfile,
                        },
                        "output_upload": {
                            "output_upload_type": "zip_and_http_put",
                            "url": url,
                        },
                        "job_uuid": job_uuid,
                    }
                ),
            ]
        )
    )

    # Act
    command.handle()

    # Assert
    assert [json.loads(msg) for msg in command.miner_client_for_tests.transport.sent_messages] == [
        {
            "message_type": "V0ReadyRequest",
            "job_uuid": job_uuid,
        },
        {
            "message_type": "V0MachineSpecsRequest",
            "specs": mock.ANY,
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
    assert request.method == "PUT"


def test_output_upload_failed(httpx_mock: HTTPXMock, tmp_path):
    # Arrange
    httpx_mock.add_response(status_code=400)
    command = CommandTested(
        iter(
            [
                json.dumps(
                    {
                        "message_type": "V0PrepareJobRequest",
                        "base_docker_image_name": "backenddevelopersltd/compute-horde-job-echo:v0-latest",
                        "timeout_seconds": None,
                        "volume_type": "inline",
                        "job_uuid": job_uuid,
                    }
                ),
                json.dumps(
                    {
                        "message_type": "V0RunJobRequest",
                        "docker_image_name": "backenddevelopersltd/compute-horde-job-echo:v0-latest",
                        "docker_run_cmd": [],
                        "docker_run_options_preset": "none",
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
                    }
                ),
            ]
        )
    )

    # Act
    command.handle()

    # Assert
    assert [json.loads(msg) for msg in command.miner_client_for_tests.transport.sent_messages] == [
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


def test_output_upload_retry(httpx_mock: HTTPXMock, tmp_path):
    # Arrange
    httpx_mock.add_response(status_code=400)
    httpx_mock.add_response(status_code=400)
    httpx_mock.add_response(status_code=200)
    command = CommandTested(
        iter(
            [
                json.dumps(
                    {
                        "message_type": "V0PrepareJobRequest",
                        "base_docker_image_name": "backenddevelopersltd/compute-horde-job-echo:v0-latest",
                        "timeout_seconds": None,
                        "volume_type": "inline",
                        "job_uuid": job_uuid,
                    }
                ),
                json.dumps(
                    {
                        "message_type": "V0RunJobRequest",
                        "docker_image_name": "backenddevelopersltd/compute-horde-job-echo:v0-latest",
                        "docker_run_cmd": [],
                        "docker_run_options_preset": "none",
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
                    }
                ),
            ]
        )
    )

    # Act
    command.handle()

    # Assert
    assert [json.loads(msg) for msg in command.miner_client_for_tests.transport.sent_messages] == [
        {
            "message_type": "V0ReadyRequest",
            "job_uuid": job_uuid,
        },
        {
            "message_type": "V0MachineSpecsRequest",
            "specs": mock.ANY,
            "job_uuid": job_uuid,
        },
        {
            "message_type": "V0FinishedRequest",
            "docker_process_stdout": payload,
            "docker_process_stderr": mock.ANY,
            "job_uuid": job_uuid,
        },
    ]

    requests = httpx_mock.get_requests()
    assert len(requests) == 3
    for request in requests:
        assert request is not None
        assert request.url == "http://localhost"
        assert request.method == "POST"


def test_raw_script_job():
    command = CommandTested(
        iter(
            [
                json.dumps(
                    {
                        "message_type": "V0PrepareJobRequest",
                        "base_docker_image_name": None,
                        "timeout_seconds": None,
                        "volume_type": "inline",
                        "job_uuid": job_uuid,
                    }
                ),
                json.dumps(
                    {
                        "message_type": "V0RunJobRequest",
                        "docker_image_name": None,
                        "raw_script": f"print('{payload}')",
                        "docker_run_cmd": [],
                        "docker_run_options_preset": "none",
                        "volume": {
                            "volume_type": "inline",
                            "contents": base64_zipfile,
                        },
                        "job_uuid": job_uuid,
                    }
                ),
            ]
        )
    )
    command.handle()
    assert [json.loads(msg) for msg in command.miner_client_for_tests.transport.sent_messages] == [
        {
            "message_type": "V0ReadyRequest",
            "job_uuid": job_uuid,
        },
        {
            "message_type": "V0MachineSpecsRequest",
            "specs": mock.ANY,
            "job_uuid": job_uuid,
        },
        {
            "message_type": "V0FinishedRequest",
            "docker_process_stdout": f"{payload}\n",
            "docker_process_stderr": mock.ANY,
            "job_uuid": job_uuid,
        },
    ]


def test_multi_upload_output_uploader_with_system_output(httpx_mock: HTTPXMock, tmp_path):
    async def read_file_callback(request: httpx.Request, *args, **kwargs) -> httpx.Response:
        # Read the content of the file-like object, it should be stored then in `content`
        await request.aread()
        return httpx.Response(status_code=200)

    httpx_mock.add_callback(callback=read_file_callback)
    url1 = "http://localhost/bucket/file1.txt"
    url2 = "http://localhost/bucket/file2.txt"
    system_output_url = "http://localhost/bucket/system_output.zip"
    relative_path1 = "file1.txt"
    relative_path2 = "file2.txt"

    command = CommandTested(
        iter(
            [
                json.dumps(
                    {
                        "message_type": "V0PrepareJobRequest",
                        "base_docker_image_name": "backenddevelopersltd/compute-horde-job-echo:v0-latest",
                        "timeout_seconds": None,
                        "volume_type": "inline",
                        "job_uuid": job_uuid,
                    }
                ),
                json.dumps(
                    {
                        "message_type": "V0RunJobRequest",
                        "docker_image_name": "backenddevelopersltd/compute-horde-job-echo:v0-latest",
                        "docker_run_cmd": [],
                        "docker_run_options_preset": "none",
                        "volume": {
                            "volume_type": "inline",
                            "contents": base64_zipfile,
                        },
                        "output_upload": {
                            "output_upload_type": "multi_upload",
                            "uploads": [
                                {
                                    "output_upload_type": "single_file_post",
                                    "url": url1,
                                    "relative_path": relative_path1,
                                },
                                {
                                    "output_upload_type": "single_file_put",
                                    "url": url2,
                                    "relative_path": relative_path2,
                                },
                            ],
                            "system_output": {
                                "output_upload_type": "zip_and_http_post",
                                "url": system_output_url,
                            },
                        },
                        "job_uuid": job_uuid,
                    }
                ),
            ]
        )
    )

    # Act
    command.handle()

    # Assert
    assert [json.loads(msg) for msg in command.miner_client_for_tests.transport.sent_messages] == [
        {
            "message_type": "V0ReadyRequest",
            "job_uuid": job_uuid,
        },
        {
            "message_type": "V0MachineSpecsRequest",
            "specs": mock.ANY,
            "job_uuid": job_uuid,
        },
        {
            "message_type": "V0FinishedRequest",
            "docker_process_stdout": payload,
            "docker_process_stderr": mock.ANY,
            "job_uuid": job_uuid,
        },
    ]

    request1 = httpx_mock.get_request(url=url1)
    assert request1 is not None
    assert request1.url == url1
    assert request1.method == "POST"
    # Assert file1.txt content
    parsed_file1 = get_file_from_request(request1)
    assert parsed_file1["file"] == b"4 // chosen by fair dice roll. guaranteed to be random :D\n"

    request2 = httpx_mock.get_request(url=url2)
    assert request2 is not None
    assert request2.url == url2
    assert request2.method == "PUT"
    # Assert file2.txt content
    # parsed_file2 = get_file_from_request(request2)
    assert request2.content == b"5 // chosen by fair dice roll. guaranteed to be random :D\n"

    system_output_request = httpx_mock.get_request(url=system_output_url)
    assert system_output_request is not None
    assert system_output_request.url == system_output_url
    assert system_output_request.method == "POST"

    parsed_file3 = get_file_from_request(system_output_request)

    # Extract and assert system_output.zip content
    with zipfile.ZipFile(io.BytesIO(parsed_file3["file"]), "r") as zip_file:
        assert set(zip_file.namelist()) == {"stdout.txt", "stderr.txt"}
        with zip_file.open("stdout.txt") as stdout_file:
            assert stdout_file.read().decode() == payload


def test_single_file_volume(httpx_mock: HTTPXMock, tmp_path):
    # Arrange
    httpx_mock.add_response(text=payload)
    url = "http://localhost/bucket/payload.txt"
    relative_path = "payload.txt"

    command = CommandTested(
        iter(
            [
                json.dumps(
                    {
                        "message_type": "V0PrepareJobRequest",
                        "base_docker_image_name": "backenddevelopersltd/compute-horde-job-echo:v0-latest",
                        "timeout_seconds": None,
                        "volume_type": "single_file",
                        "job_uuid": job_uuid,
                    }
                ),
                json.dumps(
                    {
                        "message_type": "V0RunJobRequest",
                        "docker_image_name": "backenddevelopersltd/compute-horde-job-echo:v0-latest",
                        "docker_run_cmd": [],
                        "docker_run_options_preset": "none",
                        "job_uuid": job_uuid,
                        "volume": {
                            "volume_type": "single_file",
                            "url": url,
                            "relative_path": relative_path,
                        },
                    }
                ),
            ]
        )
    )

    # Act
    command.handle()

    # Assert
    assert [json.loads(msg) for msg in command.miner_client_for_tests.transport.sent_messages] == [
        {
            "message_type": "V0ReadyRequest",
            "job_uuid": job_uuid,
        },
        {
            "message_type": "V0MachineSpecsRequest",
            "specs": mock.ANY,
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
    assert request.method == "GET"


def test_multi_volume(httpx_mock: HTTPXMock, tmp_path):
    # Arrange
    httpx_mock.add_response(text=payload)
    url1 = "http://localhost/bucket/file1.zip"
    url2 = "http://localhost/bucket/file2.zip"
    url3 = "http://localhost/bucket/payload.txt"
    relative_path1 = "input/file1.zip"
    relative_path2 = "input/file2.zip"
    relative_path3 = "payload.txt"

    command = CommandTested(
        iter(
            [
                json.dumps(
                    {
                        "message_type": "V0PrepareJobRequest",
                        "base_docker_image_name": "backenddevelopersltd/compute-horde-job-echo:v0-latest",
                        "timeout_seconds": None,
                        "volume_type": "multi_volume",
                        "job_uuid": job_uuid,
                    }
                ),
                json.dumps(
                    {
                        "message_type": "V0RunJobRequest",
                        "docker_image_name": "backenddevelopersltd/compute-horde-job-echo:v0-latest",
                        "docker_run_cmd": [],
                        "docker_run_options_preset": "none",
                        "job_uuid": job_uuid,
                        "volume": {
                            "volume_type": "multi_volume",
                            "volumes": [
                                {
                                    "volume_type": "single_file",
                                    "url": url1,
                                    "relative_path": relative_path1,
                                },
                                {
                                    "volume_type": "single_file",
                                    "url": url2,
                                    "relative_path": relative_path2,
                                },
                                {
                                    "volume_type": "single_file",
                                    "url": url3,
                                    "relative_path": relative_path3,
                                },
                            ],
                        },
                    }
                ),
            ]
        )
    )

    # Act
    command.handle()

    # Assert
    assert [json.loads(msg) for msg in command.miner_client_for_tests.transport.sent_messages] == [
        {
            "message_type": "V0ReadyRequest",
            "job_uuid": job_uuid,
        },
        {
            "message_type": "V0MachineSpecsRequest",
            "specs": mock.ANY,
            "job_uuid": job_uuid,
        },
        {
            "message_type": "V0FinishedRequest",
            "docker_process_stdout": payload,
            "docker_process_stderr": mock.ANY,
            "job_uuid": job_uuid,
        },
    ]
    print([json.loads(msg) for msg in command.miner_client_for_tests.transport.sent_messages])

    request1 = httpx_mock.get_request(url=url1)
    assert request1 is not None
    assert request1.url == url1
    assert request1.method == "GET"

    request2 = httpx_mock.get_request(url=url2)
    assert request2 is not None
    assert request2.url == url2
    assert request2.method == "GET"

    request2 = httpx_mock.get_request(url=url3)
    assert request2 is not None
    assert request2.url == url3
    assert request2.method == "GET"
