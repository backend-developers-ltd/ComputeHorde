import asyncio
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
from compute_horde.protocol_messages import V0JobFailedRequest
from compute_horde.transport import StubTransport
from pytest_httpx import HTTPXMock
from requests_toolbelt.multipart import decoder

from compute_horde_executor.executor.management.commands.run_executor import (
    Command,
    JobRunner,
    MinerClient,
)

payload = "".join(random.choice(string.ascii_uppercase + string.digits) for _ in range(32))


def mock_download(local_dir, **kwargs):
    with open(local_dir / "payload.txt", "w") as file:
        file.write(payload)


def mock_download_failure(local_dir, **kwargs):
    raise RuntimeError("Download failed")


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

    async def run_nvidia_toolkit_version_check_or_fail(self):
        is_toolkit_installed = None

        try:
            process = await asyncio.create_subprocess_exec(
                "nvidia-container-toolkit",
                "--version",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        except OSError:
            is_toolkit_installed = False

        if is_toolkit_installed is None:
            try:
                stdout, stderr = await asyncio.wait_for(process.communicate(), 5)
            except TimeoutError:
                process.kill()
                is_toolkit_installed = False

        if is_toolkit_installed is None and process.returncode != 0:
            is_toolkit_installed = False

        if is_toolkit_installed is None:
            is_toolkit_installed = True

        if is_toolkit_installed:
            return await super().run_nvidia_toolkit_version_check_or_fail()
        else:
            logger.warning(
                "NVIDIA Container Toolkit not installed - skipping safe toolkit version check in tests"
            )
            return True


def test_main_loop_basic():
    job_container_name = f"ch-{uuid.uuid4()}-job"
    nginx_container_name = f"ch-{uuid.uuid4()}-nginx"
    network_name = f"ch-{uuid.uuid4()}"
    subprocess.check_output(["docker", "network", "create", "--internal", network_name])
    for container_name in [job_container_name, nginx_container_name]:
        subprocess.check_output(
            [
                "docker",
                "run",
                "-d",
                "--network",
                "bridge",
                "--name",
                container_name,
                "busybox",
                "sleep",
                "1000",
            ]
        )
        subprocess.check_output(
            [
                "docker",
                "network",
                "connect",
                network_name,
                container_name,
            ]
        )
    for container_name in [job_container_name, nginx_container_name]:
        output = subprocess.check_output(["docker", "ps", "--filter", f"name={container_name}"])
        assert container_name.encode() in output

    command = CommandTested(
        iter(
            [
                json.dumps(
                    {
                        "message_type": "V0InitialJobRequest",
                        "executor_class": "spin_up-4min.gpu-24gb",
                        "docker_image": "backenddevelopersltd/compute-horde-job-echo:v0-latest",
                        "timeout_seconds": 10,
                        "volume_type": "inline",
                        "job_uuid": job_uuid,
                        "job_started_receipt_payload": {
                            "job_uuid": job_uuid,
                            "miner_hotkey": "miner_hotkey",
                            "validator_hotkey": "validator_hotkey",
                            "timestamp": "2025-01-01T00:00:00+00:00",
                            "executor_class": "spin_up-4min.gpu-24gb",
                            "is_organic": True,
                            "ttl": 5,
                        },
                        "job_started_receipt_signature": "blah",
                    }
                ),
                json.dumps(
                    {
                        "message_type": "V0JobRequest",
                        "executor_class": "spin_up-4min.gpu-24gb",
                        "docker_image": "backenddevelopersltd/compute-horde-job-echo:v0-latest",
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
            "message_type": "V0ExecutorReadyRequest",
            "executor_token": None,
            "job_uuid": job_uuid,
        },
        {
            "message_type": "V0MachineSpecsRequest",
            "specs": mock.ANY,
            "job_uuid": job_uuid,
        },
        {
            "message_type": "V0JobFinishedRequest",
            "docker_process_stdout": payload,
            "docker_process_stderr": mock.ANY,
            "artifacts": {},
            "job_uuid": job_uuid,
        },
    ]

    for container_name in [job_container_name, nginx_container_name]:
        output = subprocess.check_output(["docker", "ps", "--filter", f"name={container_name}"])
        assert container_name.encode() not in output

    output = subprocess.check_output(
        ["docker", "network", "ls", "--filter", f"name={network_name}"]
    )
    assert network_name.encode() not in output


def test_main_loop_streaming_job():
    _, public_key, _ = generate_certificate_at()
    command = CommandTested(
        iter(
            [
                json.dumps(
                    {
                        "message_type": "V0InitialJobRequest",
                        "executor_class": "spin_up-4min.gpu-24gb",
                        "docker_image": "backenddevelopersltd/compute-horde-streaming-job-test:v0-latest",
                        "timeout_seconds": 10,
                        "job_uuid": job_uuid,
                        "job_started_receipt_payload": {
                            "job_uuid": job_uuid,
                            "miner_hotkey": "miner_hotkey",
                            "validator_hotkey": "validator_hotkey",
                            "timestamp": "2025-01-01T00:00:00+00:00",
                            "executor_class": "spin_up-4min.gpu-24gb",
                            "is_organic": True,
                            "ttl": 5,
                        },
                        "job_started_receipt_signature": "blah",
                        "streaming_details": {
                            "public_key": public_key,
                            "executor_ip": "127.0.0.1",
                        },
                    }
                ),
                json.dumps(
                    {
                        "message_type": "V0JobRequest",
                        "executor_class": "spin_up-4min.gpu-24gb",
                        "docker_image": "backenddevelopersltd/compute-horde-streaming-job-test:v0-latest",
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
            "message_type": "V0ExecutorReadyRequest",
            "executor_token": None,
            "job_uuid": job_uuid,
        },
        {
            "message_type": "V0StreamingJobReadyRequest",
            "job_uuid": job_uuid,
            "executor_token": None,
            "public_key": mock.ANY,
            "ip": None,
            "port": mock.ANY,
            "miner_signature": None,
        },
        {
            "message_type": "V0MachineSpecsRequest",
            "specs": mock.ANY,
            "job_uuid": job_uuid,
        },
        {
            "message_type": "V0JobFinishedRequest",
            "docker_process_stdout": mock.ANY,
            "docker_process_stderr": mock.ANY,
            "artifacts": {},
            "job_uuid": job_uuid,
        },
    ]


def test_huggingface_volume():
    # Arrange
    repo_id = "huggingface/model"
    revision = "main"

    with patch("huggingface_hub.snapshot_download", side_effect=mock_download):
        command = CommandTested(
            iter(
                [
                    json.dumps(
                        {
                            "message_type": "V0InitialJobRequest",
                            "executor_class": "spin_up-4min.gpu-24gb",
                            "docker_image": "backenddevelopersltd/compute-horde-job-echo:v0-latest",
                            "timeout_seconds": 10,
                            "volume_type": "huggingface_volume",
                            "job_uuid": job_uuid,
                            "job_started_receipt_payload": {
                                "job_uuid": job_uuid,
                                "miner_hotkey": "miner_hotkey",
                                "validator_hotkey": "validator_hotkey",
                                "timestamp": "2025-01-01T00:00:00+00:00",
                                "executor_class": "spin_up-4min.gpu-24gb",
                                "is_organic": True,
                                "ttl": 5,
                            },
                            "job_started_receipt_signature": "blah",
                        }
                    ),
                    json.dumps(
                        {
                            "message_type": "V0JobRequest",
                            "executor_class": "spin_up-4min.gpu-24gb",
                            "docker_image": "backenddevelopersltd/compute-horde-job-echo:v0-latest",
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
            "message_type": "V0ExecutorReadyRequest",
            "executor_token": None,
            "job_uuid": job_uuid,
        },
        {
            "message_type": "V0MachineSpecsRequest",
            "specs": mock.ANY,
            "job_uuid": job_uuid,
        },
        {
            "message_type": "V0JobFinishedRequest",
            "docker_process_stdout": payload,
            "docker_process_stderr": mock.ANY,
            "artifacts": {},
            "job_uuid": job_uuid,
        },
    ]


def test_huggingface_volume_failure():
    # Arrange
    repo_id = "huggingface/model"
    revision = "main"

    with patch(
        "huggingface_hub.snapshot_download", side_effect=mock_download_failure
    ) as mock_snapshot_download:
        command = CommandTested(
            iter(
                [
                    json.dumps(
                        {
                            "message_type": "V0InitialJobRequest",
                            "executor_class": "spin_up-4min.gpu-24gb",
                            "docker_image": "backenddevelopersltd/compute-horde-job-echo:v0-latest",
                            "timeout_seconds": 10,
                            "volume_type": "huggingface_volume",
                            "job_uuid": job_uuid,
                            "job_started_receipt_payload": {
                                "job_uuid": job_uuid,
                                "miner_hotkey": "miner_hotkey",
                                "validator_hotkey": "validator_hotkey",
                                "timestamp": "2025-01-01T00:00:00+00:00",
                                "executor_class": "spin_up-4min.gpu-24gb",
                                "is_organic": True,
                                "ttl": 5,
                            },
                            "job_started_receipt_signature": "blah",
                        }
                    ),
                    json.dumps(
                        {
                            "message_type": "V0JobRequest",
                            "executor_class": "spin_up-4min.gpu-24gb",
                            "docker_image": "backenddevelopersltd/compute-horde-job-echo:v0-latest",
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
            "message_type": "V0ExecutorReadyRequest",
            "executor_token": None,
            "job_uuid": job_uuid,
        },
        {
            "message_type": "V0JobFailedRequest",
            "docker_process_exit_status": None,
            "docker_process_stdout": "Failed to download model from Hugging Face after 3 retries: Download failed",
            "docker_process_stderr": "",
            "error_type": V0JobFailedRequest.ErrorType.HUGGINGFACE_DOWNLOAD.value,
            "error_detail": "Download failed",
            "timeout": False,
            "job_uuid": job_uuid,
        },
    ]

    assert mock_snapshot_download.call_count == 3


def test_huggingface_volume_fail_and_retry():
    # Arrange
    repo_id = "huggingface/model"
    revision = "main"

    first_try = True

    def side_effect(*args, **kwargs):
        nonlocal first_try
        if first_try:
            first_try = False
            mock_download_failure(*args, **kwargs)
        else:
            mock_download(*args, **kwargs)

    with patch(
        "huggingface_hub.snapshot_download", side_effect=side_effect
    ) as mock_snapshot_download:
        command = CommandTested(
            iter(
                [
                    json.dumps(
                        {
                            "message_type": "V0InitialJobRequest",
                            "executor_class": "spin_up-4min.gpu-24gb",
                            "docker_image": "backenddevelopersltd/compute-horde-job-echo:v0-latest",
                            "timeout_seconds": 10,
                            "volume_type": "huggingface_volume",
                            "job_uuid": job_uuid,
                            "job_started_receipt_payload": {
                                "job_uuid": job_uuid,
                                "miner_hotkey": "miner_hotkey",
                                "validator_hotkey": "validator_hotkey",
                                "timestamp": "2025-01-01T00:00:00+00:00",
                                "executor_class": "spin_up-4min.gpu-24gb",
                                "max_timeout": 10,
                                "is_organic": True,
                                "ttl": 5,
                            },
                            "job_started_receipt_signature": "blah",
                        }
                    ),
                    json.dumps(
                        {
                            "message_type": "V0JobRequest",
                            "executor_class": "spin_up-4min.gpu-24gb",
                            "docker_image": "backenddevelopersltd/compute-horde-job-echo:v0-latest",
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
            "message_type": "V0ExecutorReadyRequest",
            "executor_token": None,
            "job_uuid": job_uuid,
        },
        {
            "message_type": "V0MachineSpecsRequest",
            "specs": mock.ANY,
            "job_uuid": job_uuid,
        },
        {
            "message_type": "V0JobFinishedRequest",
            "docker_process_stdout": payload,
            "docker_process_stderr": mock.ANY,
            "artifacts": {},
            "job_uuid": job_uuid,
        },
    ]

    assert mock_snapshot_download.call_count == 2


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
        "huggingface_hub.snapshot_download", side_effect=mock_download
    ) as mock_snapshot_download:
        command = CommandTested(
            iter(
                [
                    json.dumps(
                        {
                            "message_type": "V0InitialJobRequest",
                            "executor_class": "spin_up-4min.gpu-24gb",
                            "docker_image": "backenddevelopersltd/compute-horde-job-echo:v0-latest",
                            "timeout_seconds": 10,
                            "volume_type": "huggingface_volume",
                            "job_uuid": job_uuid,
                            "job_started_receipt_payload": {
                                "job_uuid": job_uuid,
                                "miner_hotkey": "miner_hotkey",
                                "validator_hotkey": "validator_hotkey",
                                "timestamp": "2025-01-01T00:00:00+00:00",
                                "executor_class": "spin_up-4min.gpu-24gb",
                                "is_organic": True,
                                "ttl": 5,
                            },
                            "job_started_receipt_signature": "blah",
                        }
                    ),
                    json.dumps(
                        {
                            "message_type": "V0JobRequest",
                            "executor_class": "spin_up-4min.gpu-24gb",
                            "docker_image": "backenddevelopersltd/compute-horde-job-echo:v0-latest",
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
            "message_type": "V0ExecutorReadyRequest",
            "executor_token": None,
            "job_uuid": job_uuid,
        },
        {
            "message_type": "V0MachineSpecsRequest",
            "specs": mock.ANY,
            "job_uuid": job_uuid,
        },
        {
            "message_type": "V0JobFinishedRequest",
            "docker_process_stdout": payload,
            "docker_process_stderr": mock.ANY,
            "artifacts": {},
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
                        "message_type": "V0InitialJobRequest",
                        "executor_class": "spin_up-4min.gpu-24gb",
                        "docker_image": "backenddevelopersltd/compute-horde-job-echo:v0-latest",
                        "timeout_seconds": 10,
                        "volume_type": "zip_url",
                        "job_uuid": job_uuid,
                        "job_started_receipt_payload": {
                            "job_uuid": job_uuid,
                            "miner_hotkey": "miner_hotkey",
                            "validator_hotkey": "validator_hotkey",
                            "timestamp": "2025-01-01T00:00:00+00:00",
                            "executor_class": "spin_up-4min.gpu-24gb",
                            "is_organic": True,
                            "ttl": 5,
                        },
                        "job_started_receipt_signature": "blah",
                    }
                ),
                json.dumps(
                    {
                        "message_type": "V0JobRequest",
                        "executor_class": "spin_up-4min.gpu-24gb",
                        "docker_image": "backenddevelopersltd/compute-horde-job-echo:v0-latest",
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
            "message_type": "V0ExecutorReadyRequest",
            "executor_token": None,
            "job_uuid": job_uuid,
        },
        {
            "message_type": "V0MachineSpecsRequest",
            "specs": mock.ANY,
            "job_uuid": job_uuid,
        },
        {
            "message_type": "V0JobFinishedRequest",
            "docker_process_stdout": payload,
            "docker_process_stderr": mock.ANY,
            "artifacts": {},
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
                        "message_type": "V0InitialJobRequest",
                        "executor_class": "spin_up-4min.gpu-24gb",
                        "docker_image": "backenddevelopersltd/compute-horde-job-echo:v0-latest",
                        "timeout_seconds": 10,
                        "volume_type": "zip_url",
                        "job_uuid": job_uuid,
                        "job_started_receipt_payload": {
                            "job_uuid": job_uuid,
                            "miner_hotkey": "miner_hotkey",
                            "validator_hotkey": "validator_hotkey",
                            "timestamp": "2025-01-01T00:00:00+00:00",
                            "executor_class": "spin_up-4min.gpu-24gb",
                            "is_organic": True,
                            "ttl": 5,
                        },
                        "job_started_receipt_signature": "blah",
                    }
                ),
                json.dumps(
                    {
                        "message_type": "V0JobRequest",
                        "executor_class": "spin_up-4min.gpu-24gb",
                        "docker_image": "backenddevelopersltd/compute-horde-job-echo:v0-latest",
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
            "message_type": "V0ExecutorReadyRequest",
            "executor_token": None,
            "job_uuid": job_uuid,
        },
        {
            "message_type": "V0JobFailedRequest",
            "docker_process_exit_status": None,
            "timeout": False,
            "docker_process_stdout": "Input volume too large",
            "docker_process_stderr": "",
            "error_type": None,
            "error_detail": None,
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
                        "message_type": "V0InitialJobRequest",
                        "executor_class": "spin_up-4min.gpu-24gb",
                        "docker_image": "backenddevelopersltd/compute-horde-job-echo:v0-latest",
                        "timeout_seconds": 10,
                        "volume_type": "zip_url",
                        "job_uuid": job_uuid,
                        "job_started_receipt_payload": {
                            "job_uuid": job_uuid,
                            "miner_hotkey": "miner_hotkey",
                            "validator_hotkey": "validator_hotkey",
                            "timestamp": "2025-01-01T00:00:00+00:00",
                            "executor_class": "spin_up-4min.gpu-24gb",
                            "is_organic": True,
                            "ttl": 5,
                        },
                        "job_started_receipt_signature": "blah",
                    }
                ),
                json.dumps(
                    {
                        "message_type": "V0JobRequest",
                        "executor_class": "spin_up-4min.gpu-24gb",
                        "docker_image": "backenddevelopersltd/compute-horde-job-echo:v0-latest",
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
            "message_type": "V0ExecutorReadyRequest",
            "executor_token": None,
            "job_uuid": job_uuid,
        },
        {
            "message_type": "V0MachineSpecsRequest",
            "specs": mock.ANY,
            "job_uuid": job_uuid,
        },
        {
            "message_type": "V0JobFinishedRequest",
            "docker_process_stdout": payload,
            "docker_process_stderr": mock.ANY,
            "artifacts": {},
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
                        "message_type": "V0InitialJobRequest",
                        "executor_class": "spin_up-4min.gpu-24gb",
                        "docker_image": "backenddevelopersltd/compute-horde-job-echo:v0-latest",
                        "timeout_seconds": 10,
                        "volume_type": "zip_url",
                        "job_uuid": job_uuid,
                        "job_started_receipt_payload": {
                            "job_uuid": job_uuid,
                            "miner_hotkey": "miner_hotkey",
                            "validator_hotkey": "validator_hotkey",
                            "timestamp": "2025-01-01T00:00:00+00:00",
                            "executor_class": "spin_up-4min.gpu-24gb",
                            "is_organic": True,
                            "ttl": 5,
                        },
                        "job_started_receipt_signature": "blah",
                    }
                ),
                json.dumps(
                    {
                        "message_type": "V0JobRequest",
                        "executor_class": "spin_up-4min.gpu-24gb",
                        "docker_image": "backenddevelopersltd/compute-horde-job-echo:v0-latest",
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
            "message_type": "V0ExecutorReadyRequest",
            "executor_token": None,
            "job_uuid": job_uuid,
        },
        {
            "message_type": "V0JobFailedRequest",
            "docker_process_exit_status": None,
            "timeout": False,
            "docker_process_stdout": "Input volume too large",
            "docker_process_stderr": "",
            "error_type": None,
            "error_detail": None,
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
                        "message_type": "V0InitialJobRequest",
                        "executor_class": "spin_up-4min.gpu-24gb",
                        "docker_image": "backenddevelopersltd/compute-horde-job-echo:v0-latest",
                        "timeout_seconds": 10,
                        "volume_type": "inline",
                        "job_uuid": job_uuid,
                        "job_started_receipt_payload": {
                            "job_uuid": job_uuid,
                            "miner_hotkey": "miner_hotkey",
                            "validator_hotkey": "validator_hotkey",
                            "timestamp": "2025-01-01T00:00:00+00:00",
                            "executor_class": "spin_up-4min.gpu-24gb",
                            "is_organic": True,
                            "ttl": 5,
                        },
                        "job_started_receipt_signature": "blah",
                    }
                ),
                json.dumps(
                    {
                        "message_type": "V0JobRequest",
                        "executor_class": "spin_up-4min.gpu-24gb",
                        "docker_image": "backenddevelopersltd/compute-horde-job-echo:v0-latest",
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
            "message_type": "V0ExecutorReadyRequest",
            "executor_token": None,
            "job_uuid": job_uuid,
        },
        {
            "message_type": "V0MachineSpecsRequest",
            "specs": mock.ANY,
            "job_uuid": job_uuid,
        },
        {
            "message_type": "V0JobFinishedRequest",
            "docker_process_stdout": payload,
            "docker_process_stderr": mock.ANY,
            "artifacts": {},
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
                        "message_type": "V0InitialJobRequest",
                        "executor_class": "spin_up-4min.gpu-24gb",
                        "docker_image": "backenddevelopersltd/compute-horde-job-echo:v0-latest",
                        "timeout_seconds": 10,
                        "volume_type": "inline",
                        "job_uuid": job_uuid,
                        "job_started_receipt_payload": {
                            "job_uuid": job_uuid,
                            "miner_hotkey": "miner_hotkey",
                            "validator_hotkey": "validator_hotkey",
                            "timestamp": "2025-01-01T00:00:00+00:00",
                            "executor_class": "spin_up-4min.gpu-24gb",
                            "is_organic": True,
                            "ttl": 5,
                        },
                        "job_started_receipt_signature": "blah",
                    }
                ),
                json.dumps(
                    {
                        "message_type": "V0JobRequest",
                        "executor_class": "spin_up-4min.gpu-24gb",
                        "docker_image": "backenddevelopersltd/compute-horde-job-echo:v0-latest",
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
            "message_type": "V0ExecutorReadyRequest",
            "executor_token": None,
            "job_uuid": job_uuid,
        },
        {
            "message_type": "V0MachineSpecsRequest",
            "specs": mock.ANY,
            "job_uuid": job_uuid,
        },
        {
            "message_type": "V0JobFinishedRequest",
            "docker_process_stdout": payload,
            "docker_process_stderr": mock.ANY,
            "artifacts": {},
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
                        "message_type": "V0InitialJobRequest",
                        "executor_class": "spin_up-4min.gpu-24gb",
                        "docker_image": "backenddevelopersltd/compute-horde-job-echo:v0-latest",
                        "timeout_seconds": 10,
                        "volume_type": "inline",
                        "job_uuid": job_uuid,
                        "job_started_receipt_payload": {
                            "job_uuid": job_uuid,
                            "miner_hotkey": "miner_hotkey",
                            "validator_hotkey": "validator_hotkey",
                            "timestamp": "2025-01-01T00:00:00+00:00",
                            "executor_class": "spin_up-4min.gpu-24gb",
                            "is_organic": True,
                            "ttl": 5,
                        },
                        "job_started_receipt_signature": "blah",
                    }
                ),
                json.dumps(
                    {
                        "message_type": "V0JobRequest",
                        "executor_class": "spin_up-4min.gpu-24gb",
                        "docker_image": "backenddevelopersltd/compute-horde-job-echo:v0-latest",
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
            "message_type": "V0ExecutorReadyRequest",
            "executor_token": None,
            "job_uuid": job_uuid,
        },
        {
            "message_type": "V0JobFailedRequest",
            "docker_process_exit_status": mock.ANY,
            "timeout": mock.ANY,
            "docker_process_stdout": ContainsStr("Uploading output failed"),
            "docker_process_stderr": "",
            "error_type": None,
            "error_detail": None,
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
                        "message_type": "V0InitialJobRequest",
                        "executor_class": "spin_up-4min.gpu-24gb",
                        "docker_image": "backenddevelopersltd/compute-horde-job-echo:v0-latest",
                        "timeout_seconds": 10,
                        "volume_type": "inline",
                        "job_uuid": job_uuid,
                        "job_started_receipt_payload": {
                            "job_uuid": job_uuid,
                            "miner_hotkey": "miner_hotkey",
                            "validator_hotkey": "validator_hotkey",
                            "timestamp": "2025-01-01T00:00:00+00:00",
                            "executor_class": "spin_up-4min.gpu-24gb",
                            "is_organic": True,
                            "ttl": 5,
                        },
                        "job_started_receipt_signature": "blah",
                    }
                ),
                json.dumps(
                    {
                        "message_type": "V0JobRequest",
                        "executor_class": "spin_up-4min.gpu-24gb",
                        "docker_image": "backenddevelopersltd/compute-horde-job-echo:v0-latest",
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
            "message_type": "V0ExecutorReadyRequest",
            "executor_token": None,
            "job_uuid": job_uuid,
        },
        {
            "message_type": "V0MachineSpecsRequest",
            "specs": mock.ANY,
            "job_uuid": job_uuid,
        },
        {
            "message_type": "V0JobFinishedRequest",
            "docker_process_stdout": payload,
            "docker_process_stderr": mock.ANY,
            "artifacts": {},
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
                        "message_type": "V0InitialJobRequest",
                        "executor_class": "spin_up-4min.gpu-24gb",
                        "docker_image": "python:3.11-slim",
                        "timeout_seconds": 10,
                        "volume_type": "inline",
                        "job_uuid": job_uuid,
                        "job_started_receipt_payload": {
                            "job_uuid": job_uuid,
                            "miner_hotkey": "miner_hotkey",
                            "validator_hotkey": "validator_hotkey",
                            "timestamp": "2025-01-01T00:00:00+00:00",
                            "executor_class": "spin_up-4min.gpu-24gb",
                            "is_organic": True,
                            "ttl": 5,
                        },
                        "job_started_receipt_signature": "blah",
                    }
                ),
                json.dumps(
                    {
                        "message_type": "V0JobRequest",
                        "executor_class": "spin_up-4min.gpu-24gb",
                        "docker_image": "python:3.11-slim",
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
            "message_type": "V0ExecutorReadyRequest",
            "executor_token": None,
            "job_uuid": job_uuid,
        },
        {
            "message_type": "V0MachineSpecsRequest",
            "specs": mock.ANY,
            "job_uuid": job_uuid,
        },
        {
            "message_type": "V0JobFinishedRequest",
            "docker_process_stdout": f"{payload}\n",
            "docker_process_stderr": mock.ANY,
            "artifacts": {},
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
                        "message_type": "V0InitialJobRequest",
                        "executor_class": "spin_up-4min.gpu-24gb",
                        "docker_image": "backenddevelopersltd/compute-horde-job-echo:v0-latest",
                        "timeout_seconds": 10,
                        "volume_type": "inline",
                        "job_uuid": job_uuid,
                        "job_started_receipt_payload": {
                            "job_uuid": job_uuid,
                            "miner_hotkey": "miner_hotkey",
                            "validator_hotkey": "validator_hotkey",
                            "timestamp": "2025-01-01T00:00:00+00:00",
                            "executor_class": "spin_up-4min.gpu-24gb",
                            "is_organic": True,
                            "ttl": 5,
                        },
                        "job_started_receipt_signature": "blah",
                    }
                ),
                json.dumps(
                    {
                        "message_type": "V0JobRequest",
                        "executor_class": "spin_up-4min.gpu-24gb",
                        "docker_image": "backenddevelopersltd/compute-horde-job-echo:v0-latest",
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
            "message_type": "V0ExecutorReadyRequest",
            "executor_token": None,
            "job_uuid": job_uuid,
        },
        {
            "message_type": "V0MachineSpecsRequest",
            "specs": mock.ANY,
            "job_uuid": job_uuid,
        },
        {
            "message_type": "V0JobFinishedRequest",
            "docker_process_stdout": payload,
            "docker_process_stderr": mock.ANY,
            "artifacts": {},
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
                        "message_type": "V0InitialJobRequest",
                        "executor_class": "spin_up-4min.gpu-24gb",
                        "docker_image": "backenddevelopersltd/compute-horde-job-echo:v0-latest",
                        "timeout_seconds": 10,
                        "volume_type": "single_file",
                        "job_uuid": job_uuid,
                        "job_started_receipt_payload": {
                            "job_uuid": job_uuid,
                            "miner_hotkey": "miner_hotkey",
                            "validator_hotkey": "validator_hotkey",
                            "timestamp": "2025-01-01T00:00:00+00:00",
                            "executor_class": "spin_up-4min.gpu-24gb",
                            "is_organic": True,
                            "ttl": 5,
                        },
                        "job_started_receipt_signature": "blah",
                    }
                ),
                json.dumps(
                    {
                        "message_type": "V0JobRequest",
                        "executor_class": "spin_up-4min.gpu-24gb",
                        "docker_image": "backenddevelopersltd/compute-horde-job-echo:v0-latest",
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
            "message_type": "V0ExecutorReadyRequest",
            "executor_token": None,
            "job_uuid": job_uuid,
        },
        {
            "message_type": "V0MachineSpecsRequest",
            "specs": mock.ANY,
            "job_uuid": job_uuid,
        },
        {
            "message_type": "V0JobFinishedRequest",
            "docker_process_stdout": payload,
            "docker_process_stderr": mock.ANY,
            "artifacts": {},
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
                        "message_type": "V0InitialJobRequest",
                        "executor_class": "spin_up-4min.gpu-24gb",
                        "docker_image": "backenddevelopersltd/compute-horde-job-echo:v0-latest",
                        "timeout_seconds": 10,
                        "volume_type": "multi_volume",
                        "job_uuid": job_uuid,
                        "job_started_receipt_payload": {
                            "job_uuid": job_uuid,
                            "miner_hotkey": "miner_hotkey",
                            "validator_hotkey": "validator_hotkey",
                            "timestamp": "2025-01-01T00:00:00+00:00",
                            "executor_class": "spin_up-4min.gpu-24gb",
                            "is_organic": True,
                            "ttl": 5,
                        },
                        "job_started_receipt_signature": "blah",
                    }
                ),
                json.dumps(
                    {
                        "message_type": "V0JobRequest",
                        "executor_class": "spin_up-4min.gpu-24gb",
                        "docker_image": "backenddevelopersltd/compute-horde-job-echo:v0-latest",
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
            "message_type": "V0ExecutorReadyRequest",
            "executor_token": None,
            "job_uuid": job_uuid,
        },
        {
            "message_type": "V0MachineSpecsRequest",
            "specs": mock.ANY,
            "job_uuid": job_uuid,
        },
        {
            "message_type": "V0JobFinishedRequest",
            "docker_process_stdout": payload,
            "docker_process_stderr": mock.ANY,
            "artifacts": {},
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


def test_artifacts():
    original_JobRunner_prepare = JobRunner.prepare_initial

    async def patch_JobRunner_prepare(self):
        await original_JobRunner_prepare(self)

        with open(self.artifacts_mount_dir / "empty", "wb") as f:
            pass

        with open(self.artifacts_mount_dir / "space", "wb") as f:
            f.write(b" ")

        with open(self.artifacts_mount_dir / "small.txt", "wb") as f:
            f.write(b"artifact 2\nsecond line\nx=1,y=2\n")

        with open(self.artifacts_mount_dir / "data.json", "wb") as f:
            f.write(b'{"a": 1, b: [2, 3]}')

        with open(self.artifacts_mount_dir / "large artifact.bin", "wb") as f:
            f.write(b"x" * 999_000)

        with open(self.artifacts_mount_dir / "very-large.bin", "wb") as f:
            f.write(b"x" * 1_000_000)

    with patch(
        "compute_horde_executor.executor.management.commands.run_executor.JobRunner.prepare",
        new=patch_JobRunner_prepare,
    ):
        command = CommandTested(
            iter(
                [
                    json.dumps(
                        {
                            "message_type": "V0InitialJobRequest",
                            "executor_class": "spin_up-4min.gpu-24gb",
                            "docker_image": "backenddevelopersltd/compute-horde-job-echo:v0-latest",
                            "timeout_seconds": 10,
                            "volume_type": "inline",
                            "job_uuid": job_uuid,
                            "job_started_receipt_payload": {
                                "job_uuid": job_uuid,
                                "miner_hotkey": "miner_hotkey",
                                "validator_hotkey": "validator_hotkey",
                                "timestamp": "2025-01-01T00:00:00+00:00",
                                "executor_class": "spin_up-4min.gpu-24gb",
                                "is_organic": True,
                                "ttl": 5,
                            },
                            "job_started_receipt_signature": "blah",
                        }
                    ),
                    json.dumps(
                        {
                            "message_type": "V0JobRequest",
                            "executor_class": "spin_up-4min.gpu-24gb",
                            "docker_image": "backenddevelopersltd/compute-horde-job-echo:v0-latest",
                            "docker_run_cmd": [],
                            "docker_run_options_preset": "none",
                            "job_uuid": job_uuid,
                            "volume": {
                                "volume_type": "inline",
                                "contents": base64_zipfile,
                            },
                            "artifacts_dir": "/artifacts",
                        }
                    ),
                ]
            )
        )

        # Act
        command.handle()

    all_bytes = b"".join(bytes([i]) for i in range(256))

    # Assert
    assert [json.loads(msg) for msg in command.miner_client_for_tests.transport.sent_messages] == [
        {
            "message_type": "V0ExecutorReadyRequest",
            "executor_token": None,
            "job_uuid": job_uuid,
        },
        {
            "message_type": "V0MachineSpecsRequest",
            "specs": mock.ANY,
            "job_uuid": job_uuid,
        },
        {
            "message_type": "V0JobFinishedRequest",
            "docker_process_stdout": payload,
            "docker_process_stderr": "",
            "artifacts": {
                "/artifacts/empty": "",
                "/artifacts/space": "IA==",
                "/artifacts/small.txt": "YXJ0aWZhY3QgMgpzZWNvbmQgbGluZQp4PTEseT0yCg==",
                "/artifacts/data.json": "eyJhIjogMSwgYjogWzIsIDNdfQ==",
                "/artifacts/large artifact.bin": base64.b64encode(b"x" * 999_000).decode(),
                # very large artifact is not included
                # "/artifacts/very-large.bin"
                # the following are written by the compute-horde-job-echo image:
                "/artifacts/empty.bin": "",
                "/artifacts/All-BYTES.bin": base64.b64encode(all_bytes).decode(),
                "/artifacts/text.txt": "SSBhbSBMTE0sIHlvdXIgQUkgYXNzaXN0YW50Cg==",
                "/artifacts/100k zeros": base64.b64encode(b"\x00" * 100_000).decode(),
            },
            "job_uuid": job_uuid,
        },
    ]
