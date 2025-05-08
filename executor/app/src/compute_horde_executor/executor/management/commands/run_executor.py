import asyncio
import base64
import csv
import logging
import os
import pathlib
import re
import shlex
import shutil
import subprocess
import tempfile
import typing
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any, cast

import packaging.version
import pydantic
from asgiref.sync import async_to_sync
from compute_horde.base.docker import DockerRunOptionsPreset
from compute_horde.certificate import (
    check_endpoint,
    generate_certificate_at,
    get_docker_container_ip,
    save_public_key,
    start_nginx,
)
from compute_horde.miner_client.base import (
    AbstractMinerClient,
    UnsupportedMessageReceived,
)
from compute_horde.protocol_messages import (
    ExecutorToMinerMessage,
    GenericError,
    MinerToExecutorMessage,
    V0ExecutionDoneRequest,
    V0ExecutorReadyRequest,
    V0InitialJobRequest,
    V0JobFailedRequest,
    V0JobFinishedRequest,
    V0JobRequest,
    V0MachineSpecsRequest,
    V0StreamingJobNotReadyRequest,
    V0StreamingJobReadyRequest,
    V0VolumesReadyRequest,
)
from compute_horde.transport import AbstractTransport, WSTransport
from compute_horde.utils import MachineSpecs, Timer
from compute_horde_core.output_upload import OutputUploader, OutputUploadFailed
from compute_horde_core.volume import (
    HuggingfaceVolume,
    Volume,
    VolumeDownloader,
    VolumeDownloadFailed,
)
from django.conf import settings
from django.core.management.base import BaseCommand
from pydantic import TypeAdapter

logger = logging.getLogger(__name__)

MAX_RESULT_SIZE_IN_RESPONSE = 1_000_000  # 1 MB
TRUNCATED_RESPONSE_PREFIX_LEN = MAX_RESULT_SIZE_IN_RESPONSE // 2
TRUNCATED_RESPONSE_SUFFIX_LEN = MAX_RESULT_SIZE_IN_RESPONSE // 2
MAX_ARTIFACT_SIZE = 1_000_000
DOCKER_STOP_TIMEOUT_SECONDS = 15
INPUT_VOLUME_UNPACK_TIMEOUT_SECONDS = 60 * 15

CVE_2022_0492_TIMEOUT_SECONDS = 120
CVE_2022_0492_IMAGE = (
    "us-central1-docker.pkg.dev/twistlock-secresearch/public/can-ctr-escape-cve-2022-0492:latest"
)
# Previous CVE: CVE-2024-0132 fixed in 1.16.2
# Current CVE: CVE-2025-23359 fixed in 1.17.4
NVIDIA_CONTAINER_TOOLKIT_MINIMUM_SAFE_VERSION = packaging.version.parse("1.17.4")
NVIDIA_CONTAINER_TOOLKIT_VERSION_CHECK_TIMEOUT = 120

NGINX_CONF = """
http {
    server {
        listen 443 ssl;
        server_name localhost;

        ssl_certificate /etc/nginx/ssl/certificate.pem;
        ssl_certificate_key /etc/nginx/ssl/private_key.pem;

        ssl_client_certificate /etc/nginx/ssl/client.crt;
        ssl_verify_client on;

        location / {
            if ($ssl_client_verify != SUCCESS) { return 403; }
            proxy_pass http://CONTAINER;
        }
    }

    server {
        # this port is not public - only executor connects to it
        listen 80;
        server_name localhost;

        # for checking if job is ready to serve requests
        location /health {
            proxy_pass http://CONTAINER/health;
        }

        # for checking if nginx is running
        location /ok { return 200; }
    }
}

events {
    worker_connections 1024;
}
"""

JOB_CONTAINER_PORT = 8000
WAIT_FOR_STREAMING_JOB_TIMEOUT = 25
WAIT_FOR_NGINX_TIMEOUT = 10


class JobError(Exception):
    def __init__(
        self,
        error_message: str,
        error_type: V0JobFailedRequest.ErrorType | None = None,
        error_detail: str | None = None,
    ):
        self.error_message = error_message
        self.error_type = error_type
        self.error_detail = error_detail


@asynccontextmanager
async def temporary_process(program, *args, clean_exit_timeout: float = 1.0, **subprocess_kwargs):
    """
    Context manager.
    Runs the program in a subprocess, yields it for you to interact with and cleans it up after the context exits.
    This will first try to stop the process nicely but kill it shortly after.

    Parameters:
        program: Program to execute
        *args: Program arguments
        clean_exit_timeout: Seconds to wait before force kill (default: 1.0)
        **subprocess_kwargs: Additional keyword arguments passed to asyncio.create_subprocess_exec()
    """
    process = await asyncio.create_subprocess_exec(program, *args, **subprocess_kwargs)
    try:
        yield process
    finally:
        try:
            try:
                process.terminate()
                await asyncio.wait_for(process.wait(), timeout=clean_exit_timeout)
            except ProcessLookupError:
                # Process already gone - nothing to do
                pass
            except TimeoutError:
                logger.warning(
                    f"Process `{program}` didn't exit after {clean_exit_timeout} seconds - killing ({args=})"
                )
                process.kill()
        except Exception as e:
            logger.error(f"Failed to clean up process `{program}` ({args=}): {e}", exc_info=True)


class RunConfigManager:
    @classmethod
    def preset_to_docker_run_args(cls, preset: DockerRunOptionsPreset) -> list[str]:
        if settings.DEBUG_NO_GPU_MODE:
            return []
        elif preset == "none":
            return []
        elif preset == "nvidia_all":
            return ["--runtime=nvidia", "--gpus", "all"]
        else:
            raise JobError(f"Invalid preset: {preset}")


class MinerClient(AbstractMinerClient[MinerToExecutorMessage, ExecutorToMinerMessage]):
    class NotInitialized(Exception):
        pass

    def __init__(self, miner_address: str, token: str, transport: AbstractTransport | None = None):
        self.miner_address = miner_address
        self.token = token
        transport = transport or WSTransport(miner_address, self.miner_url())
        super().__init__(miner_address, transport)

        self._maybe_job_uuid: str | None = None
        loop = asyncio.get_running_loop()
        self.initial_msg: asyncio.Future[V0InitialJobRequest] = loop.create_future()
        self.initial_msg_lock = asyncio.Lock()
        self.full_payload: asyncio.Future[V0JobRequest] = loop.create_future()
        self.full_payload_lock = asyncio.Lock()

    @property
    def job_uuid(self) -> str:
        if self._maybe_job_uuid is None:
            raise MinerClient.NotInitialized("Job UUID is missing")
        return self._maybe_job_uuid

    def miner_url(self) -> str:
        return f"{self.miner_address}/v0.1/executor_interface/{self.token}"

    def parse_message(self, raw_msg: str | bytes) -> MinerToExecutorMessage:
        return TypeAdapter(MinerToExecutorMessage).validate_json(raw_msg)

    async def handle_message(self, msg: MinerToExecutorMessage) -> None:
        if isinstance(msg, V0InitialJobRequest):
            await self.handle_initial_job_request(msg)
        elif isinstance(msg, V0JobRequest):
            await self.handle_job_request(msg)
        else:
            raise UnsupportedMessageReceived(msg)

    async def handle_initial_job_request(self, msg: V0InitialJobRequest):
        async with self.initial_msg_lock:
            if self.initial_msg.done():
                details = f"Received duplicate initial job request: first {self.job_uuid=} and then {msg.job_uuid=}"
                logger.error(details)
                await self.send_generic_error(details)
                return
            self._maybe_job_uuid = msg.job_uuid
            logger.debug(f"Received initial job request: {msg.job_uuid=}")
            self.initial_msg.set_result(msg)

    async def handle_job_request(self, msg: V0JobRequest):
        async with self.full_payload_lock:
            if not self.initial_msg.done():
                details = f"Received job request before an initial job request {msg.job_uuid=}"
                logger.error(details)
                await self.send_generic_error(details)
                return
            if self.full_payload.done():
                details = (
                    f"Received duplicate full job payload request: first "
                    f"{self.job_uuid=} and then {msg.job_uuid=}"
                )
                logger.error(details)
                await self.send_generic_error(details)
                return
            logger.debug(f"Received full job payload request: {msg.job_uuid=}")
            self.full_payload.set_result(msg)

    async def send_streaming_job_ready(self, certificate: str):
        self.deferred_send_model(
            V0StreamingJobReadyRequest(
                job_uuid=self.job_uuid, public_key=certificate, port=settings.NGINX_PORT
            )
        )

    async def send_volumes_ready(self):
        self.deferred_send_model(V0VolumesReadyRequest(job_uuid=self.job_uuid))

    async def send_execution_done(self):
        self.deferred_send_model(V0ExecutionDoneRequest(job_uuid=self.job_uuid))

    async def send_executor_ready(self):
        self.deferred_send_model(V0ExecutorReadyRequest(job_uuid=self.job_uuid))

    async def send_job_error(self, job_error: JobError):
        await self.send_model(
            V0JobFailedRequest(
                job_uuid=self.job_uuid,
                error_type=job_error.error_type,
                error_detail=f"{job_error.error_message} - {job_error.error_detail}",
                docker_process_stdout="",
                docker_process_stderr="",
            )
        )

    async def send_job_process_error(self, job_error: JobError, result: "_ExecutionResult"):
        pass

    async def send_result(self, job_result: "JobResult"):
        if job_result.specs:
            self.deferred_send_model(
                V0MachineSpecsRequest(
                    job_uuid=self.job_uuid,
                    specs=job_result.specs,
                )
            )
        await self.send_model(
            V0JobFinishedRequest(
                job_uuid=self.job_uuid,
                docker_process_stdout=job_result.stdout,
                docker_process_stderr=job_result.stderr,
                artifacts=job_result.artifacts,
            )
        )

    async def send_generic_error(self, details: str):
        await self.send_model(
            GenericError(
                details=details,
            )
        )

    async def send_streaming_job_failed_to_prepare(self):
        await self.send_model(
            V0StreamingJobNotReadyRequest(
                job_uuid=self.job_uuid,
            )
        )


class JobResult(pydantic.BaseModel):
    success: bool
    exit_status: int | None
    timeout: bool
    stdout: str
    stderr: str
    artifacts: dict[str, str]
    specs: MachineSpecs | None = None
    error_type: V0JobFailedRequest.ErrorType | None = None
    error_detail: str | None = None


def truncate(v: str) -> str:
    if len(v) > MAX_RESULT_SIZE_IN_RESPONSE:
        return f"{v[:TRUNCATED_RESPONSE_PREFIX_LEN]} ... {v[-TRUNCATED_RESPONSE_SUFFIX_LEN:]}"
    else:
        return v


def run_cmd(cmd):
    proc = subprocess.run(cmd, shell=True, capture_output=True, check=False, text=True)
    if proc.returncode != 0:
        raise RuntimeError(
            f"run_cmd error {cmd=!r} {proc.returncode=} {proc.stdout=!r} {proc.stderr=!r}"
        )
    return proc.stdout


@typing.no_type_check
def get_machine_specs() -> MachineSpecs:
    data = {}

    data["gpu"] = {"count": 0, "details": []}
    try:
        nvidia_cmd = run_cmd(
            "docker run --rm --runtime=nvidia --gpus all ubuntu "
            "nvidia-smi --query-gpu=name,driver_version,name,memory.total,compute_cap,power.limit,clocks.gr,clocks.mem,uuid,serial --format=csv"
        )
        csv_data = csv.reader(nvidia_cmd.splitlines())
        header = [x.strip() for x in next(csv_data)]
        for row in csv_data:
            row = [x.strip() for x in row]
            gpu_data = dict(zip(header, row))
            data["gpu"]["details"].append(
                {
                    "name": gpu_data["name"],
                    "driver": gpu_data["driver_version"],
                    "capacity": gpu_data["memory.total [MiB]"].split(" ")[0],
                    "cuda": gpu_data["compute_cap"],
                    "power_limit": gpu_data["power.limit [W]"].split(" ")[0],
                    "graphics_speed": gpu_data["clocks.current.graphics [MHz]"].split(" ")[0],
                    "memory_speed": gpu_data["clocks.current.memory [MHz]"].split(" ")[0],
                    "uuid": gpu_data["uuid"].split(" ")[0],
                    "serial": gpu_data["serial"].split(" ")[0],
                }
            )
        data["gpu"]["count"] = len(data["gpu"]["details"])
    except Exception as exc:
        # print(f'Error processing scraped gpu specs: {exc}', flush=True)
        data["gpu_scrape_error"] = repr(exc)

    data["cpu"] = {"count": 0, "model": "", "clocks": []}
    try:
        lscpu_output = run_cmd("lscpu")
        data["cpu"]["model"] = re.search(r"Model name:\s*(.*)$", lscpu_output, re.M).group(1)
        data["cpu"]["count"] = int(re.search(r"CPU\(s\):\s*(.*)", lscpu_output).group(1))

        cpu_data = run_cmd('lscpu --parse=MHZ | grep -Po "^[0-9,.]*$"').splitlines()
        data["cpu"]["clocks"] = [float(x) for x in cpu_data]
    except Exception as exc:
        # print(f'Error getting cpu specs: {exc}', flush=True)
        data["cpu_scrape_error"] = repr(exc)

    data["ram"] = {}
    try:
        with open("/proc/meminfo") as f:
            meminfo = f.read()

        for name, key in [
            ("MemAvailable", "available"),
            ("MemFree", "free"),
            ("MemTotal", "total"),
        ]:
            data["ram"][key] = int(re.search(rf"^{name}:\s*(\d+)\s+kB$", meminfo, re.M).group(1))
        data["ram"]["used"] = data["ram"]["total"] - data["ram"]["free"]
    except Exception as exc:
        # print(f"Error reading /proc/meminfo; Exc: {exc}", file=sys.stderr)
        data["ram_scrape_error"] = repr(exc)

    data["hard_disk"] = {}
    try:
        disk_usage = shutil.disk_usage(".")
        data["hard_disk"] = {
            "total": disk_usage.total // 1024,  # in kiB
            "used": disk_usage.used // 1024,
            "free": disk_usage.free // 1024,
        }
    except Exception as exc:
        # print(f"Error getting disk_usage from shutil: {exc}", file=sys.stderr)
        data["hard_disk_scrape_error"] = repr(exc)

    data["os"] = ""
    try:
        data["os"] = run_cmd('lsb_release -d | grep -Po "Description:\\s*\\K.*"').strip()
    except Exception as exc:
        # print(f'Error getting os specs: {exc}', flush=True)
        data["os_scrape_error"] = repr(exc)

    return MachineSpecs(specs=data)


def job_container_name(docker_objects_infix: str) -> str:
    return f"ch-{docker_objects_infix}-job"


def nginx_container_name(docker_objects_infix: str) -> str:
    return f"ch-{docker_objects_infix}-nginx"


def network_name(docker_objects_infix: str) -> str:
    return f"ch-{docker_objects_infix}"


class _ExecutionResult(typing.NamedTuple):
    """Exit output and status of the job's docker container."""

    timed_out: bool
    """Whether the job's docker container timed out and was forced to exit."""

    return_code: int | None
    """Status code returned by the job process. None means the job timed out and was stopped."""

    stdout: str
    stderr: str


class JobRunner:
    def __init__(self):
        self.initial_job_request: V0InitialJobRequest | None = None
        self.full_job_request: V0JobRequest | None = None

        self.temp_dir = pathlib.Path(tempfile.mkdtemp())
        self.volume_mount_dir = self.temp_dir / "volume"
        self.output_volume_mount_dir = self.temp_dir / "output"
        self.specs_volume_mount_dir = self.temp_dir / "specs"
        self.artifacts_mount_dir = self.temp_dir / "artifacts"

        self.job_container_name = job_container_name(settings.EXECUTOR_TOKEN)
        self.nginx_container_name = nginx_container_name(settings.EXECUTOR_TOKEN)
        self.job_network_name = network_name(settings.EXECUTOR_TOKEN)
        self.process: asyncio.subprocess.Process | None = None
        self.cmd: list[str] = []

        self.execution_result: _ExecutionResult | None = None

        # for streaming job
        self.is_streaming_job: bool = False
        self.nginx_dir_path: pathlib.Path | None = None
        self.executor_certificate: str | None = None

    async def cleanup_potential_old_jobs(self):
        await (
            await asyncio.create_subprocess_shell(
                f"docker kill $(docker ps -q --filter 'name={job_container_name('.*')}')"
            )
        ).communicate()
        await (
            await asyncio.create_subprocess_shell(
                f"docker kill $(docker ps -q --filter 'name={nginx_container_name('.*')}')"
            )
        ).communicate()
        await (
            await asyncio.create_subprocess_shell(
                f"docker network rm $(docker network ls -q --filter 'name={network_name('.*')}')"
            )
        ).communicate()

    async def prepare_initial(self, initial_job_request: V0InitialJobRequest):
        self.initial_job_request = initial_job_request
        if initial_job_request.streaming_details is not None:
            assert initial_job_request.streaming_details.executor_ip is not None
            self.nginx_dir_path, self.executor_certificate, _ = generate_certificate_at(
                alternative_name=initial_job_request.streaming_details.executor_ip
            )
            save_public_key(initial_job_request.streaming_details.public_key, self.nginx_dir_path)
            self.is_streaming_job = True

        self.volume_mount_dir.mkdir(exist_ok=True)
        self.output_volume_mount_dir.mkdir(exist_ok=True)
        self.artifacts_mount_dir.mkdir(exist_ok=True)

        await self.cleanup_potential_old_jobs()

        if self.initial_job_request.docker_image is not None:
            # TODO: TIMEOUTS - Check if this actually kills the process on timeout
            async with temporary_process(
                "docker",
                "pull",
                self.initial_job_request.docker_image,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            ) as docker_process:
                stdout, stderr = await docker_process.communicate()
                return_code = docker_process.returncode

            if return_code != 0:
                raise JobError(
                    f"Failed to pull docker image: exit code {return_code}",
                    error_detail=f'stdout="{stdout.decode()}"\nstderr="{stderr.decode()}',
                )

    async def prepare_full(self, full_job_request: V0JobRequest):
        self.full_job_request = full_job_request

    @asynccontextmanager
    async def start_job(self) -> AsyncGenerator[None, Any]:
        """
        Trigger a job to run in a Docker container.
        """
        assert self.initial_job_request is not None, (
            "Call prepare_initial() and prepare_full() first"
        )
        assert self.full_job_request is not None, "Call prepare_initial() and prepare_full() first"

        docker_run_options = RunConfigManager.preset_to_docker_run_args(
            self.full_job_request.docker_run_options_preset
        )

        docker_image = self.full_job_request.docker_image
        extra_volume_flags = []
        docker_run_cmd = self.full_job_request.docker_run_cmd

        if self.full_job_request.raw_script:
            raw_script_path = self.temp_dir / "script.py"
            raw_script_path.write_text(self.full_job_request.raw_script)
            extra_volume_flags = ["-v", f"{raw_script_path.absolute().as_posix()}:/script.py"]

            if not docker_run_cmd:
                docker_run_cmd = ["python", "/script.py"]

        if not docker_image:
            raise JobError("Could not determine Docker image to run")

        job_network = "none"
        # if streaming job create a local network for it to communicate with nginx
        if self.is_streaming_job:
            logger.debug("Spinning up local network for streaming job")
            job_network = self.job_network_name
            process = await asyncio.create_subprocess_exec(
                "docker", "network", "create", "--internal", self.job_network_name
            )
            await process.wait()

        if self.full_job_request.artifacts_dir:
            extra_volume_flags += [
                "-v",
                f"{self.artifacts_mount_dir.as_posix()}/:{self.full_job_request.artifacts_dir}",
            ]

        self.cmd = [
            "docker",
            "run",
            *docker_run_options,
            "--name",
            self.job_container_name,
            "--rm",
            "--network",
            job_network,
            "-v",
            f"{self.volume_mount_dir.as_posix()}/:/volume/",
            "-v",
            f"{self.output_volume_mount_dir.as_posix()}/:/output/",
            "-v",
            f"{self.specs_volume_mount_dir.as_posix()}/:/specs/",
            *extra_volume_flags,
            docker_image,
            *docker_run_cmd,
        ]

        # TODO: TIMEOUTS - This doesn't kill the docker container, just the docker process that communicates with it.
        async with temporary_process(
            *self.cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        ) as job_process:
            if self.is_streaming_job:
                logger.debug("Spinning up nginx for streaming job")
                await asyncio.sleep(1)
                try:
                    # the job container ip is needed to proxy the request to the job container
                    job_ip = await get_docker_container_ip(self.job_container_name)
                    # update the nginx conf with the job container ip
                    nginx_conf = NGINX_CONF.replace("CONTAINER", f"{job_ip}:{JOB_CONTAINER_PORT}")
                    assert self.nginx_dir_path is not None, (
                        "nginx dir path is None. Call prepare_initial() first."
                    )
                    await start_nginx(
                        nginx_conf,
                        port=settings.NGINX_PORT,
                        dir_path=self.nginx_dir_path,
                        job_network=self.job_network_name,
                        container_name=self.nginx_container_name,
                        timeout=WAIT_FOR_NGINX_TIMEOUT,
                    )
                except Exception as e:
                    raise JobError(f"Failed to start Nginx: {e}") from e

                assert self.executor_certificate is not None
                # check that the job is ready to serve requests
                ip = await get_docker_container_ip(self.nginx_container_name, bridge_network=True)
                logger.debug(f"Checking if streaming job is ready at http://{ip}/health")
                job_ready = await check_endpoint(
                    f"http://{ip}/health",
                    WAIT_FOR_STREAMING_JOB_TIMEOUT,  # TODO: TIMEOUTS - Remove timeout?
                )
                if not job_ready:
                    raise JobError("Streaming job health check failed")

            yield

            try:
                # Support for the legacy single-timeout
                docker_process_timeout = (
                    self.initial_job_request.executor_timing.execution_time_limit
                    if self.initial_job_request.executor_timing
                    else self.initial_job_request.timeout_seconds
                )
                result = await asyncio.wait_for(
                    job_process.communicate(),
                    timeout=docker_process_timeout,
                )
                self.execution_result = _ExecutionResult(
                    timed_out=False,
                    return_code=job_process.returncode,
                    stdout=result[0].decode(),
                    stderr=result[1].decode(),
                )

            except TimeoutError:
                stdout = (await job_process.stdout.read()).decode() if job_process.stdout else ""
                stderr = (await job_process.stderr.read()).decode() if job_process.stderr else ""
                self.execution_result = _ExecutionResult(
                    timed_out=True,
                    return_code=None,
                    stdout=stdout,
                    stderr=stderr,
                )

            if self.is_streaming_job:
                # stop the associated nginx server
                try:
                    await asyncio.sleep(1)
                    process = await asyncio.create_subprocess_exec(
                        "docker",
                        "stop",
                        self.nginx_container_name,
                    )
                    try:
                        await asyncio.wait_for(process.wait(), DOCKER_STOP_TIMEOUT_SECONDS)
                    except TimeoutError:
                        process.kill()
                        raise
                except Exception as e:
                    logger.error(f"Failed to stop Nginx: {e}")

    async def upload_results(self) -> JobResult:
        assert self.execution_result is not None, "No execution result"
        assert self.full_job_request is not None, (
            "Full job request must be set. Call prepare_full() first."
        )
        assert self.initial_job_request is not None, (
            "Initial job request must be set. Call prepare_initial() first."
        )

        # Save the streams in output volume and truncate them in response.
        with open(self.output_volume_mount_dir / "stdout.txt", "w") as f:
            f.write(self.execution_result.stdout)
        stdout = truncate(self.execution_result.stdout)
        with open(self.output_volume_mount_dir / "stderr.txt", "w") as f:
            f.write(self.execution_result.stderr)
        stderr = truncate(self.execution_result.stderr)

        success = self.execution_result.return_code == 0

        artifacts = {}
        for artifact_filename in os.listdir(self.artifacts_mount_dir):
            artifact_path = self.artifacts_mount_dir / artifact_filename
            if os.path.isfile(artifact_path):
                with open(artifact_path, "rb") as f:
                    content = f.read()
                artifact_size = len(content)
                if artifact_size < MAX_ARTIFACT_SIZE:
                    artifacts[f"{self.full_job_request.artifacts_dir}/{artifact_filename}"] = (
                        base64.b64encode(content).decode()
                    )
                else:
                    logger.error(f"Artifact {artifact_filename} too large: {artifact_size:,} bytes")
            else:
                logger.error(f"Directory found in artifacts: {artifact_filename}")

        if success:
            # upload the output if requested and job succeeded
            if self.full_job_request.output_upload:
                try:
                    output_uploader = OutputUploader.for_upload_output(
                        self.full_job_request.output_upload
                    )
                    output_uploader.max_size_bytes = settings.OUTPUT_ZIP_UPLOAD_MAX_SIZE_BYTES
                    await output_uploader.upload(self.output_volume_mount_dir)
                except OutputUploadFailed as ex:
                    logger.warning(
                        f"Uploading output failed for job {self.initial_job_request.job_uuid} with error: {ex!r}"
                    )
                    success = False
                    stdout = ex.description
                    stderr = ""

        return JobResult(
            success=success,
            exit_status=self.execution_result.return_code,
            timeout=self.execution_result.timed_out,
            stdout=stdout,
            stderr=stderr,
            artifacts=artifacts,
        )

    async def clean(self):
        # remove input/output directories with docker, to deal with funky file permissions
        root_for_remove = pathlib.Path("/temp_dir/")
        process = await asyncio.create_subprocess_exec(
            "docker",
            "run",
            "--rm",
            "-v",
            f"{self.temp_dir.as_posix()}/:/{root_for_remove.as_posix()}/",
            "alpine:3.19",
            "sh",
            "-c",
            f"rm -rf {shlex.quote(root_for_remove.as_posix())}/*",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await process.communicate()
        if process.returncode == 0:
            self.temp_dir.rmdir()
        else:
            logger.error(
                f"Failed to clean up {self.temp_dir.as_posix()}/: process exited with return code {process.returncode}\n"
                "Stdout and stderr:\n"
                f"{truncate(stdout.decode())}\n"
                f"{truncate(stderr.decode())}\n"
            )
        if self.is_streaming_job:
            process = await asyncio.create_subprocess_exec(
                "docker", "network", "rm", self.job_network_name
            )
            await process.wait()

    async def _unpack_volume(self, volume: Volume | None):
        assert str(self.volume_mount_dir) not in {"~", "/"}
        for path in self.volume_mount_dir.glob("*"):
            if path.is_file():
                path.unlink()
            elif path.is_dir():
                shutil.rmtree(path)

        if volume is not None:
            # TODO(mlech): Refactor this to not treat `HuggingfaceVolume` with a special care
            volume_downloader = VolumeDownloader.for_volume(volume)
            volume_downloader.max_size_bytes = settings.VOLUME_MAX_SIZE_BYTES
            if volume_downloader.handles_volume_type() is HuggingfaceVolume:
                if volume.token is None:
                    volume.token = settings.HF_ACCESS_TOKEN
                try:
                    await volume_downloader.download(self.volume_mount_dir)
                except VolumeDownloadFailed as exc:
                    logger.error(f"Failed to download model from Hugging Face: {exc}")
                    raise JobError(
                        str(exc),
                        V0JobFailedRequest.ErrorType.HUGGINGFACE_DOWNLOAD,
                        exc.error_detail,
                    ) from exc
            else:
                try:
                    await volume_downloader.download(self.volume_mount_dir)
                except VolumeDownloadFailed as exc:
                    raise JobError(str(exc)) from exc

        chmod_proc = await asyncio.create_subprocess_exec(
            "chmod", "-R", "777", self.temp_dir.as_posix()
        )
        assert 0 == await chmod_proc.wait()

    async def get_job_volume(self) -> Volume | None:
        assert self.full_job_request is not None, (
            "Full job request must be set. Call prepare_full() first."
        )
        assert self.initial_job_request is not None, (
            "Initial job request must be set. Call prepare_initial() first."
        )

        initial_volume = self.initial_job_request.volume
        late_volume = self.full_job_request.volume if self.full_job_request else None

        if initial_volume and late_volume:
            raise JobError("Received multiple volumes")

        return initial_volume or late_volume

    async def unpack_volume(self):
        await self._unpack_volume(await self.get_job_volume())


class Command(BaseCommand):
    help = "Run the executor, query the miner for job details, and run the job docker"

    MINER_CLIENT_CLASS = MinerClient
    JOB_RUNNER_CLASS = JobRunner

    def add_arguments(self, parser):
        parser.add_argument(
            "--startup-time-limit",
            type=int,
            help="Time limit in seconds for startup stage.",
            required=True,
        )

    runner: JobRunner
    miner_client: MinerClient

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @async_to_sync
    async def handle(self, *args, **options):
        self.runner = self.JOB_RUNNER_CLASS()
        self.miner_client = self.MINER_CLIENT_CLASS(settings.MINER_ADDRESS, settings.EXECUTOR_TOKEN)
        self.miner_client_for_tests = self.miner_client  # TODO: Remove this?
        await self._execute(startup_time_limit=cast(int, options.get("startup_time_limit")))

    async def _execute(self, startup_time_limit: int):
        async with self.miner_client:  # TODO: Can this hang?
            try:
                try:
                    # This limit should be enough to receive the initial job request, which contains further timing
                    # details.
                    async with asyncio.timeout(startup_time_limit):
                        logger.debug("Entering startup stage")
                        initial_job_request = await self._startup_stage()
                        timing_details = initial_job_request.executor_timing
                except TimeoutError as e:
                    raise JobError(
                        "Timed out during startup stage",
                        error_type=V0JobFailedRequest.ErrorType.TIMEOUT,
                    ) from e

                deadline = Timer()
                if timing_details:
                    # Initialize the deadline with leeway; it will be extended before each stage down the line
                    logger.debug(
                        f"Initializing deadline with leeway: {timing_details.allowed_leeway}s"
                    )
                    deadline.set_timeout(timing_details.allowed_leeway)
                elif initial_job_request.timeout_seconds is not None:
                    # For single-timeout, initialize with the full timeout for the whole job
                    logger.debug(
                        f"Initializing deadline with deprecated total timeout: {initial_job_request.timeout_seconds}s"
                    )
                    deadline.set_timeout(initial_job_request.timeout_seconds)
                else:
                    raise JobError(
                        "No timing received: either timeout_seconds or timing_details must be set"
                    )

                try:
                    if timing_details:
                        logger.debug(
                            f"Extending deadline by download time limit: +{timing_details.allowed_leeway}s"
                        )
                        deadline.extend_timeout(timing_details.download_time_limit)
                    async with asyncio.timeout(deadline.time_left()):
                        logger.debug(
                            f"Entering download stage; Time left: {deadline.time_left():.2f}s"
                        )
                        await self._download_stage()
                except TimeoutError as e:
                    raise JobError(
                        "Timed out during download stage",
                        error_type=V0JobFailedRequest.ErrorType.TIMEOUT,
                    ) from e

                try:
                    if timing_details:
                        logger.debug(
                            f"Extending deadline by execution time limit: +{timing_details.execution_time_limit}s"
                        )
                        deadline.extend_timeout(timing_details.execution_time_limit)
                    async with asyncio.timeout(deadline.time_left()):
                        logger.debug(
                            f"Entering execution stage; Time left: {deadline.time_left():.2f}s"
                        )
                        await self._execution_stage()
                except TimeoutError as e:
                    # Note - this timeout should never really fire.
                    # The job subprocess itself has an `execution_time_limit` timeout,
                    # and considering there is most likely some accumulated leeway,
                    # it should always either finish or time out by itself.
                    raise JobError(
                        "Timed out during execution stage",
                        error_type=V0JobFailedRequest.ErrorType.TIMEOUT,
                    ) from e

                try:
                    if timing_details:
                        logger.debug(
                            f"Extending deadline by upload time limit: +{timing_details.upload_time_limit}s"
                        )
                        deadline.extend_timeout(timing_details.upload_time_limit)
                    async with asyncio.timeout(deadline.time_left()):
                        logger.debug(
                            f"Entering upload stage; Time left: {deadline.time_left():.2f}s"
                        )
                        await self._upload_stage()
                except TimeoutError as e:
                    raise JobError(
                        "Timed out during upload stage",
                        error_type=V0JobFailedRequest.ErrorType.TIMEOUT,
                    ) from e

                logger.debug(f"Finished with {deadline.time_left():.2f}s time left")

            except JobError as e:
                logger.warning(f"Job error: {e}")
                await self.miner_client.send_job_error(e)

            except BaseException as e:
                logger.error(f"Unexpected error: {e}")
                await self.miner_client.send_job_error(JobError(f"Unexpected error: {e}"))
                raise

    async def _startup_stage(self) -> V0InitialJobRequest:
        await self.run_security_checks_or_fail()
        initial_job_request = await self.miner_client.initial_msg
        await self.runner.prepare_initial(initial_job_request)
        await self.miner_client.send_executor_ready()
        return initial_job_request

    async def _download_stage(self):
        full_job_request = await self.miner_client.full_payload
        await self.runner.prepare_full(full_job_request)
        await self.runner.unpack_volume()
        await self.miner_client.send_volumes_ready()

    async def _execution_stage(self):
        async with self.runner.start_job():
            if self.runner.is_streaming_job:
                assert self.runner.executor_certificate is not None, (
                    "Executor certificate is missing."
                )
                await self.miner_client.send_streaming_job_ready(self.runner.executor_certificate)
        await self.miner_client.send_execution_done()

    async def _upload_stage(self):
        job_result = await self.runner.upload_results()
        await self.miner_client.send_result(job_result)

    async def run_security_checks_or_fail(self):
        await self.run_cve_2022_0492_check_or_fail()
        if not settings.DEBUG_NO_GPU_MODE:
            await self.run_nvidia_toolkit_version_check_or_fail()

    async def run_cve_2022_0492_check_or_fail(self):
        # TODO: TIMEOUTS - This doesn't kill the docker container, just the docker process that communicates with it.
        async with temporary_process(
            "docker",
            "run",
            "--rm",
            CVE_2022_0492_IMAGE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        ) as docker_process:
            stdout, stderr = await docker_process.communicate()
            return_code = docker_process.returncode

        if return_code != 0:
            raise JobError(
                "CVE-2022-0492 check failed",
                error_detail=f'stdout="{stdout.decode()}"\nstderr="{stderr.decode()}"',
            )

        expected_output = "Contained: cannot escape via CVE-2022-0492"
        if expected_output not in stdout.decode():
            raise JobError(
                f'CVE-2022-0492 check failed: "{expected_output}" not in stdout.',
                V0JobFailedRequest.ErrorType.SECURITY_CHECK,
                f'stdout="{stdout.decode()}"\nstderr="{stderr.decode()}"',
            )

    async def run_nvidia_toolkit_version_check_or_fail(self):
        # TODO: TIMEOUTS - This doesn't kill the docker container, just the docker process that communicates with it.
        async with temporary_process(
            "docker",
            "run",
            "--rm",
            "--privileged",
            "-v",
            "/:/host:ro",
            "-v",
            "/usr/bin:/usr/bin",
            "-v",
            "/usr/lib:/usr/lib",
            "ubuntu:latest",
            "bash",
            "-c",
            "nvidia-container-toolkit --version",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        ) as docker_process:
            stdout, stderr = docker_process.communicate()
            return_code = docker_process.returncode

        if return_code != 0:
            raise JobError(
                f"nvidia-container-toolkit check failed: exit code {return_code}",
                error_detail=f'stdout="{stdout.decode()}"\nstderr="{stderr.decode()}"',
            )

        lines = stdout.decode().splitlines()
        if not lines:
            raise JobError(
                "nvidia-container-toolkit check failed: no output from nvidia-container-toolkit",
                error_detail=f'stdout="{stdout.decode()}"\nstderr="{stderr.decode()}"',
            )

        version = lines[0].rpartition(" ")[2]
        is_fixed_version = (
            packaging.version.parse(version) >= NVIDIA_CONTAINER_TOOLKIT_MINIMUM_SAFE_VERSION
        )
        if not is_fixed_version:
            raise JobError(
                f"Outdated NVIDIA Container Toolkit detected:"
                f'{version}" not >= {NVIDIA_CONTAINER_TOOLKIT_MINIMUM_SAFE_VERSION}',
                V0JobFailedRequest.ErrorType.SECURITY_CHECK,
                f'stdout="{stdout.decode()}"\nstderr="{stderr.decode()}',
            )
