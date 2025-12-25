import asyncio
import base64
import json
import logging
import os
import pathlib
import re
import shlex
import shutil
import tempfile
from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

import aiodocker
from compute_horde.base.docker import DockerRunOptionsPreset
from compute_horde.job_errors import HordeError, JobError
from compute_horde.protocol_consts import JobFailureReason
from compute_horde.protocol_messages import V0InitialJobRequest, V0JobRequest
from compute_horde_core.certificate import (
    check_endpoint,
    generate_certificate_at,
    get_docker_container_ip,
    save_public_key,
    start_nginx,
)
from compute_horde_core.output_upload import OutputUploader, OutputUploadFailed
from compute_horde_core.volume import (
    HuggingfaceVolume,
    Volume,
    VolumeDownloader,
    VolumeDownloadFailed,
)
from django.conf import settings

from compute_horde_executor.executor.miner_client import (
    ExecutionResult,
    JobResult,
)
from compute_horde_executor.executor.utils import (
    docker_container_wrapper,
    get_docker_container_outputs,
)

logger = logging.getLogger(__name__)

MAX_RESULT_SIZE_IN_RESPONSE = 1_000_000  # 1 MB
TRUNCATED_RESPONSE_PREFIX_LEN = MAX_RESULT_SIZE_IN_RESPONSE // 2
TRUNCATED_RESPONSE_SUFFIX_LEN = MAX_RESULT_SIZE_IN_RESPONSE // 2
MAX_ARTIFACT_SIZE = 1_000_000
DOCKER_STOP_TIMEOUT_SECONDS = 15

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


def preset_to_docker_run_args(preset: DockerRunOptionsPreset) -> dict[str, Any]:
    if settings.DEBUG_NO_GPU_MODE:
        return {}
    elif preset == "none":
        return {}
    elif preset == "nvidia_all":
        return {
            "Runtime": "nvidia",
            "DeviceRequests": [
                {
                    "Driver": "nvidia",
                    "Count": -1,  # All GPUs
                    "Capabilities": [["gpu"]],
                }
            ],
        }
    else:
        raise HordeError(f"Invalid preset: {preset}")


def truncate(v: str) -> str:
    if len(v) > MAX_RESULT_SIZE_IN_RESPONSE:
        return f"{v[:TRUNCATED_RESPONSE_PREFIX_LEN]} ... {v[-TRUNCATED_RESPONSE_SUFFIX_LEN:]}"
    else:
        return v


def job_container_name(docker_objects_infix: str) -> str:
    return f"ch-{docker_objects_infix}-job"


def nginx_container_name(docker_objects_infix: str) -> str:
    return f"ch-{docker_objects_infix}-nginx"


def network_name(docker_objects_infix: str) -> str:
    return f"ch-{docker_objects_infix}"


def fix_docker_tag(image: str) -> str:
    if ":" not in image:
        return f"{image}:latest"
    return image


class BaseJobRunner(ABC):
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

        self.execution_result: ExecutionResult | None = None

        # for streaming job
        self.is_streaming_job: bool = False
        self.nginx_dir_path: pathlib.Path | None = None
        self.executor_certificate: str | None = None

    def generate_streaming_certificate(self, executor_ip: str, public_key: str):
        """
        Generate and save the streaming certificate and public key for the executor.
        """
        self.nginx_dir_path, self.executor_certificate, _ = generate_certificate_at(
            alternative_name=executor_ip,
        )
        save_public_key(public_key, self.nginx_dir_path)
        self.is_streaming_job = True

    async def cleanup_potential_old_jobs(self):
        logger.debug("Cleaning up potential old jobs")

        async with aiodocker.Docker() as client:
            # Clean up -job and -nginx containers
            job_pattern = re.compile(job_container_name(".*"))
            nginx_pattern = re.compile(nginx_container_name(".*"))
            containers = await client.containers.list(all=True)
            for _container in containers:
                info = await _container.show()
                name = info.get("Name", "").lstrip("/")
                if job_pattern.match(name) or nginx_pattern.match(name):
                    try:
                        # TODO: This only kills but potentially leaves behind a stopped container. also remove?
                        await _container.kill()
                    except Exception as e:
                        logger.warning(f"Failed to kill container {name}: {e}")

            # Clean up networks
            network_pattern = re.compile(network_name(".*"))
            networks = await client.networks.list()
            for _network in networks:
                name = _network.get("Name", "")
                if network_pattern.match(name):
                    network_obj = await client.networks.get(name)
                    try:
                        await network_obj.delete()
                    except Exception as e:
                        logger.warning(f"Failed to remove network {name}: {e}")

    async def prepare_initial(self, initial_job_request: V0InitialJobRequest):
        self.initial_job_request = initial_job_request
        self.volume_mount_dir.mkdir(exist_ok=True)
        self.output_volume_mount_dir.mkdir(exist_ok=True)
        self.artifacts_mount_dir.mkdir(exist_ok=True)
        await self.cleanup_potential_old_jobs()
        await self.pull_initial_job_image()

    async def pull_initial_job_image(self):
        assert self.initial_job_request, "Initial job request must be set before pulling the image"

        if self.initial_job_request.docker_image is not None:
            logger.debug(f"Pulling Docker image {self.initial_job_request.docker_image}")
            async with aiodocker.Docker() as client:
                stdout = ""
                stderr = ""
                try:
                    docker_image = fix_docker_tag(self.initial_job_request.docker_image)
                    async for _line in client.images.pull(docker_image, stream=True):
                        line_str = json.dumps(_line)
                        if "error" in _line:
                            stderr += line_str + "\n"
                        else:
                            stdout += line_str + "\n"
                except Exception:
                    raise HordeError(
                        "Failed to pull docker image",
                        context={
                            "stdout": truncate(stdout),
                            "stderr": truncate(stderr),
                        },
                    )

    async def prepare_full(self, full_job_request: V0JobRequest):
        self.full_job_request = full_job_request

    @abstractmethod
    async def get_docker_image(self) -> str:
        """
        Return the docker image name to run by the executor.
        """

    @abstractmethod
    async def get_docker_run_args(self) -> dict[str, Any]:
        """
        Return a dictionary of keyword arguments to pass to aiodocker.Docker().containers.create().
        Should typically contain the keys "name", and "HostConfig" but should not include volumes
        and docker image names.
        """

    @abstractmethod
    async def get_docker_run_cmd(self) -> list[str]:
        """
        Return a cmd to pass to the docker container, e.g. `self.full_job_request.docker_run_cmd`.
        The return value of this method is appended after the image name in the `docker run` command.
        """

    @abstractmethod
    async def job_cleanup(self, container: aiodocker.containers.DockerContainer):
        """
        Perform any cleanup necessary after the job is finished.
        """

    async def before_start_job(self):
        """
        Perform any action necessary just before the job process is started.
        """

    async def after_start_job(self):
        """
        Perform any action necessary just after the job process is started.
        """

    @asynccontextmanager
    async def start_job(self) -> AsyncGenerator[None, Any]:
        """
        Trigger a job to run in a Docker container.
        """
        assert self.initial_job_request is not None, (
            "Call prepare_initial() and prepare_full() first"
        )
        assert self.full_job_request is not None, "Call prepare_initial() and prepare_full() first"

        # Get docker args
        docker_kwargs = await self.get_docker_run_args()

        # Add volume mounts
        if "Binds" not in docker_kwargs["HostConfig"]:
            docker_kwargs["HostConfig"]["Binds"] = []
        docker_kwargs["HostConfig"]["Binds"].append(f"{self.volume_mount_dir.as_posix()}:/volume/")
        docker_kwargs["HostConfig"]["Binds"].append(
            f"{self.output_volume_mount_dir.as_posix()}:/output/"
        )
        docker_kwargs["HostConfig"]["Binds"].append(
            f"{self.specs_volume_mount_dir.as_posix()}:/specs/"
        )
        if self.full_job_request.artifacts_dir:
            docker_kwargs["HostConfig"]["Binds"].append(
                f"{self.artifacts_mount_dir.as_posix()}/:{self.full_job_request.artifacts_dir}"
            )
        docker_kwargs["Env"] = [f"{k}={v}" for k, v in self.full_job_request.env.items()]

        await self.before_start_job()
        async with docker_container_wrapper(
            image=await self.get_docker_image(),
            command=await self.get_docker_run_cmd(),
            **docker_kwargs,
        ) as docker_container:
            await self.after_start_job()
            yield
            await self.job_cleanup(docker_container)

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

        upload_results = {}

        # upload the output if requested
        if self.full_job_request.output_upload:
            try:
                output_uploader = OutputUploader.for_upload_output(
                    self.full_job_request.output_upload
                )
                output_uploader.max_size_bytes = settings.OUTPUT_ZIP_UPLOAD_MAX_SIZE_BYTES
                upload_results = await output_uploader.upload(self.output_volume_mount_dir)
            except OutputUploadFailed as ex:
                # TODO(error propagation): add some context
                raise JobError("Upload failed", JobFailureReason.UPLOAD_FAILED) from ex

        return JobResult(
            exit_status=self.execution_result.return_code,
            timeout=self.execution_result.timed_out,
            stdout=stdout,
            stderr=stderr,
            artifacts=artifacts,
            upload_results=(
                {key: json.dumps(value.dict()) for key, value in upload_results.items()}
            ),
        )

    async def clean(self):
        # remove input/output directories with docker, to deal with funky file permissions
        root_for_remove = pathlib.Path("/temp_dir/")
        async with docker_container_wrapper(
            image="alpine:3.19",
            command=["sh", "-c", f"rm -rf {shlex.quote(root_for_remove.as_posix())}/*"],
            auto_remove=True,
            HostConfig={
                "Binds": [
                    f"{self.temp_dir.as_posix()}/:/{root_for_remove.as_posix()}/",
                ]
            },
        ) as docker_container:
            result = await docker_container.wait()
            return_code = result["StatusCode"]
            stdout, stderr = await get_docker_container_outputs(docker_container)

        if return_code != 0:
            logger.error(
                f"Failed to clean up {self.temp_dir.as_posix()}/: process exited with return code {return_code}\n"
                "Stdout and stderr:\n"
                f"{stdout}\n"
                f"{stderr}\n"
            )
        try:
            shutil.rmtree(self.temp_dir)
        except Exception as e:
            logger.error(f"Failed to remove temp dir {self.temp_dir}: {e}")

    async def download_volume(self):
        assert str(self.volume_mount_dir) not in {"~", "/"}
        for path in self.volume_mount_dir.glob("*"):
            if path.is_file():
                path.unlink()
            elif path.is_dir():
                shutil.rmtree(path)

        volume = await self.get_job_volume()
        if volume is not None:
            # TODO(mlech): Refactor this to not treat `HuggingfaceVolume` with a special care
            volume_downloader = VolumeDownloader.for_volume(volume)
            volume_downloader.max_size_bytes = settings.VOLUME_MAX_SIZE_BYTES
            if volume_downloader.handles_volume_type() is HuggingfaceVolume:
                if volume.token is None:
                    volume.token = settings.HF_ACCESS_TOKEN
            try:
                await volume_downloader.download(self.volume_mount_dir)
            except VolumeDownloadFailed as e:
                raise JobError(
                    f"Download failed: {str(e)}", JobFailureReason.DOWNLOAD_FAILED
                ) from e

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
            raise HordeError("Received multiple volumes")

        return initial_volume or late_volume


class DefaultJobRunner(BaseJobRunner):
    """
    Default implementation of a job runner.
    The image and run args are taken from a job request.
    The job runner is prepared to handle the streaming jobs
    by automatically creating and managing a docker network and Nginx instance.
    """

    async def get_docker_image(self) -> str:
        assert self.full_job_request is not None, (
            "Full job request must be set. Call prepare_full() first."
        )
        assert self.initial_job_request is not None, (
            "Initial job request must be set. Call prepare_initial() first."
        )
        return fix_docker_tag(self.full_job_request.docker_image)

    async def get_docker_run_args(self) -> dict[str, Any]:
        assert self.full_job_request is not None, (
            "Full job request must be set. Call prepare_full() first."
        )
        assert self.initial_job_request is not None, (
            "Initial job request must be set. Call prepare_initial() first."
        )

        # Build keyword arguments to be passed to aiodocker.Docker().containers.create()
        docker_kwargs: dict[str, Any] = {"name": self.job_container_name, "auto_remove": True}

        # NVIDIA environment
        docker_kwargs["HostConfig"] = preset_to_docker_run_args(
            self.full_job_request.docker_run_options_preset
        )

        job_network = "none"
        # if streaming job - create a local network for it to communicate with nginx
        if self.is_streaming_job:
            logger.debug("Spinning up local network for streaming job")
            job_network = self.job_network_name
            async with aiodocker.Docker() as client:
                try:
                    await client.networks.create(
                        {
                            "Name": self.job_network_name,
                            "CheckDuplicate": True,
                            "Internal": True,
                        }
                    )
                except Exception as e:
                    logger.warning(f"Failed to create network {job_network}: {e}")
        docker_kwargs["HostConfig"]["NetworkMode"] = job_network

        if self.full_job_request.raw_script:
            raw_script_path = self.temp_dir / "script.py"
            raw_script_path.write_text(self.full_job_request.raw_script)
            docker_kwargs["HostConfig"]["Binds"] = [
                f"{raw_script_path.absolute().as_posix()}:/script.py"
            ]

        return docker_kwargs

    async def get_docker_run_cmd(self) -> list[str]:
        assert self.full_job_request is not None, (
            "Full job request must be set. Call prepare_full() first."
        )
        assert self.initial_job_request is not None, (
            "Initial job request must be set. Call prepare_initial() first."
        )
        cmd = self.full_job_request.docker_run_cmd
        if not cmd and self.full_job_request.raw_script:
            cmd = ["python", "/script.py"]
        return cmd

    async def after_start_job(self):
        if not self.is_streaming_job:
            return
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
            raise HordeError(f"Failed to start Nginx: {truncate(str(e))}") from e

        assert self.executor_certificate is not None
        # check that the job is ready to serve requests
        ip = await get_docker_container_ip(self.nginx_container_name, bridge_network=True)
        logger.debug(f"Checking if streaming job is ready at http://{ip}/health")
        job_ready = await check_endpoint(
            f"http://{ip}/health",
            WAIT_FOR_STREAMING_JOB_TIMEOUT,  # TODO: TIMEOUTS - Remove timeout?
        )
        if not job_ready:
            raise JobError("Streaming job health check failed", JobFailureReason.TIMEOUT)

    async def job_cleanup(self, container: aiodocker.containers.DockerContainer):
        assert self.initial_job_request is not None, (
            "Initial job request must be set. Call prepare_initial() first."
        )
        try:
            # Support for the legacy single-timeout
            docker_process_timeout = (
                self.initial_job_request.executor_timing.execution_time_limit
                if self.initial_job_request.executor_timing
                else self.initial_job_request.timeout_seconds
            )
            logger.debug(f"Waiting {docker_process_timeout} seconds for job container to finish")
            result = await asyncio.wait_for(
                container.wait(),
                timeout=docker_process_timeout,
            )
            return_code = result["StatusCode"]
            stdout, stderr = await get_docker_container_outputs(container)
            logger.debug(f"Job container exited with return code {return_code}")
            self.execution_result = ExecutionResult(
                timed_out=False,
                return_code=return_code,
                stdout=stdout,
                stderr=stderr,
            )

        except TimeoutError:
            logger.debug("Job container timed out")
            stdout, stderr = await get_docker_container_outputs(container)
            self.execution_result = ExecutionResult(
                timed_out=True,
                return_code=None,
                stdout=stdout,
                stderr=stderr,
            )

        if self.is_streaming_job:

            async def _stop_nginx():
                async with aiodocker.Docker() as client:
                    container = await client.containers.get(self.nginx_container_name)
                    await container.stop()

            # stop the associated nginx server
            try:
                await asyncio.sleep(1)
                await asyncio.wait_for(_stop_nginx(), DOCKER_STOP_TIMEOUT_SECONDS)
            except Exception as e:
                logger.error(f"Failed to stop Nginx: {e}")
