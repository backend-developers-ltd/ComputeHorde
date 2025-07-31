import asyncio
import base64
import json
import logging
import os
import pathlib
import shlex
import shutil
import tempfile
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

from compute_horde.base.docker import DockerRunOptionsPreset
from compute_horde.protocol_messages import V0InitialJobRequest, V0JobFailedRequest, V0JobRequest
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
    VolumeManagerClient,
    VolumeManagerError,
    create_volume_manager_client,
    get_volume_manager_headers,
)
from django.conf import settings

from compute_horde_executor.executor.miner_client import ExecutionResult, JobError, JobResult
from compute_horde_executor.executor.utils import temporary_process

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


def preset_to_docker_run_args(preset: DockerRunOptionsPreset) -> list[str]:
    if settings.DEBUG_NO_GPU_MODE:
        return []
    elif preset == "none":
        return []
    elif preset == "nvidia_all":
        return ["--runtime=nvidia", "--gpus", "all"]
    else:
        raise JobError(f"Invalid preset: {preset}")


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

        self.execution_result: ExecutionResult | None = None

        # for streaming job
        self.is_streaming_job: bool = False
        self.nginx_dir_path: pathlib.Path | None = None
        self.executor_certificate: str | None = None

        # Volume manager client (if configured)
        self.volume_manager_client: VolumeManagerClient | None = None
        if settings.VOLUME_MANAGER_ADDRESS:
            headers = get_volume_manager_headers()
            self.volume_manager_client = create_volume_manager_client(
                settings.VOLUME_MANAGER_ADDRESS, headers
            )

        # Track volume manager mounts for cleanup
        self.volume_manager_mounts: list[list[str]] = []

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
        self.volume_mount_dir.mkdir(exist_ok=True)
        self.output_volume_mount_dir.mkdir(exist_ok=True)
        self.artifacts_mount_dir.mkdir(exist_ok=True)
        await self.cleanup_potential_old_jobs()
        await self.pull_initial_job_image()

    async def pull_initial_job_image(self):
        assert self.initial_job_request, "Initial job request must be set before pulling the image"

        if self.initial_job_request.docker_image is not None:
            logger.debug(f"Pulling Docker image {self.initial_job_request.docker_image}")
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

        docker_run_options = preset_to_docker_run_args(
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

        # Build volume mount flags
        volume_flags = []
        if self.volume_manager_mounts:
            # Use volume manager mounts
            for mount_flags in self.volume_manager_mounts:
                volume_flags.extend(mount_flags)
                logger.debug(f"Adding volume manager mount: {mount_flags}")
        else:
            # Use default volume mount
            volume_flags = ["-v", f"{self.volume_mount_dir.as_posix()}/:/volume/"]

        self.cmd = [
            "docker",
            "run",
            *docker_run_options,
            "--name",
            self.job_container_name,
            "--rm",
            "--network",
            job_network,
            *volume_flags,
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
                logger.debug(
                    f"Waiting {docker_process_timeout} seconds for job container to finish"
                )
                result = await asyncio.wait_for(
                    job_process.communicate(),
                    timeout=docker_process_timeout,
                )
                logger.debug(f"Job container exited with return code {job_process.returncode}")
                self.execution_result = ExecutionResult(
                    timed_out=False,
                    return_code=job_process.returncode,
                    stdout=result[0].decode(),
                    stderr=result[1].decode(),
                )

            except TimeoutError:
                logger.debug("Job container timed out")
                stdout = (await job_process.stdout.read()).decode() if job_process.stdout else ""
                stderr = (await job_process.stderr.read()).decode() if job_process.stderr else ""
                self.execution_result = ExecutionResult(
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
                raise JobError("Job failed during upload", error_detail=str(ex)) from ex

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
        # Close volume manager client to prevent resource leaks
        if self.volume_manager_client:
            if self.initial_job_request:
                # Notify volume manager if configured
                await self._notify_volume_manager_job_finished()

            try:
                await self.volume_manager_client.close()
            except Exception as e:
                logger.warning(f"Failed to close volume manager client: {e}")
            finally:
                self.volume_manager_client = None

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
        if process.returncode != 0:
            logger.error(
                f"Failed to clean up {self.temp_dir.as_posix()}/: process exited with return code {process.returncode}\n"
                "Stdout and stderr:\n"
                f"{truncate(stdout.decode())}\n"
                f"{truncate(stderr.decode())}\n"
            )
        try:
            shutil.rmtree(self.temp_dir)
        except Exception as e:
            logger.error(f"Failed to remove temp dir {self.temp_dir}: {e}")

    async def _notify_volume_manager_job_finished(self):
        """Notify the volume manager that the job has finished."""
        assert self.volume_manager_client is not None
        assert self.initial_job_request is not None

        job_uuid = self.initial_job_request.job_uuid
        try:
            logger.debug(f"Notifying Volume Manager that job {job_uuid} has finished")
            await self.volume_manager_client.job_finished(job_uuid)
        except Exception as e:
            # Log the error but don't fail the job cleanup
            logger.warning(f"Failed to notify Volume Manager of job completion: {e}")

    async def _unpack_volume(self, volume: Volume | None):
        """
        Handle volume preparation. If volume manager is configured, delegate to it.
        Otherwise, use the traditional download approach.
        """
        # Clear the volume mount directory
        assert str(self.volume_mount_dir) not in {"~", "/"}
        for path in self.volume_mount_dir.glob("*"):
            if path.is_file():
                path.unlink()
            elif path.is_dir():
                shutil.rmtree(path)

        if volume is not None:
            # Check if volume manager is configured
            if self.volume_manager_client:
                await self._prepare_volume_with_manager(volume)
            else:
                await self._download_volume_directly(volume)

        chmod_proc = await asyncio.create_subprocess_exec(
            "chmod", "-R", "777", self.temp_dir.as_posix()
        )
        assert 0 == await chmod_proc.wait()

    async def _prepare_volume_with_manager(self, volume: Volume):
        """Use volume manager to prepare the volume."""
        assert self.initial_job_request is not None
        assert self.full_job_request is not None
        assert self.volume_manager_client is not None

        job_uuid = self.initial_job_request.job_uuid

        # Prepare job metadata
        job_metadata = self.full_job_request.model_dump()

        try:
            logger.debug(f"Requesting volume preparation from Volume Manager for job {job_uuid}")
            response = await self.volume_manager_client.prepare_volume(
                job_uuid=job_uuid, volume=volume, job_metadata=job_metadata
            )

            # Store the mounts for later use in Docker command
            self.volume_manager_mounts = response
            logger.debug(f"Volume Manager provided {len(response)} mounts")

        except VolumeManagerError as exc:
            logger.warning(f"Volume Manager failed to prepare volume for job {job_uuid}: {exc}")
            logger.info("Falling back to direct volume download")

            # Fallback to direct download
            await self._download_volume_directly(volume)

    async def _download_volume_directly(self, volume: Volume):
        """Traditional volume download approach."""
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
