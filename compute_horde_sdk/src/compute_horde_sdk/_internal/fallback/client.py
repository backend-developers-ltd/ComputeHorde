import asyncio
import logging
import pathlib
import random
import socket
import subprocess
import tempfile
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Coroutine
from typing import TYPE_CHECKING, Any, TypeAlias

from .adaptors.utils import lazy_import_adaptor
from .exceptions import FallbackNotFoundError
from .job import FallbackJob, FallbackJobResult, FallbackJobSpec, FallbackJobStatus
from .utils import PackageAnalyzer, change_dir

if TYPE_CHECKING:
    from .adaptors.sky import SkyCloud as SkyCloudType
    from .adaptors.sky import SkyJob as SkyJobType

logger = logging.getLogger(__name__)

sky = lazy_import_adaptor("sky")

JobAttemptCallbackType: TypeAlias = (
    Callable[["FallbackJob"], None]
    | Callable[["FallbackJob"], Awaitable[None]]
    | Callable[["FallbackJob"], Coroutine[Any, Any, None]]
)

_SETUP_TMPL = """
#!/bin/bash

set -euo pipefail

ARTIFACTS_DIR="{artifacts_dir}"
VOLUME_DIR="/volume"
OUTPUT_UPLOAD_DIR="/output"
[ -n "$ARTIFACTS_DIR" ] && rm -rf "$ARTIFACTS_DIR" && mkdir -p "$ARTIFACTS_DIR"
rm -rf "$VOLUME_DIR" && mkdir -p "$VOLUME_DIR"
rm -rf "$OUTPUT_UPLOAD_DIR" && mkdir -p "$OUTPUT_UPLOAD_DIR"
{command}
"""

_RUN_TMPL = """
#!/bin/bash

set -euo pipefail
shopt -s nullglob

volumes=(volume-*.json)
if (( ${{#volumes[@]}} )); then
  python3 -m compute_horde_core.volume ${{volumes[@]}} --dir /volume > ./ch-volume.log 2>&1
fi
pushd "{workdir}" > /dev/null
{command}
popd > /dev/null
output_uploads=(output_upload-*.json)
if (( ${{#output_uploads[@]}} )); then
  python3 -m compute_horde_core.output_upload ${{output_uploads[@]}} --dir /output > ./ch-output_upload.log 2>&1
fi
"""


class FallbackClient:
    """
    A fallback client that provides the same API as ComputeHordeClient.
    """

    DEFAULT_MAX_JOB_RUN_ATTEMPTS = 3
    HTTP_RETRY_MIN_WAIT_SECONDS = 0.2
    HTTP_RETRY_MAX_WAIT_SECONDS = 5
    MAX_ARTIFACT_SIZE = 1_000_000

    def __init__(self, cloud: str, idle_minutes: int = 15, **kwargs: Any) -> None:
        """
        Initializes a FallbackClient that can execute jobs using a SkyPilot-managed cloud backend.

        :param cloud: The name of the cloud backend to use (e.g. "runpod").
            This value is passed directly to SkyPilot. We currently only test
            with "runpod", but you are welcome to try the other providers too.

        :param idle_minutes: Number of minutes the fallback instance can remain idle before being shut down.
            Defaults to 15 minutes.

        :param kwargs: Additional arguments forwarded to the SkyPilot cloud environment setup.

        :raises ModuleNotFoundError: If the ``fallback`` extra is not installed.

        .. note::
            You must install the fallback extra with:

                pip install compute-horde-sdk[fallback]

            For details on available cloud environments, see:
            https://docs.skypilot.co/en/v0.8.1/overview.html#cloud-vms
        """

        try:
            self.cloud: SkyCloudType = sky.SkyCloud(cloud, **kwargs)
        except ModuleNotFoundError:
            raise ModuleNotFoundError(
                "The fallback extra has not been installed. "
                "Please install it with `pip install compute-horde-sdk[fallback]`."
            ) from None
        self.idle_minutes = idle_minutes

        self._jobs: dict[str, SkyJobType] = {}
        self.streaming_port: str | None = None
        self._active_tunnels: dict[str, subprocess.Popen[bytes]] = {}

    def __enter__(self) -> "FallbackClient":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        # Kill all active tunnels
        for job_uuid, tunnel in self._active_tunnels.items():
            try:
                tunnel.terminate()
                tunnel.wait(timeout=5)
            except Exception as e:
                logger.warning(f"Failed to kill tunnel for job {job_uuid}: {e}")
        self._active_tunnels.clear()

    async def create_job(self, job_spec: FallbackJobSpec) -> FallbackJob:
        """
        Run a fallback job in the SkyPilot cluster. This method does not retry a failed job.
        Use :meth:`run_until_complete` if you want failed jobs to be automatically retried.

        :param job_spec: Job specification to run.
        :return: A :class:`FallbackJob` class instance representing the created job.
        """
        logger.info("Running fallback job...")
        logger.debug("Fallback job spec: %s", job_spec)

        workdir = self._prepare_workdir()
        setup = self._prepare_setup(workdir, job_spec)
        run = self._prepare_run(workdir, job_spec)

        sky_job: SkyJobType = sky.SkyJob(
            cloud=self.cloud,
            workdir=workdir,
            setup=setup,
            run=run,
            envs=job_spec.envs,
            artifacts_dir=job_spec.artifacts_dir,
            accelerators=job_spec.accelerators,
            cpus=job_spec.cpus,
            memory=job_spec.memory,
            disk_size=job_spec.disk_size,
            ports=job_spec.ports,
            instance_type=job_spec.instance_type,
            image_id=job_spec.image_id,
            region=job_spec.region,
            zone=job_spec.zone,
        )
        sky_job.submit(idle_minutes=self.idle_minutes)
        self._jobs[sky_job.job_uuid] = sky_job

        job = FallbackJob(
            self,
            uuid=sky_job.job_uuid,
            status=FallbackJobStatus.SENT,
        )
        logger.info("The job has been submitted: %s", job)

        if job_spec.streaming:
            logger.info("Opening streaming tunnel...")

            def is_port_available(port: int) -> bool:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                try:
                    sock.bind(("localhost", port))
                    sock.close()
                    return True
                except OSError:
                    return False

            for port in range(8000, 9000):
                if is_port_available(port):
                    job.streaming_server_port = port
                    break

            if job.streaming_server_port is None:
                raise FallbackNotFoundError(f"Could not find available port for job {job.uuid}")

            await self.create_ssh_tunnel(job.uuid, job.streaming_server_port)
            logger.info(f"Created SSH tunnel for job {job.uuid}")

        return job

    async def run_until_complete(
        self,
        job_spec: FallbackJobSpec,
        job_attempt_callback: JobAttemptCallbackType | None = None,
        timeout: float | None = None,
        max_attempts: int = DEFAULT_MAX_JOB_RUN_ATTEMPTS,
    ) -> FallbackJob:
        """
        Run a fallback job in the SkyPilot cluster until it is successful.
        It will call :meth:`create_job` repeatedly until the job is successful.

        :param job_spec: Job specification to run.
        :param job_attempt_callback: A callback function that will be called after every attempt of running the job.
            The callback will be called immediately after an attempt is made run the job,
            before waiting for the job to complete.
            The function must take one argument of type FallbackJob.
            It can be a regular or an async function.
        :param timeout: Maximum number of seconds to wait for.
        :param max_attempts: Maximum number times the job will be attempted to run within ``timeout`` seconds.
            Negative or ``0`` means unlimited attempts.
        :return: A :class:`FallbackJob` class instance representing the created job.
            If the job was rerun, it will represent the last attempt.
        """
        start_time = time.monotonic()

        def remaining_timeout() -> float | None:
            if timeout is None:
                return None
            new_timeout = timeout - (time.monotonic() - start_time)
            return max(new_timeout, 0)

        attempt = 0
        while True:
            attempt += 1
            attempt_msg = f"{attempt}/{max_attempts}" if max_attempts > 0 else f"{attempt}"
            logger.info("Attempting to run job [%s]", attempt_msg)

            job = await self.create_job(job_spec)

            if job_attempt_callback:
                maybe_coro = job_attempt_callback(job)
                if asyncio.iscoroutine(maybe_coro):
                    await maybe_coro

            await job.wait(timeout=remaining_timeout())

            if job.status.is_successful():
                return job

            if max_attempts > 0 and attempt >= max_attempts:
                return job

            # Apply exponential backoff with jitter before retrying
            backoff_seconds = min(
                self.HTTP_RETRY_MIN_WAIT_SECONDS * (2 ** (attempt - 1)), self.HTTP_RETRY_MAX_WAIT_SECONDS
            )
            jitter = random.uniform(0, backoff_seconds * 0.1)
            wait_time = backoff_seconds + jitter
            logger.info(f"Job attempt {attempt_msg} failed, waiting {wait_time:.2f} seconds before retry")
            await asyncio.sleep(wait_time)

    async def get_job(self, job_uuid: str) -> FallbackJob:
        """
        Retrieve information about a job from the SkyPilot cluster.

        :param job_uuid: The UUID of the job to retrieve.
        :return: A :class:`FallbackJob` instance representing this job.
        :raises FallbackNotFoundError: If the job with this UUID does not exist.
        """
        job = self._jobs.get(job_uuid)
        if job is None:
            raise FallbackNotFoundError(f"Job with UUID {job_uuid} not found")

        status = self._get_status(job)
        logger.debug("Fallback job status: %s", status)
        if not status.is_in_progress():
            result = self._get_result(job)
        else:
            result = None

        return FallbackJob(
            self,
            uuid=job_uuid,
            status=status,
            result=result,
        )

    async def get_jobs(self) -> list[FallbackJob]:
        """
        Retrieve information about your jobs from the SkyPilot cluster.

        :return: A list of :class:`FallbackJob` instances representing your jobs.
        """
        return [j async for j in self.iter_jobs()]

    async def iter_jobs(self) -> AsyncIterator[FallbackJob]:
        """
        Retrieve information about your jobs from the ComputeHorde.

        :return: An async iterator of :class:`FallbackJob` instances representing your jobs.
        """
        for job_uuid in self._jobs.keys():
            yield await self.get_job(job_uuid)

    async def get_job_streaming_port(self, job_uuid: str) -> int | None:
        """
        Retrieve the SSH port of the job for streaming.
        """
        job = self._jobs[job_uuid]
        return job.get_job_ssh_port()

    async def create_ssh_tunnel(self, job_uuid: str, local_port: int) -> None:
        """
        Create an SSH tunnel to the job's streaming port.

        :param job_uuid: The UUID of the job to tunnel to
        :param local_port: The local port to forward to
        """
        job = self._jobs[job_uuid]
        head_ip = job.get_job_head_ip()
        ssh_port = await self.get_job_streaming_port(job_uuid)

        if not ssh_port:
            raise FallbackNotFoundError(f"Could not get SSH port for job {job_uuid}")

        tunnel_cmd = [
            "ssh",
            "-N",
            "-o",
            "StrictHostKeyChecking=no",
            "-o",
            "UserKnownHostsFile=/dev/null",
            "-i",
            str(pathlib.Path.home() / ".ssh" / "sky-key"),
            "-L",
            f"127.0.0.1:{local_port}:localhost:8000",
            f"root@{head_ip}",
            "-p",
            str(ssh_port),
        ]

        try:
            tunnel = subprocess.Popen(tunnel_cmd)
            self._active_tunnels[job_uuid] = tunnel
            logger.info(f"Created SSH tunnel from localhost:{local_port} to {head_ip}:{self.streaming_port}")
        except Exception as e:
            logger.error(f"Failed to create SSH tunnel: {e}")
            raise

    @classmethod
    def _prepare_workdir(cls) -> pathlib.Path:
        workdir = pathlib.Path(tempfile.mkdtemp(prefix="ch-"))
        logger.debug("Working directory: %s", workdir)

        return workdir

    @classmethod
    def _prepare_setup(cls, workdir: pathlib.Path, job_spec: FallbackJobSpec) -> str:
        script = "./setup.sh"
        with change_dir(workdir):
            pa = PackageAnalyzer("compute-horde-sdk")
            source = pa.to_source()
            logger.debug("ComputeHorde SDK installable: %s", source)

            setup_sh = pathlib.Path(script)
            # TODO(maciek): use already preinstalled uv (by SkyPilot) for managing python and virtualenv
            setup_sh.write_text(
                _SETUP_TMPL.format(
                    command=f"pip install --force-reinstall {source}", artifacts_dir=job_spec.artifacts_dir or ""
                )
            )
            setup_sh.chmod(0o755)

        return script

    @classmethod
    def _prepare_run(cls, workdir: pathlib.Path, job_spec: FallbackJobSpec) -> str:
        script = "./run.sh"
        with change_dir(workdir):
            if job_spec.input_volumes is not None:
                for index, input_volume in enumerate(job_spec.input_volumes):
                    volume_json = pathlib.Path(f"./volume-{index}.json")
                    volume_json.write_text(
                        job_spec.input_volumes[input_volume].to_compute_horde_volume(input_volume).json()
                    )
            if job_spec.output_volumes is not None:
                for index, output_volume in enumerate(job_spec.output_volumes):
                    output_upload_json = pathlib.Path(f"./output_upload-{index}.json")
                    output_upload_json.write_text(
                        job_spec.output_volumes[output_volume].to_compute_horde_output_upload(output_volume).json()
                    )

            run_sh = pathlib.Path(script)
            run_sh.write_text(_RUN_TMPL.format(command=job_spec.run, workdir=job_spec.work_dir))
            run_sh.chmod(0o755)

        return script

    def _get_status(self, job: "SkyJobType") -> FallbackJobStatus:
        status = job.status()

        if status is None:
            return FallbackJobStatus.SENT
        elif status in {
            sky.SkyJobStatus.INIT,
            sky.SkyJobStatus.PENDING,
            sky.SkyJobStatus.SETTING_UP,
            sky.SkyJobStatus.RUNNING,
        }:
            return FallbackJobStatus.ACCEPTED
        elif status in {sky.SkyJobStatus.FAILED_DRIVER, sky.SkyJobStatus.FAILED_SETUP}:
            return FallbackJobStatus.REJECTED
        elif status == sky.SkyJobStatus.SUCCEEDED:
            return FallbackJobStatus.COMPLETED
        elif status == sky.SkyJobStatus.FAILED:
            return FallbackJobStatus.FAILED
        else:
            raise NotImplementedError(f"Unsupported status: {status}")

    def _get_result(self, job: "SkyJobType") -> FallbackJobResult:
        stdout = job.output()
        if job.artifacts_dir is not None:
            artifacts = job.download(job.artifacts_dir, max_size=self.MAX_ARTIFACT_SIZE)
        else:
            artifacts = {}

        return FallbackJobResult(stdout=stdout, stderr="", artifacts=artifacts)
