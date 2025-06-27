import asyncio
import sys
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING, Any

import httpx

from ..models import ComputeHordeJobResult, ComputeHordeJobStatus, InputVolume, OutputVolume
from ..sdk import ComputeHordeJobSpec
from .exceptions import FallbackJobTimeoutError

if TYPE_CHECKING:
    from .client import FallbackClient

if sys.version_info >= (3, 11):  # noqa: UP036
    from typing import Self
else:
    from typing_extensions import Self  # noqa: UP035


JOB_REFRESH_INTERVAL = timedelta(seconds=3)


@dataclass
class FallbackJobSpec:
    """
    Specification of a fallback job to run on the given cloud.
    """

    run: str | None = None
    """A command to execute. It should be a single string, not a list of arguments."""

    envs: Mapping[str, str] | None = None
    """Environment variables to run the job with."""

    work_dir: str = "/"
    """Path to the working directory for the job. Defaults to ``/``."""

    artifacts_dir: str | None = None
    """
    Path of the directory that the job will write its results to.
    Contents of files found in this directory will be returned after the job completes
    as a part of the job result. It should be an absolute path (starting with ``/``).
    """

    input_volumes: Mapping[str, InputVolume] | None = None
    """
    The data to be made available to the job in Docker volumes.
    The keys should be absolute file/directory paths under which you want your data to be available.
    The values should be :class:`InputVolume` instances representing how to obtain the input data.
    For now, input volume paths must start with ``/volume/``.
    """

    output_volumes: Mapping[str, OutputVolume] | None = None
    """
    The data to be read from the Docker volumes after job completion
    and uploaded to the described destinations. Use this for outputs that are too big
    to be treated as ``artifacts``.
    The keys should be absolute file paths under which job output data will be available.
    The values should be :class:`OutputVolume` instances representing how to handle the output data.
    For now, output volume paths must start with ``/output/``.
    """

    cpus: int | float | str | None = None
    """Required number of CPUs for the job."""

    memory: int | float | str | None = None
    """Required amount of memory for the job."""

    accelerators: str | Mapping[str, int] | None = None
    """Required GPUs for the job."""

    disk_size: int | None = None
    """Required amount of memory for the job."""

    ports: int | str | Sequence[str] | None = None
    """Required amount of memory for the job."""

    instance_type: str | None = None
    """Instance type."""

    image_id: str | None = None
    """Image ID. For docker it must be in the form of ``docker:<image>``."""

    region: str | None = None
    """Region to run the job in."""

    zone: str | None = None
    """Zone to run the job in."""

    streaming: bool = False
    """Whether to enable streaming."""

    @classmethod
    def from_job_spec(cls, job_spec: ComputeHordeJobSpec, **kwargs: Any) -> Self:
        if job_spec.executor_class == job_spec.executor_class.__class__.always_on__llm__a6000:
            accelerators = {"RTXA6000": 1}
        else:
            accelerators = None

        return cls(
            run=" ".join(job_spec.args),
            envs=job_spec.env,
            artifacts_dir=job_spec.artifacts_dir,
            input_volumes=job_spec.input_volumes,
            output_volumes=job_spec.output_volumes,
            accelerators=accelerators,
            image_id=f"docker:{job_spec.docker_image}",
            streaming=job_spec.streaming,
            **kwargs,
        )


@dataclass
class FallbackJobResult(ComputeHordeJobResult):
    """
    Result of a fallback job.
    """


FallbackJobStatus = ComputeHordeJobStatus


class FallbackJob:
    """
    The class representing a job running on the SkyPilot cloud.
    Do not construct it directly, always use :class:`FallbackClient`.

    :ivar str uuid: The UUID of the job.
    :ivar FallbackJobStatus status: The status of the job.
    :ivar FallbackJobResult | None result: The result of the job, if it has completed.
    """

    def __init__(
        self,
        client: "FallbackClient",
        uuid: str,
        status: FallbackJobStatus,
        result: FallbackJobResult | None = None,
    ):
        self._client = client
        self.uuid = uuid
        self.status = status
        self.result = result
        self.streaming_server_address = "127.0.0.1"
        self.streaming_server_port: int | None = None

    async def wait(self, timeout: float | None = None) -> None:
        """
        Wait for this job to complete or fail.

        :param timeout: Maximum number of seconds to wait for.
        :raises FallbackJobTimeoutError: If the job does not complete within ``timeout`` seconds.
        """
        start_time = time.monotonic()

        while self.status.is_in_progress():
            if timeout is not None and time.monotonic() - start_time > timeout:
                raise FallbackJobTimeoutError(
                    f"Job {self.uuid} did not complete within {timeout} seconds, last status: {self.status}"
                )
            await asyncio.sleep(JOB_REFRESH_INTERVAL.total_seconds())
            await self.refresh()

    async def wait_for_streaming(self, timeout: int = 120) -> None:
        """
        Wait for the streaming to start.

        :param timeout: Maximum number of seconds to wait for streaming to start.
        :raises FallbackJobTimeoutError: If streaming does not start within timeout seconds.
        """
        start_time = time.time()
        url = f"http://{self.streaming_server_address}:{self.streaming_server_port}/health"

        with httpx.Client() as client:
            while time.time() - start_time < timeout:
                try:
                    resp = client.get(url)
                    if resp.status_code == 200:
                        client.close()
                        return
                except Exception:
                    pass
                await asyncio.sleep(1)

        raise FallbackJobTimeoutError(f"Streaming did not start within {timeout} seconds")

    async def refresh(self) -> None:
        """
        Refresh the current status and result of the job.
        """

        new_job = await self._client.get_job(self.uuid)
        self.status = new_job.status
        self.result = new_job.result

    def __repr__(self) -> str:
        return f"<{self.__class__.__qualname__}: {self.uuid!r}>"
