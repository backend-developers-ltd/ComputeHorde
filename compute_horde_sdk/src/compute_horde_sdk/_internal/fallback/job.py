import asyncio
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Self

from ..models import ComputeHordeJobResult, ComputeHordeJobStatus
from ..sdk import ComputeHordeJobSpec
from .exceptions import FallbackJobTimeoutError

if TYPE_CHECKING:
    from .client import FallbackClient


JOB_REFRESH_INTERVAL = timedelta(seconds=3)


@dataclass
class FallbackJobSpec:
    """
    Specification of a fallback job to run on the given cloud.
    """

    run: str | None = None
    envs: Mapping[str, str] | None = None
    artifacts_dir: str | None = None
    input_volumes: Mapping[str, str] | None = None
    output_volumes: Mapping[str, str] | None = None

    cpus: int | float | str | None = None
    memory: int | float | str | None = None
    accelerators: str | Mapping[str, int] | None = None
    disk_size: int | None = None
    ports: int | str | Sequence[str] | None = None

    instance_type: str | None = None
    image_id: str | None = None

    region: str | None = None
    zone: str | None = None

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
            accelerators=accelerators,
            image_id=f"docker:{job_spec.docker_image}",
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

    async def refresh(self) -> None:
        """
        Refresh the current status and result of the job.
        """

        new_job = await self._client.get_job(self.uuid)
        self.status = new_job.status
        self.result = new_job.result

    def __repr__(self) -> str:
        return f"<{self.__class__.__qualname__}: {self.uuid!r}>"
