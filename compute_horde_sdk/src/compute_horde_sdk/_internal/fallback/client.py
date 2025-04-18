import asyncio
import logging
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Coroutine
from functools import cache
from typing import TYPE_CHECKING, Any, TypeAlias

from .adaptors.utils import lazy_import_adaptor
from .exceptions import FallbackNotFoundError
from .job import FallbackJob, FallbackJobResult, FallbackJobSpec, FallbackJobStatus
from .utils import PackageAnalyzer, get_tempdir

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


class FallbackClient:
    """
    A fallback client that provides the same API as ComputeHordeClient.
    """

    DEFAULT_MAX_JOB_RUN_ATTEMPTS = 3
    MAX_ARTIFACT_SIZE = 1_000_000

    def __init__(self, cloud: str, idle_minutes: int = 15, **kwargs: Any) -> None:
        try:
            self.cloud: SkyCloudType = sky.SkyCloud(cloud, **kwargs)
        except ModuleNotFoundError:
            raise ModuleNotFoundError(
                "The fallback extra has not been installed. "
                "Please install it with `pip install compute-horde-sdk[fallback]`."
            ) from None
        self.idle_minutes = idle_minutes

        self._jobs: dict[str, SkyJobType] = {}

    async def create_job(self, job_spec: FallbackJobSpec) -> FallbackJob:
        """
        Run a fallback job in the SkyPilot cluster. This method does not retry a failed job.
        Use :meth:`run_until_complete` if you want failed jobs to be automatically retried.

        :param job_spec: Job specification to run.
        :return: A :class:`FallbackJob` class instance representing the created job.
        """
        logger.info("Running job")

        workdir = get_tempdir()
        setup = self._get_setup(str(workdir))
        job: SkyJobType = sky.SkyJob(
            cloud=self.cloud,
            workdir=workdir,
            setup=setup,
            run=job_spec.run,
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
        job.submit(idle_minutes=self.idle_minutes)
        self._jobs[job.job_uuid] = job

        return FallbackJob(
            self,
            uuid=job.job_uuid,
            status=FallbackJobStatus.SENT,
        )

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

            if 0 < max_attempts <= attempt:
                return job

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

    @cache
    def _get_setup(self, workdir: str) -> str:
        pa = PackageAnalyzer("compute-horde-sdk")
        source = pa.to_source(temp_dir=workdir)
        # TODO(maciek): use already preinstalled uv (by SkyPilot) for managing python and virtualenv
        return f"pip install {source}"

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

        return FallbackJobResult(stdout=stdout, artifacts=artifacts)
