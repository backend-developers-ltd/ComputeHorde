from collections.abc import AsyncIterator, Awaitable, Callable, Coroutine
from typing import TYPE_CHECKING, Any, TypeAlias

from .adaptors.utils import lazy_import_adaptor
from .exceptions import FallbackNotFoundError
from .job import FallbackJob, FallbackJobSpec

if TYPE_CHECKING:
    from .adaptors.sky import SkyCloud as SkyCloudType
    from .adaptors.sky import SkyJob as SkyJobType

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

    def __init__(self, cloud: str, idle_minutes: int = 5, **kwargs: Any) -> None:
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
        job: SkyJobType = sky.SkyJob(self.cloud, job_spec)
        status = job.submit(idle_minutes=self.idle_minutes)
        self._jobs[job.job_uuid] = job

        return FallbackJob(
            self,
            uuid=job.job_uuid,
            status=status,
        )
        # sky.tail_logs(cluster_name, job_id, True)

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

        status = job.status()

        return FallbackJob(
            self,
            uuid=job_uuid,
            status=status,
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
