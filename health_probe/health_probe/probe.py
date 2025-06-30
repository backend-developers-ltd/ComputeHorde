import dataclasses
import logging
from abc import ABC, abstractmethod
from datetime import datetime

from compute_horde_sdk.v1 import ComputeHordeClient, ComputeHordeJobSpec, ComputeHordeJobStatus, ComputeHordeJobTimeoutError, ComputeHordeError, ComputeHordeNotFoundError, ComputeHordeJob

from health_probe.choices import HealthSeverity
from health_probe.exceptions import ProbeImproperlyConfigured, HealthProbeFailed, HealthProbeFailedJobCreation, \
    HealthProbeFailedJobExecution, HealthProbeFailedJobRejected, HealthProbeFailedJobFailed
from health_probe.models import HealthProbeResults, BaseProbeResults

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class ProbeConfig:
    client: ComputeHordeClient
    job_spec: ComputeHordeJobSpec
    job_timeout: float


class Probe(ABC):
    """
    Abstract class for probe implementations.
    A probe should estimate and return the desired statistics about the subnet.
    """

    def __init__(self, config: ProbeConfig):
        self.config = config

    @abstractmethod
    async def _create_job(self, job_spec: ComputeHordeJobSpec) -> ComputeHordeJob:
        """Method should create a Compute Horde job and handle any exceptions."""

    @abstractmethod
    async def _await_job(self, job: ComputeHordeJob, timeout: float) -> None:
        """Method should wait for Compute Horde job completion and handle any exceptions."""

    @abstractmethod
    async def _verify_job_results(self, job: ComputeHordeJob):
        """Method should verify job results and raise an error if the outcome is undesired."""

    @abstractmethod
    async def estimate(self) -> BaseProbeResults:
        """
        Method should perform the test (self.perform_test) and return probe results by interpreting the success
        or failure of the test_coroutine.
        """

    async def perform_test(self):
        # Handling job creation problems.
        job = await self._create_job(self.config.job_spec)
        # Handling job execution problems.
        await self._await_job(job, timeout=self.config.job_timeout)
        # Handle wrong end statuses.
        return await self._verify_job_results(job)


class HealthProbe(Probe):
    """
    The probe estimates the health of the subnet by running an organic task and verifying if it completes successfully.

    See `HealthSeverity` enum for possible results.
    """

    async def _create_job(self, job_spec: ComputeHordeJobSpec) -> ComputeHordeJob:
        logger.info("Creating a job...")
        try:
            return await self.config.client.create_job(job_spec)
        except ComputeHordeNotFoundError as exc:
            raise ProbeImproperlyConfigured(
                "Resource not found while trying to create job. Did you set a proper facilitator url?"
            ) from exc
        except ComputeHordeError as exc:
            raise HealthProbeFailedJobCreation() from exc

    async def _await_job(self, job: ComputeHordeJob, timeout: float) -> None:
        logger.info(f"Waiting {timeout} seconds for the job with uuid: '{job.uuid}'.")
        try:
            return await job.wait(timeout=timeout)
        except ComputeHordeJobTimeoutError as exc:
            raise HealthProbeFailedJobExecution("Execution of the probe's job timed out.") from exc
        except ComputeHordeError as exc:
            raise HealthProbeFailedJobExecution() from exc

    async def _verify_job_results(self, job: ComputeHordeJob) -> None:
        match job.status:
            case ComputeHordeJobStatus.REJECTED:
                raise HealthProbeFailedJobRejected()
            case ComputeHordeJobStatus.FAILED:
                raise HealthProbeFailedJobFailed()

    async def estimate(self) -> BaseProbeResults:
        severity = HealthSeverity.HEALTHY
        start_time = datetime.now()
        try:
            await self.perform_test()
        except HealthProbeFailed as exc:
            logger.warning("The probe has failed.", exc_info=True)
            severity = exc.severity
        except Exception:
            logger.exception("The probe encountered an unexpected exception.")
            severity = HealthSeverity.UNHEALTHY
        return HealthProbeResults(start_time=start_time, end_time=datetime.now(), severity=severity)
