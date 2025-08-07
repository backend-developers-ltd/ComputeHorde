import asyncio
import logging

import packaging.version
from compute_horde import protocol_consts
from compute_horde.protocol_messages import (
    V0HordeFailedRequest,
    V0InitialJobRequest,
    V0JobFailedRequest,
)
from compute_horde.utils import MachineSpecs, Timer
from django.conf import settings
from pydantic import JsonValue

from compute_horde_executor.executor.job_runner import JobRunner
from compute_horde_executor.executor.miner_client import (
    ExecutionResult,
    ExecutorError,
    JobError,
    MinerClient,
)
from compute_horde_executor.executor.utils import get_machine_specs, temporary_process

logger = logging.getLogger(__name__)

CVE_2022_0492_IMAGE = (
    "us-central1-docker.pkg.dev/twistlock-secresearch/public/can-ctr-escape-cve-2022-0492:latest"
)
# Previous CVE: CVE-2024-0132 fixed in 1.16.2
# Current CVE: CVE-2025-23359 fixed in 1.17.4
NVIDIA_CONTAINER_TOOLKIT_MINIMUM_SAFE_VERSION = packaging.version.parse("1.17.4")


class JobDriver:
    """
    NOTE: This used to be the main body of the "run_executor" management command.
    The distinction between JobDriver and JobRunner is not very clear.
    """

    def __init__(self, runner: JobRunner, miner_client: MinerClient, startup_time_limit: int):
        self.runner = runner
        self.miner_client = miner_client
        self.startup_time_limit = startup_time_limit
        self.specs: MachineSpecs | None = None
        self.deadline = Timer()
        # TODO(error propagation): use stages from protocol_consts, or map to them in the error handler
        self.current_stage = "pre-startup"

    @property
    def time_left(self) -> float:
        return self.deadline.time_left()

    async def execute(self):
        async with self.miner_client:  # TODO: Can this hang?
            try:
                await self._execute()

            except JobError as e:
                # TODO(post error propagation): Extract executor errors into ExecutorError.
                # ...we don't want random errors here.
                if e.execution_result:
                    logger.warning(f"Job failed: {e}")
                    await self.send_job_failed(
                        reason=e.reason,
                        message=e.error_message,
                        execution_result=e.execution_result,
                        context={"detail": e.error_detail},
                    )
                else:
                    logger.warning(
                        f"Generic executor failure: {e.reason.value} ({e.error_message})"
                    )
                    await self.send_horde_failed(
                        reason=protocol_consts.HordeFailureReason.GENERIC_EXECUTOR_FAILED,
                        message=e.error_message,
                        context={"detail": e.error_detail},
                    )

            except ExecutorError as e:
                await self.send_horde_failed(
                    reason=e.reason,
                    message=e.message,
                    context=e.context,
                )

            except TimeoutError:
                # TODO(post error propagation): This is not right, only specific timeouts should result in job error.
                # TODO(post error propagation): ...wrap them into JobError with reason=TIMEOUT.
                # TODO(post error propagation): ...otherwise any uncaught timeout will result in a job error.
                logger.error(f"Job timed out during {self.current_stage} stage")
                await self.send_job_failed(
                    reason=protocol_consts.JobFailureReason.TIMEOUT,
                    message=f"Job timed out during {self.current_stage} stage",
                    execution_result=None,
                )

            except BaseException as e:
                # Uncaught exceptions are mapped to horde failures.
                # If they happen a lot, specific horde failure reasons should be defined for them and handled by the
                # ExecutorError handler above.
                await self.send_horde_failed(
                    reason=protocol_consts.HordeFailureReason.UNCAUGHT_EXCEPTION,
                    message="Executor failed with unexpected exception",
                    context={"exception_type": type(e).__qualname__},
                )
                raise

            finally:
                try:
                    await self.runner.clean()
                except Exception as e:
                    logger.error(f"Job cleanup failed: {e}")

    async def send_job_failed(
        self,
        reason: protocol_consts.JobFailureReason,
        message: str,
        execution_result: ExecutionResult | None,
        context: dict[str, JsonValue] | None = None,
    ):
        await self.miner_client.send_job_failed(
            V0JobFailedRequest(
                job_uuid=self.miner_client.job_uuid,
                stage=protocol_consts.JobStage.UNKNOWN,  # TODO(post error propagation): fill this in
                reason=reason,
                message=message,
                docker_process_exit_status=execution_result.return_code
                if execution_result
                else None,
                docker_process_stdout=execution_result.stdout if execution_result else None,
                docker_process_stderr=execution_result.stderr if execution_result else None,
                context=context,
            )
        )

    async def send_horde_failed(
        self,
        reason: protocol_consts.HordeFailureReason,
        message: str,
        context: dict[str, JsonValue] | None = None,
    ):
        await self.miner_client.send_horde_failed(
            V0HordeFailedRequest(
                job_uuid=self.miner_client.job_uuid,
                reported_by=protocol_consts.JobParticipantType.EXECUTOR,
                stage=protocol_consts.JobStage.UNKNOWN,  # TODO(post error propagation): fill this in
                reason=reason,
                message=message,
                context=context,
            )
        )

    async def _execute(self):
        # This limit should be enough to receive the initial job request, which contains further timing details.
        self._set_deadline(self.startup_time_limit, "startup time limit")
        async with asyncio.timeout(self.time_left):
            initial_job_request = await self._startup_stage()
            timing_details = initial_job_request.executor_timing

        if timing_details:
            # With timing details, re-initialize the deadline with leeway
            # It will be extended before each stage down the line
            self._set_deadline(timing_details.allowed_leeway, "allowed leeway")
        elif initial_job_request.timeout_seconds is not None:
            # For single-timeout, this is the full timeout for the whole job
            self._set_deadline(initial_job_request.timeout_seconds, "single-timeout mode")
        else:
            raise JobError(
                "No timing received: either timeout_seconds or timing_details must be set"
            )

        # Download stage
        if timing_details:
            self._extend_deadline(timing_details.download_time_limit, "download time limit")
        async with asyncio.timeout(self.time_left):
            await self._download_stage()

        # Execution stage
        if timing_details:
            self._extend_deadline(timing_details.execution_time_limit, "execution time limit")
            if self.runner.is_streaming_job:
                self._extend_deadline(
                    timing_details.streaming_start_time_limit, "streaming start time limit"
                )
        async with asyncio.timeout(self.time_left):
            await self._execution_stage()

        # Upload stage
        if timing_details:
            self._extend_deadline(timing_details.upload_time_limit, "upload time limit")
        async with asyncio.timeout(self.time_left):
            await self._upload_stage()

        logger.debug(f"Finished with {self.time_left:.2f}s time left")

    def _set_deadline(self, seconds: float, reason: str):
        self.deadline.set_timeout(seconds)
        logger.debug(f"Setting deadline to {seconds}s: {reason}")

    def _extend_deadline(self, seconds: float, reason: str):
        self.deadline.extend_timeout(seconds)
        logger.debug(
            f"Extending deadline by +{seconds:.2f}s to {self.deadline.time_left():.2f}s: {reason}"
        )

    def _enter_stage(self, stage: str):
        self.current_stage = stage
        logger.debug(f"Entering stage {stage} with {self.deadline.time_left():.2f}s time left")

    async def _startup_stage(self) -> V0InitialJobRequest:
        self._enter_stage("startup")

        self.specs = get_machine_specs()
        await self.run_security_checks_or_fail()
        initial_job_request = await self.miner_client.initial_msg
        await self.runner.prepare_initial(initial_job_request)
        await self.miner_client.send_executor_ready()
        if initial_job_request.streaming_details is not None:
            assert initial_job_request.streaming_details.executor_ip is not None
            self.runner.generate_streaming_certificate(
                executor_ip=initial_job_request.streaming_details.executor_ip,
                public_key=initial_job_request.streaming_details.public_key,
            )
        return initial_job_request

    async def _download_stage(self):
        self._enter_stage("download")

        logger.debug("Waiting for full payload")
        full_job_request = await self.miner_client.full_payload
        logger.debug("Full payload received")
        await self.runner.prepare_full(full_job_request)
        await self.runner.download_volume()
        await self.miner_client.send_volumes_ready()

    async def _execution_stage(self):
        self._enter_stage("execution")

        async with self.runner.start_job():
            if self.runner.is_streaming_job:
                assert self.runner.executor_certificate is not None, (
                    "Executor certificate is missing."
                )
                await self.miner_client.send_streaming_job_ready(self.runner.executor_certificate)
        await self.fail_if_execution_unsuccessful()
        await self.miner_client.send_execution_done()

    async def _upload_stage(self):
        self._enter_stage("upload")

        job_result = await self.runner.upload_results()
        job_result.specs = self.specs
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
                protocol_consts.JobFailureReason.SECURITY_CHECK,
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
            stdout, stderr = await docker_process.communicate()
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
                protocol_consts.JobFailureReason.SECURITY_CHECK,
                f'stdout="{stdout.decode()}"\nstderr="{stderr.decode()}',
            )

    async def fail_if_execution_unsuccessful(self):
        assert self.runner.execution_result is not None, "No execution result"

        if self.runner.execution_result.timed_out:
            raise JobError(
                "Job container timed out during execution",
                reason=protocol_consts.JobFailureReason.TIMEOUT,
            )

        if self.runner.execution_result.return_code != 0:
            raise JobError(
                f"Job container exited with non-zero exit code: {self.runner.execution_result.return_code}",
                reason=protocol_consts.JobFailureReason.NONZERO_EXIT_CODE,
                error_detail=f"exit code: {self.runner.execution_result.return_code}",
                execution_result=self.runner.execution_result,
            )
