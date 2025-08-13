import asyncio
import logging

import packaging.version
from compute_horde.protocol_messages import V0InitialJobRequest, V0JobFailedRequest
from compute_horde.utils import MachineSpecs, Timer
from django.conf import settings

from compute_horde_executor.executor.job_runner import BaseJobRunner
from compute_horde_executor.executor.miner_client import JobError, MinerClient
from compute_horde_executor.executor.utils import (
    docker_container_wrapper,
    get_docker_container_outputs,
    get_machine_specs,
)

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

    def __init__(self, runner: BaseJobRunner, miner_client: MinerClient, startup_time_limit: int):
        self.runner = runner
        self.miner_client = miner_client
        self.startup_time_limit = startup_time_limit
        self.specs: MachineSpecs | None = None

    async def execute(self):
        async with self.miner_client:  # TODO: Can this hang?
            try:
                try:
                    # This limit should be enough to receive the initial job request, which contains further timing
                    # details.
                    async with asyncio.timeout(self.startup_time_limit):
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

                if initial_job_request.streaming_details is not None:
                    assert initial_job_request.streaming_details.executor_ip is not None
                    self.runner.generate_streaming_certificate(
                        executor_ip=initial_job_request.streaming_details.executor_ip,
                        public_key=initial_job_request.streaming_details.public_key,
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
                        timeout_extend = timing_details.execution_time_limit
                        if self.runner.is_streaming_job:
                            timeout_extend += timing_details.streaming_start_time_limit
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

            finally:
                try:
                    await self.runner.clean()
                except Exception as e:
                    logger.error(f"Job cleanup failed: {e}")

    async def _startup_stage(self) -> V0InitialJobRequest:
        self.specs = await get_machine_specs()
        await self.run_security_checks_or_fail()
        initial_job_request = await self.miner_client.initial_msg
        await self.runner.prepare_initial(initial_job_request)
        await self.miner_client.send_executor_ready()
        return initial_job_request

    async def _download_stage(self):
        logger.debug("Waiting for full payload")
        full_job_request = await self.miner_client.full_payload
        logger.debug("Full payload received")
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
        await self.fail_if_execution_unsuccessful()
        await self.miner_client.send_execution_done()

    async def _upload_stage(self):
        job_result = await self.runner.upload_results()
        job_result.specs = self.specs
        await self.miner_client.send_result(job_result)

    async def run_security_checks_or_fail(self):
        await self.run_cve_2022_0492_check_or_fail()
        if not settings.DEBUG_NO_GPU_MODE:
            await self.run_nvidia_toolkit_version_check_or_fail()

    async def run_cve_2022_0492_check_or_fail(self):
        async with docker_container_wrapper(
            image=CVE_2022_0492_IMAGE, auto_remove=True
        ) as docker_container:
            results = await docker_container.wait()
            return_code = results["StatusCode"]
            stdout, stderr = await get_docker_container_outputs(docker_container)

        if return_code != 0:
            raise JobError(
                "CVE-2022-0492 check failed",
                error_detail=f'stdout="{stdout}"\nstderr="{stderr}"',
            )

        expected_output = "Contained: cannot escape via CVE-2022-0492"
        if expected_output not in stdout:
            raise JobError(
                f'CVE-2022-0492 check failed: "{expected_output}" not in stdout.',
                V0JobFailedRequest.ErrorType.SECURITY_CHECK,
                f'stdout="{stdout}"\nstderr="{stderr}"',
            )

    async def run_nvidia_toolkit_version_check_or_fail(self):
        async with docker_container_wrapper(
            image="ubuntu:latest",
            command=["bash", "-c", "nvidia-container-toolkit --version"],
            auto_remove=True,
            HostConfig={
                "Privileged": True,
                "Binds": [
                    "/:/host:ro",
                    "/usr/bin:/usr/bin",
                    "/usr/lib:/usr/lib",
                ],
            },
        ) as docker_container:
            results = await docker_container.wait()
            return_code = results["StatusCode"]
            stdout, stderr = await get_docker_container_outputs(docker_container)

        if return_code != 0:
            raise JobError(
                f"nvidia-container-toolkit check failed: exit code {return_code}",
                error_detail=f'stdout="{stdout}"\nstderr="{stderr}"',
            )

        lines = stdout.splitlines()
        if not lines:
            raise JobError(
                "nvidia-container-toolkit check failed: no output from nvidia-container-toolkit",
                error_detail=f'stdout="{stdout}"\nstderr="{stderr}"',
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
                f'stdout="{stdout}"\nstderr="{stderr}',
            )

    async def fail_if_execution_unsuccessful(self):
        assert self.runner.execution_result is not None, "No execution result"

        if self.runner.execution_result.timed_out:
            raise JobError(
                "Job container timed out during execution",
                error_type=V0JobFailedRequest.ErrorType.TIMEOUT,
            )

        if self.runner.execution_result.return_code != 0:
            raise JobError(
                f"Job container exited with non-zero exit code: {self.runner.execution_result.return_code}",
                error_type=V0JobFailedRequest.ErrorType.NONZERO_EXIT_CODE,
                error_detail=f"exit code: {self.runner.execution_result.return_code}",
                execution_result=self.runner.execution_result,
            )
