import asyncio
import contextlib
import logging
import time
from functools import partial
from typing import Literal

from compute_horde.base.output_upload import ZipAndHttpPutUpload
from compute_horde.base.volume import InlineVolume, ZipUrlVolume
from compute_horde.miner_client.base import MinerConnectionError
from compute_horde.mv_protocol.miner_requests import (
    V0DeclineJobRequest,
    V0ExecutorFailedRequest,
    V0ExecutorReadyRequest,
    V0JobFailedRequest,
    V0JobFinishedRequest,
)
from compute_horde.mv_protocol.validator_requests import (
    V0InitialJobRequest,
    V0JobRequest,
)
from django.conf import settings
from pydantic import BaseModel

from compute_horde_validator.validator.models import JobBase, OrganicJob, SystemEvent
from compute_horde_validator.validator.utils import Timer, get_dummy_inline_zip_volume

logger = logging.getLogger(__name__)


class MinerResponse(BaseModel, extra="allow"):
    job_uuid: str
    message_type: None | str
    docker_process_stderr: str
    docker_process_stdout: str


class JobStatusMetadata(BaseModel, extra="allow"):
    comment: str
    miner_response: MinerResponse | None = None


class JobStatusUpdate(BaseModel, extra="forbid"):
    """
    Message sent from validator to facilitator in response to NewJobRequest.
    """

    message_type: str = "V0JobStatusUpdate"
    uuid: str
    status: Literal["failed", "rejected", "accepted", "completed"]
    metadata: JobStatusMetadata | None = None

    @staticmethod
    def from_job(job: JobBase, status, message_type=None) -> "JobStatusUpdate":
        job_status = JobStatusUpdate(
            uuid=str(job.job_uuid),
            status=status,
            metadata=JobStatusMetadata(
                comment=job.comment,
            ),
        )
        if isinstance(job, OrganicJob):
            job_status.metadata.miner_response = MinerResponse(
                job_uuid=str(job.job_uuid),
                message_type=message_type,
                docker_process_stdout=job.stdout,
                docker_process_stderr=job.stderr,
            )
        return job_status


async def save_job_execution_event(subtype: str, long_description: str, data={}, success=False):
    await SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).acreate(
        type=SystemEvent.EventType.MINER_ORGANIC_JOB_SUCCESS
        if success
        else SystemEvent.EventType.MINER_ORGANIC_JOB_FAILURE,
        subtype=subtype,
        long_description=long_description,
        data=data,
    )


async def execute_organic_job(
    miner_client,
    job,
    job_request,
    total_job_timeout: int = 300,
    wait_timeout: int = 300,
    notify_callback=None,
):
    save_event = partial(
        save_job_execution_event,
        data={"miner_hotkey": miner_client.my_hotkey, "job_uuid": str(job.job_uuid)},
    )

    job_state = miner_client.get_job_state(job.job_uuid)
    async with contextlib.AsyncExitStack() as exit_stack:
        try:
            await exit_stack.enter_async_context(miner_client)
        except MinerConnectionError as exc:
            comment = f"Miner connection error: {exc}"
            job.status = OrganicJob.Status.FAILED
            job.comment = comment
            await job.asave()

            logger.warning(comment)
            await save_event(
                subtype=SystemEvent.EventSubType.MINER_CONNECTION_ERROR, long_description=comment
            )
            if notify_callback:
                await notify_callback(JobStatusUpdate.from_job(job, status="failed"))
            return

        job_timer = Timer(timeout=total_job_timeout)

        if job_request.input_url:
            volume = ZipUrlVolume(contents=str(job_request.input_url))
        else:
            volume = InlineVolume(contents=get_dummy_inline_zip_volume())

        await miner_client.send_model(
            V0InitialJobRequest(
                job_uuid=job.job_uuid,
                executor_class=job_request.executor_class,
                base_docker_image_name=job_request.docker_image or None,
                timeout_seconds=total_job_timeout,
                volume_type=volume.volume_type.value,
            )
        )

        try:
            msg = await asyncio.wait_for(
                job_state.miner_ready_or_declining_future,
                timeout=min(job_timer.time_left(), wait_timeout),
            )
        except TimeoutError:
            comment = f"Miner {miner_client.miner_name} timed out while preparing executor for job {job.job_uuid} after {wait_timeout} seconds"
            job.status = OrganicJob.Status.FAILED
            job.comment = comment
            await job.asave()

            logger.warning(comment)
            await save_event(
                subtype=SystemEvent.EventSubType.JOB_NOT_STARTED,
                long_description=comment,
            )
            if notify_callback:
                await notify_callback(JobStatusUpdate.from_job(job, "failed"))
            return

        if isinstance(msg, V0DeclineJobRequest | V0ExecutorFailedRequest):
            comment = f"Miner {miner_client.miner_name} won't do job: {msg.model_dump_json()}"
            job.status = OrganicJob.Status.FAILED
            job.comment = comment
            await job.asave()

            logger.info(comment)
            if notify_callback:
                await notify_callback(JobStatusUpdate.from_job(job, "rejected"))
            return
        elif isinstance(msg, V0ExecutorReadyRequest):
            logger.debug(f"Miner {miner_client.miner_name} ready for job: {msg}")
            if notify_callback:
                await notify_callback(
                    JobStatusUpdate.from_job(job, "accepted", msg.message_type.value)
                )
        else:
            raise ValueError(f"Unexpected msg from miner {miner_client.miner_name}: {msg}")

        docker_run_options_preset = "nvidia_all" if job_request.use_gpu else "none"

        if job_request.output_url:
            output_upload = ZipAndHttpPutUpload(
                url=str(job_request.output_url),
            )
        else:
            output_upload = None

        await miner_client.send_model(
            V0JobRequest(
                job_uuid=job.job_uuid,
                executor_class=job_request.executor_class,
                docker_image_name=job_request.docker_image or None,
                raw_script=job_request.raw_script or None,
                docker_run_options_preset=docker_run_options_preset,
                docker_run_cmd=job_request.get_args(),
                volume=volume,  # TODO: raw scripts
                output_upload=output_upload,
            )
        )
        full_job_sent = time.time()
        try:
            msg = await asyncio.wait_for(
                job_state.miner_finished_or_failed_future,
                timeout=job_timer.time_left(),
            )
            time_took = job_state.miner_finished_or_failed_timestamp - full_job_sent
            logger.info(f"Miner took {time_took} seconds to finish {job.job_uuid}")
        except TimeoutError:
            comment = f"Miner {miner_client.miner_name} timed out after {total_job_timeout} seconds"
            job.status = OrganicJob.Status.FAILED
            job.comment = comment
            await job.asave()

            logger.warning(comment)
            await save_event(
                subtype=SystemEvent.EventSubType.JOB_EXECUTION_TIMEOUT, long_description=comment
            )
            if notify_callback:
                await notify_callback(JobStatusUpdate.from_job(job, "failed"))
            return
        if isinstance(msg, V0JobFailedRequest):
            comment = f"Miner {miner_client.miner_name} failed: {msg.model_dump_json()}"
            job.stdout = msg.docker_process_stdout
            job.stderr = msg.docker_process_stderr
            job.status = OrganicJob.Status.FAILED
            job.comment = comment
            await job.asave()

            logger.info(comment)
            await save_event(subtype=SystemEvent.EventSubType.FAILURE, long_description=comment)
            if notify_callback:
                await notify_callback(
                    JobStatusUpdate.from_job(job, "failed", msg.message_type.value)
                )
            return
        elif isinstance(msg, V0JobFinishedRequest):
            comment = f"Miner {miner_client.miner_name} finished: {msg.model_dump_json()}"
            job.stdout = msg.docker_process_stdout
            job.stderr = msg.docker_process_stderr
            job.status = OrganicJob.Status.COMPLETED
            job.comment = comment
            await job.asave()

            logger.info(comment)
            await save_event(
                subtype=SystemEvent.EventSubType.SUCCESS, long_description=comment, success=True
            )
            if notify_callback:
                await notify_callback(
                    JobStatusUpdate.from_job(job, "completed", msg.message_type.value)
                )
            return
        else:
            comment = f"Unexpected msg from miner {miner_client.miner_name}: {msg}"
            logger.warning(comment)
            await save_event(subtype=SystemEvent.EventSubType.FAILURE, long_description=comment)
            raise ValueError(comment)
