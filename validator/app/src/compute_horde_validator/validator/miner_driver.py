import asyncio
import contextlib
import logging
import time
from typing import Literal

from compute_horde.miner_client.base import MinerConnectionError
from compute_horde.mv_protocol.miner_requests import (
    V0DeclineJobRequest,
    V0ExecutorFailedRequest,
    V0ExecutorReadyRequest,
    V0JobFailedRequest,
    V0JobFinishedRequest,
)
from compute_horde.mv_protocol.validator_requests import (
    OutputUpload,
    OutputUploadType,
    V0InitialJobRequest,
    V0JobRequest,
    Volume,
    VolumeType,
)
from pydantic import BaseModel

from compute_horde_validator.validator.models import OrganicJob
from compute_horde_validator.validator.utils import Timer, get_dummy_inline_zip_volume

logger = logging.getLogger(__name__)


class MinerResponse(BaseModel, extra="allow"):
    job_uuid: str
    message_type: str
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


async def run_miner_job(
    miner_client,
    job,
    job_request,
    total_job_timeout: int = 300,
    wait_timeout: int = 300,
    notify_callback=None,
):
    job_state = miner_client.get_job_state(job.job_uuid)
    async with contextlib.AsyncExitStack() as exit_stack:
        try:
            await exit_stack.enter_async_context(miner_client)
        except MinerConnectionError as exc:
            comment = f"Miner connection error: {exc}"
            logger.error(comment)
            if notify_callback:
                await notify_callback(
                    JobStatusUpdate(
                        uuid=job.job_uuid,
                        status="failed",
                        metadata=JobStatusMetadata(comment=comment),
                    )
                )
            job.status = OrganicJob.Status.FAILED
            job.comment = comment
            await job.asave()
            return

        job_timer = Timer(timeout=total_job_timeout)
        if job_request.input_url:
            volume = Volume(volume_type=VolumeType.zip_url, contents=str(job_request.input_url))
        else:
            volume = Volume(volume_type=VolumeType.inline, contents=get_dummy_inline_zip_volume())

        await miner_client.send_model(
            V0InitialJobRequest(
                job_uuid=job.job_uuid,
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
            logger.error(
                f"Miner {miner_client.miner_name} timed out out while preparing executor for job {job.job_uuid}"
                f" after {wait_timeout} seconds"
            )
            if notify_callback:
                await notify_callback(
                    JobStatusUpdate(
                        uuid=job.job_uuid,
                        status="failed",
                        metadata=JobStatusMetadata(
                            comment=f"Miner timed out while preparing executor after {wait_timeout} seconds",
                        ),
                    )
                )
            job.status = OrganicJob.Status.FAILED
            job.comment = "Miner timed out while preparing executor"
            await job.asave()
            return

        if isinstance(msg, V0DeclineJobRequest | V0ExecutorFailedRequest):
            logger.info(f"Miner {miner_client.miner_name} won't do job: {msg}")
            if notify_callback:
                await notify_callback(
                    JobStatusUpdate(
                        uuid=job.job_uuid,
                        status="rejected",
                        metadata=JobStatusMetadata(
                            comment=f"Miner didn't accept the job. Miner sent {msg.message_type}"
                        ),
                    )
                )
            job.status = OrganicJob.Status.FAILED
            job.comment = f"Miner didn't accept the job saying: {msg.model_dump_json()}"
            await job.asave()
            return
        elif isinstance(msg, V0ExecutorReadyRequest):
            logger.debug(f"Miner {miner_client.miner_name} ready for job: {msg}")
            if notify_callback:
                await notify_callback(
                    JobStatusUpdate(
                        uuid=job.job_uuid,
                        status="accepted",
                        metadata=JobStatusMetadata(comment="Miner accepted job"),
                    )
                )
        else:
            raise ValueError(f"Unexpected msg: {msg}")

        docker_run_options_preset = "nvidia_all" if job_request.use_gpu else "none"

        if job_request.output_url:
            output_upload = OutputUpload(
                output_upload_type=OutputUploadType.zip_and_http_put,
                url=str(job_request.output_url),
            )
        else:
            output_upload = None

        await miner_client.send_model(
            V0JobRequest(
                job_uuid=job.job_uuid,
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
            logger.error(
                f"Miner {miner_client.miner_name} timed out after {total_job_timeout} seconds"
            )
            if notify_callback:
                await notify_callback(
                    JobStatusUpdate(
                        uuid=job.job_uuid,
                        status="failed",
                        metadata=JobStatusMetadata(
                            comment=f"Miner timed out after {total_job_timeout} seconds"
                        ),
                    )
                )
            job.status = OrganicJob.Status.FAILED
            job.comment = "Miner timed out"
            await job.asave()
            return
        if isinstance(msg, V0JobFailedRequest):
            logger.info(f"Miner {miner_client.miner_name} failed: {msg}")
            if notify_callback:
                await notify_callback(
                    JobStatusUpdate(
                        uuid=job.job_uuid,
                        status="failed",
                        metadata=JobStatusMetadata(
                            comment="Miner failed",
                            miner_response=MinerResponse(
                                job_uuid=msg.job_uuid,
                                message_type=msg.message_type.value,
                                docker_process_stderr=msg.docker_process_stderr,
                                docker_process_stdout=msg.docker_process_stdout,
                            ),
                        ),
                    )
                )
            job.stdout = msg.docker_process_stdout
            job.stderr = msg.docker_process_stderr
            job.status = OrganicJob.Status.FAILED
            job.comment = f"Miner failed: {msg.model_dump_json()}"
            await job.asave()
            return
        elif isinstance(msg, V0JobFinishedRequest):
            logger.info(f"Miner {miner_client.miner_name} finished: {msg}")
            if notify_callback:
                await notify_callback(
                    JobStatusUpdate(
                        uuid=job.job_uuid,
                        status="completed",
                        metadata=JobStatusMetadata(
                            comment="Miner finished",
                            miner_response=MinerResponse(
                                job_uuid=msg.job_uuid,
                                message_type=msg.message_type.value,
                                docker_process_stderr=msg.docker_process_stderr,
                                docker_process_stdout=msg.docker_process_stdout,
                            ),
                        ),
                    )
                )
            job.stdout = msg.docker_process_stdout
            job.stderr = msg.docker_process_stderr
            job.status = OrganicJob.Status.COMPLETED
            job.comment = f"Miner finished: {msg.model_dump_json()}"
            await job.asave()
            return
        else:
            raise ValueError(f"Unexpected msg: {msg}")
