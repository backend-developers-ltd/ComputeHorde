import logging
import time
from collections.abc import Awaitable, Callable
from functools import partial

import sentry_sdk
from asgiref.sync import sync_to_async
from channels.layers import get_channel_layer
from compute_horde.fv_protocol.facilitator_requests import OrganicJobRequest, V2JobRequest
from compute_horde.fv_protocol.validator_requests import (
    HordeFailureDetails,
    JobFailureDetails,
    JobRejectionDetails,
    JobResultDetails,
    JobStatusMetadata,
    JobStatusUpdate,
    StreamingServerDetails,
)
from compute_horde.job_errors import HordeError
from compute_horde.miner_client.organic import (
    MinerRejectedJob,
    MinerReportedHordeFailed,
    MinerReportedJobFailed,
    OrganicJobDetails,
    execute_organic_job_on_miner,
)
from compute_horde.protocol_consts import (
    HordeFailureReason,
    JobFailureReason,
    JobParticipantType,
    JobRejectionReason,
    JobStatus,
)
from compute_horde.protocol_messages import (
    MinerToValidatorMessage,
    V0StreamingJobReadyRequest,
)
from compute_horde_core.executor_class import ExecutorClass
from django.conf import settings
from pydantic import JsonValue

from compute_horde_validator.validator import job_excuses
from compute_horde_validator.validator.allowance.default import allowance
from compute_horde_validator.validator.dynamic_config import aget_config
from compute_horde_validator.validator.models import (
    AdminJobRequest,
    MetagraphSnapshot,
    Miner,
    OrganicJob,
    SystemEvent,
)
from compute_horde_validator.validator.organic_jobs.miner_client import MinerClient
from compute_horde_validator.validator.receipts.default import receipts
from compute_horde_validator.validator.routing.types import JobRoute
from compute_horde_validator.validator.utils import TRUSTED_MINER_FAKE_KEY

logger = logging.getLogger(__name__)


def status_update_from_success(job: OrganicJob) -> JobStatusUpdate:
    metadata = JobStatusMetadata(
        miner_response=JobResultDetails(
            docker_process_stdout=job.stdout,
            docker_process_stderr=job.stderr,
            artifacts=job.artifacts,
            upload_results=job.upload_results,
        ),
    )
    return JobStatusUpdate(
        uuid=str(job.job_uuid),
        status=JobStatus.COMPLETED,
        metadata=metadata,
    )


def status_update_from_miner_rejection(
    job: OrganicJob, rejection: MinerRejectedJob, comment: str | None = None
) -> JobStatusUpdate:
    comment = comment or rejection.msg.message
    metadata = JobStatusMetadata(
        job_rejection_details=JobRejectionDetails(
            rejected_by=JobParticipantType.MINER,
            reason=rejection.msg.reason,
            message=comment,
            context=rejection.msg.context,
        ),
    )
    return JobStatusUpdate(
        uuid=str(job.job_uuid),
        status=JobStatus.REJECTED,
        metadata=metadata,
    )


def status_update_from_miner_job_failure(
    job: OrganicJob, failure: MinerReportedJobFailed
) -> JobStatusUpdate:
    metadata = JobStatusMetadata(
        job_failure_details=JobFailureDetails(
            reason=failure.msg.reason,
            stage=failure.msg.stage,
            message=failure.msg.message,
            context=failure.msg.context,
            docker_process_exit_status=failure.msg.docker_process_exit_status,
            docker_process_stdout=failure.msg.docker_process_stdout,
            docker_process_stderr=failure.msg.docker_process_stderr,
        ),
    )
    return JobStatusUpdate(
        uuid=str(job.job_uuid),
        status=JobStatus.FAILED,
        metadata=metadata,
    )


def status_update_from_miner_horde_failure(
    job: OrganicJob, failure: MinerReportedHordeFailed
) -> JobStatusUpdate:
    metadata = JobStatusMetadata(
        horde_failure_details=HordeFailureDetails(
            reported_by=failure.msg.reported_by,
            reason=failure.msg.reason,
            message=failure.msg.message,
            context=failure.msg.context,
        ),
    )
    return JobStatusUpdate(
        uuid=str(job.job_uuid),
        status=JobStatus.HORDE_FAILED,
        metadata=metadata,
    )


def status_update_from_horde_error(job: OrganicJob, error: HordeError) -> JobStatusUpdate:
    metadata = JobStatusMetadata(
        horde_failure_details=HordeFailureDetails(
            reported_by=JobParticipantType.VALIDATOR,
            reason=error.reason,
            message=error.message,
            context=error.context,
        ),
    )
    return JobStatusUpdate(
        uuid=str(job.job_uuid),
        status=JobStatus.FAILED,
        metadata=metadata,
    )


async def save_job_execution_event(
    subtype: str, long_description: str, data: JsonValue = None, success: bool = False
) -> None:
    await SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).acreate(
        type=SystemEvent.EventType.MINER_ORGANIC_JOB_SUCCESS
        if success
        else SystemEvent.EventType.MINER_ORGANIC_JOB_FAILURE,
        subtype=subtype,
        long_description=long_description,
        data=data or {},
    )


async def _dummy_notify_callback(_: JobStatusUpdate) -> None:
    pass


async def _get_current_block() -> int:
    return (await MetagraphSnapshot.aget_latest()).block


async def execute_organic_job_request(
    job_request: OrganicJobRequest, job_route: JobRoute
) -> OrganicJob:
    if (
        job_route.miner.hotkey_ss58 == settings.DEBUG_MINER_KEY
        and settings.DEBUG_MINER_ADDRESS
        and settings.DEBUG_MINER_PORT
    ):
        miner_ip = settings.DEBUG_MINER_ADDRESS
        miner_port = settings.DEBUG_MINER_PORT
        ip_type = 4
        on_trusted_miner = False
    elif job_route.miner.hotkey_ss58 == TRUSTED_MINER_FAKE_KEY:
        miner_ip = settings.TRUSTED_MINER_ADDRESS
        miner_port = settings.TRUSTED_MINER_PORT
        ip_type = 4
        on_trusted_miner = True
    else:
        miner_ip = job_route.miner.address
        miner_port = job_route.miner.port
        ip_type = job_route.miner.ip_version
        on_trusted_miner = False

    if settings.DEBUG_USE_MOCK_BLOCK_NUMBER:
        block = 5136476 + int((time.time() - 1742076533) / 12)
    else:
        block = await _get_current_block()

    miner = await Miner.objects.aget(hotkey=job_route.miner.hotkey_ss58)
    job = await OrganicJob.objects.acreate(
        job_uuid=str(job_request.uuid),
        miner=miner,
        miner_address=miner_ip,
        miner_address_ip_version=ip_type,
        miner_port=miner_port,
        executor_class=job_request.executor_class,
        job_description="User job from facilitator",
        block=block,
        on_trusted_miner=on_trusted_miner,
        streaming_details=job_request.streaming_details.model_dump()
        if job_request.streaming_details
        else None,
        allowance_reservation_id=job_route.allowance_reservation_id,
    )

    miner_client = MinerClient(
        miner_hotkey=job_route.miner.hotkey_ss58,
        miner_address=job.miner_address,
        miner_port=job.miner_port,
        job_uuid=str(job.job_uuid),
        my_keypair=settings.BITTENSOR_WALLET().hotkey,
    )

    async def job_status_callback(status_update: JobStatusUpdate):
        await get_channel_layer().send(
            f"job_status_updates__{status_update.uuid}",
            {"type": "job_status_update", "payload": status_update.model_dump(mode="json")},
        )

    await drive_organic_job(
        miner_client,
        job,
        job_request,
        notify_callback=job_status_callback,
    )

    return job


async def drive_organic_job(
    miner_client: MinerClient,
    job: OrganicJob,
    job_request: OrganicJobRequest | AdminJobRequest,
    notify_callback: Callable[[JobStatusUpdate], Awaitable[None]] | None = None,
) -> bool:
    """
    Execute an organic job on a miner client.
    Returns True if the job was successfully executed, False otherwise.
    """
    if notify_callback is None:
        notify_callback = _dummy_notify_callback

    if job.on_trusted_miner and await aget_config("DYNAMIC_DISABLE_TRUSTED_ORGANIC_JOB_EVENTS"):
        # ignore trusted system events
        async def save_event(*args, **kwargs):
            pass
    else:
        data: JsonValue = {"job_uuid": str(job.job_uuid), "miner_hotkey": miner_client.my_hotkey}
        save_event = partial(save_job_execution_event, data=data)

    def status_callback(status: JobStatus):
        async def relay(msg: MinerToValidatorMessage) -> None:
            await notify_callback(JobStatusUpdate(uuid=str(job.job_uuid), status=status))

        return relay

    async def streaming_ready_callback(msg: V0StreamingJobReadyRequest) -> None:
        await notify_callback(
            JobStatusUpdate(
                uuid=str(job.job_uuid),
                status=JobStatus.STREAMING_READY,
                metadata=JobStatusMetadata(
                    streaming_details=StreamingServerDetails(
                        streaming_server_cert=msg.public_key,
                        streaming_server_address=msg.ip,
                        streaming_server_port=msg.port,
                    ),
                ),
            )
        )

    miner_client.notify_job_accepted = status_callback(JobStatus.ACCEPTED)  # type: ignore[method-assign]
    miner_client.notify_executor_ready = status_callback(JobStatus.EXECUTOR_READY)  # type: ignore[method-assign]
    miner_client.notify_volumes_ready = status_callback(JobStatus.VOLUMES_READY)  # type: ignore[method-assign]
    miner_client.notify_execution_done = status_callback(JobStatus.EXECUTION_DONE)  # type: ignore[method-assign]
    miner_client.notify_streaming_readiness = streaming_ready_callback  # type: ignore[method-assign]
    # TODO: remove method assignment above and properly handle notify_* cases

    artifacts_dir = job_request.artifacts_dir if isinstance(job_request, V2JobRequest) else None
    job_details = OrganicJobDetails(
        job_uuid=str(job.job_uuid),  # TODO: fix uuid field in AdminJobRequest
        executor_class=ExecutorClass(job_request.executor_class),
        docker_image=job_request.docker_image,
        docker_run_options_preset="nvidia_all" if job_request.use_gpu else "none",
        docker_run_cmd=job_request.get_args(),
        total_job_timeout=job_request.timeout
        if isinstance(job_request, AdminJobRequest)
        else OrganicJobDetails.total_job_timeout,
        volume=job_request.volume,
        output=job_request.output_upload,
        artifacts_dir=artifacts_dir,
        job_timing=OrganicJobDetails.TimingDetails(
            allowed_leeway=await aget_config("DYNAMIC_ORGANIC_JOB_ALLOWED_LEEWAY_TIME"),
            download_time_limit=job_request.download_time_limit,
            execution_time_limit=job_request.execution_time_limit,
            upload_time_limit=job_request.upload_time_limit,
            streaming_start_time_limit=job_request.streaming_start_time_limit,
        )
        if isinstance(job_request, V2JobRequest)
        else None,
        streaming_details=job.streaming_details,
    )

    try:
        stdout, stderr, artifacts, upload_results = await execute_organic_job_on_miner(
            miner_client,
            job_details,
            reservation_time_limit=await aget_config("DYNAMIC_EXECUTOR_RESERVATION_TIME_LIMIT"),
            executor_startup_time_limit=await aget_config("DYNAMIC_EXECUTOR_STARTUP_TIME_LIMIT"),
        )

        if job.allowance_reservation_id is not None:
            try:
                await sync_to_async(allowance().spend_allowance)(job.allowance_reservation_id)
                logger.info(
                    "Successfully spent allowance for reservation %s for job %s",
                    job.allowance_reservation_id,
                    job.job_uuid,
                )
            except Exception as e:
                logger.error(
                    "Failed to spend allowance for reservation %s for job %s: %s",
                    job.allowance_reservation_id,
                    job.job_uuid,
                    e,
                    exc_info=True,
                )

        comment = f"Miner {miner_client.miner_name} hotkey={job.miner.hotkey} finished: {stdout=} {stderr=}"
        job.stdout = stdout
        job.stderr = stderr
        job.artifacts = artifacts
        job.upload_results = upload_results
        job.status = OrganicJob.Status.COMPLETED
        job.comment = comment
        await job.asave()
        logger.info(comment)
        await save_event(
            subtype=SystemEvent.EventSubType.SUCCESS, long_description=comment, success=True
        )
        await notify_callback(status_update_from_success(job))
        return True

    except MinerRejectedJob as rejection:
        # The only valid reason for rejection is being busy and providing the receipts to prove it
        if rejection.msg.reason != JobRejectionReason.BUSY:
            comment = rejection.msg.message
            status = (
                OrganicJob.Status.FAILED
            )  # As far as the validator is concerned, the job is as good as failed
            system_event_subtype = SystemEvent.EventSubType.JOB_REJECTED
        else:  # rejection.msg.reason == JobRejectionReason.BUSY
            job_started_receipt = await receipts().get_job_started_receipt_by_uuid(
                str(job.job_uuid)
            )
            job_request_time = job_started_receipt.timestamp
            valid_excuses = await job_excuses.filter_valid_excuse_receipts(
                receipts_to_check=rejection.msg.receipts or [],
                check_time=job_request_time,
                declined_job_uuid=str(job.job_uuid),
                declined_job_executor_class=ExecutorClass(job.executor_class),
                declined_job_is_synthetic=False,
                minimum_validator_stake_for_excuse=await aget_config(
                    "DYNAMIC_MINIMUM_VALIDATOR_STAKE_FOR_EXCUSE"
                ),
                miner_hotkey=job.miner.hotkey,
            )
            expected_executor_count = await job_excuses.get_expected_miner_executor_count(
                check_time=job_request_time,
                miner_hotkey=job.miner.hotkey,
                executor_class=ExecutorClass(job.executor_class),
            )
            if len(valid_excuses) >= expected_executor_count:
                comment = "Miner properly excused job"
                status = OrganicJob.Status.EXCUSED
                system_event_subtype = SystemEvent.EventSubType.JOB_EXCUSED
            else:
                comment = "Miner failed to excuse job"
                status = OrganicJob.Status.FAILED
                system_event_subtype = SystemEvent.EventSubType.JOB_REJECTED

        logger.info(comment)
        job.comment = comment
        job.status = status
        await job.asave()
        await save_event(subtype=system_event_subtype, long_description=comment)
        status_update = status_update_from_miner_rejection(job, rejection, comment)
        await notify_callback(status_update)

    except MinerReportedJobFailed as failure:
        job.status = OrganicJob.Status.FAILED
        job.comment = failure.msg.message
        await job.asave()
        await save_event(
            subtype=_job_event_subtype_map.get(
                failure.msg.reason, SystemEvent.EventSubType.GENERIC_JOB_FAILURE
            ),
            long_description=failure.msg.message,
        )
        status_update = status_update_from_miner_job_failure(job, failure)
        await notify_callback(status_update)

    except MinerReportedHordeFailed as failure:
        job.status = OrganicJob.Status.FAILED
        job.comment = failure.msg.message
        await job.asave()
        await save_event(
            subtype=_horde_event_subtype_map.get(
                failure.msg.reason, SystemEvent.EventSubType.GENERIC_ERROR
            ),
            long_description=failure.msg.message,
        )
        status_update = status_update_from_miner_horde_failure(job, failure)
        await notify_callback(status_update)

    except Exception as e:
        sentry_sdk.capture_exception(e)
        e = HordeError.wrap_unhandled(e)
        comment = str(e)
        logger.warning(comment)
        job.status = OrganicJob.Status.FAILED
        job.comment = comment
        await job.asave()

        event_subtype = _horde_event_subtype_map.get(
            e.reason, SystemEvent.EventSubType.GENERIC_ERROR
        )
        await save_event(subtype=event_subtype, long_description=comment)

        status_update = status_update_from_horde_error(job, e)
        await notify_callback(status_update)

    # Undo allowance reservation for any job failure
    if job.allowance_reservation_id is not None:
        try:
            await sync_to_async(allowance().undo_allowance_reservation)(
                job.allowance_reservation_id
            )
            logger.info(
                "Successfully undid allowance reservation %s for failed job %s",
                job.allowance_reservation_id,
                job.job_uuid,
            )
        except Exception as e:
            logger.error(
                "Failed to undo allowance reservation %s for failed job %s: %s",
                job.allowance_reservation_id,
                job.job_uuid,
                e,
                exc_info=True,
            )

    return False


_job_event_subtype_map: dict[JobFailureReason, str] = {
    JobFailureReason.UNKNOWN: SystemEvent.EventSubType.GENERIC_ERROR,
    JobFailureReason.TIMEOUT: SystemEvent.EventSubType.JOB_TIMEOUT,
    JobFailureReason.NONZERO_RETURN_CODE: SystemEvent.EventSubType.JOB_PROCESS_NONZERO_EXIT_CODE,
    JobFailureReason.DOWNLOAD_FAILED: SystemEvent.EventSubType.JOB_VOLUME_DOWNLOAD_FAILED,
    JobFailureReason.UPLOAD_FAILED: SystemEvent.EventSubType.JOB_RESULT_UPLOAD_FAILED,
}

_horde_event_subtype_map: dict[HordeFailureReason, str] = {
    HordeFailureReason.MINER_CONNECTION_FAILED: SystemEvent.EventSubType.MINER_CONNECTION_ERROR,
    HordeFailureReason.INITIAL_RESPONSE_TIMED_OUT: SystemEvent.EventSubType.ERROR_VALIDATOR_REPORTED_TIMEOUT,
    HordeFailureReason.EXECUTOR_READINESS_RESPONSE_TIMED_OUT: SystemEvent.EventSubType.ERROR_VALIDATOR_REPORTED_TIMEOUT,
    HordeFailureReason.STREAMING_JOB_READY_TIMED_OUT: SystemEvent.EventSubType.ERROR_VALIDATOR_REPORTED_TIMEOUT,
    HordeFailureReason.VOLUMES_TIMED_OUT: SystemEvent.EventSubType.ERROR_VALIDATOR_REPORTED_TIMEOUT,
    HordeFailureReason.EXECUTION_TIMED_OUT: SystemEvent.EventSubType.ERROR_VALIDATOR_REPORTED_TIMEOUT,
    HordeFailureReason.FINAL_RESPONSE_TIMED_OUT: SystemEvent.EventSubType.ERROR_VALIDATOR_REPORTED_TIMEOUT,
    HordeFailureReason.SECURITY_CHECK_FAILED: SystemEvent.EventSubType.ERROR_FAILED_SECURITY_CHECK,
    HordeFailureReason.STREAMING_FAILED: SystemEvent.EventSubType.GENERIC_ERROR,
    HordeFailureReason.UNHANDLED_EXCEPTION: SystemEvent.EventSubType.GENERIC_ERROR,
    HordeFailureReason.UNKNOWN: SystemEvent.EventSubType.GENERIC_ERROR,
}
