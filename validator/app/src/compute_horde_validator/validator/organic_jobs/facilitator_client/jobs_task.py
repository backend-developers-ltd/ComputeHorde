import logging

import pydantic
from compute_horde.fv_protocol.facilitator_requests import (
    OrganicJobRequest,
    V0JobCheated,
)
from compute_horde.fv_protocol.validator_requests import (
    JobStatusUpdate,
)
from compute_horde.protocol_consts import (
    JobStatus,
)
from compute_horde_core.signature import SignedRequest, verify_signature
from django.conf import settings
from django.utils.timezone import now

from compute_horde_validator.validator.dynamic_config import aget_config
from compute_horde_validator.validator.models import (
    MinerBlacklist,
    OrganicJob,
    SystemEvent,
    ValidatorWhitelist,
)
from compute_horde_validator.validator.organic_jobs import blacklist
from compute_horde_validator.validator.organic_jobs.blacklist import report_miner_failed_job
from compute_horde_validator.validator.routing.default import routing
from compute_horde_validator.validator.tasks import (
    execute_organic_job_request_on_worker,
    slash_collateral_task,
)

from .constants import JOB_STATUS_UPDATE_CHANNEL
from .exceptions import LocalChannelSendError
from .util import safe_send_local_message

logger = logging.getLogger(__name__)


class JobRequestVerificationFailed(Exception):
    def __init__(self, message: str):
        self.message = message
        super().__init__(message)


class InvalidJobRequestFormat(Exception):
    def __init__(self, message: str):
        self.message = message
        super().__init__(message)


async def verify_request_or_fail(job_request: SignedRequest) -> None:
    """
    Check that the signer is in validator whitelist and that the signature is
    valid.
    """
    if job_request.signature is None:
        raise JobRequestVerificationFailed("Signature is empty")

    signature = job_request.signature
    signer = signature.signatory
    signed_payload = job_request.get_signed_payload()

    my_keypair = settings.BITTENSOR_WALLET().get_hotkey()
    if signer != my_keypair.ss58_address:
        whitelisted = await ValidatorWhitelist.objects.filter(hotkey=signer).aexists()
        if not whitelisted:
            raise JobRequestVerificationFailed(f"Signatory {signer} is not in validator whitelist")

    try:
        verify_signature(signed_payload, signature)
    except Exception as e:
        raise JobRequestVerificationFailed("Bad signature") from e


async def process_miner_cheat_report(cheated_job_request: V0JobCheated) -> None:
    """
    Process a cheated job report and blacklist the miner.
    """
    try:
        await verify_request_or_fail(cheated_job_request)
    except Exception as e:
        logger.warning(f"Failed to verify signed payload: {e} - will ignore")
        return
    job_uuid = cheated_job_request.job_uuid
    try:
        job = await OrganicJob.objects.prefetch_related("miner").aget(job_uuid=job_uuid)
    except OrganicJob.DoesNotExist:
        logger.error(f"Job {job_uuid} reported for cheating does not exist")
        return

    if job.cheated:
        logger.warning(f"Job {job_uuid} already marked as cheated - ignoring")
        return

    if job.status != OrganicJob.Status.COMPLETED:
        logger.info(f"Job {job_uuid} reported for cheating is not complete yet")
        return

    job.cheated = True
    job.cheat_reported_at = now()
    await job.asave(update_fields=["cheated", "cheat_reported_at"])

    blacklist_time = await aget_config("DYNAMIC_JOB_CHEATED_BLACKLIST_TIME_SECONDS")
    await blacklist.blacklist_miner(job, MinerBlacklist.BlacklistReason.JOB_CHEATED, blacklist_time)

    event_data = {
        "job_uuid": str(job.job_uuid),
        "miner_hotkey": job.miner.hotkey,
        "trusted_job_uuid": cheated_job_request.trusted_job_uuid,
        "cheat_details": cheated_job_request.details,
    }

    await SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).acreate(
        type=SystemEvent.EventType.MINER_ORGANIC_JOB_FAILURE,
        subtype=SystemEvent.EventSubType.JOB_CHEATED,
        long_description="Job was reported as cheated",
        data=event_data,
    )

    if not job.slashed:
        slash_collateral_task.delay(str(job.job_uuid))


async def job_request_task(job_request: str) -> None:
    """
    Select an appropriate miner for the task and submit the task to it.

    Args:
        job_request (str): The job request as a JSON string.
    """
    try:
        organic_job_request: OrganicJobRequest = pydantic.TypeAdapter(
            OrganicJobRequest
        ).validate_json(job_request)
    except pydantic.ValidationError:
        raise InvalidJobRequestFormat(f"Invalid job request format: {job_request}")

    logger.debug(f"Received signed job request: {organic_job_request}")
    await verify_request_or_fail(organic_job_request)

    # Notify facilitator that the job request has been received
    try:
        await safe_send_local_message(
            channel=JOB_STATUS_UPDATE_CHANNEL,
            message=JobStatusUpdate(uuid=organic_job_request.uuid, status=JobStatus.RECEIVED),
        )
    except LocalChannelSendError as exc:
        # Not sending a job update shouldn't abort the job itself
        logger.error(str(exc))

    # Select an appropriate miner for the task and submit the task to it
    job_route = await routing().pick_miner_for_job_request(organic_job_request)
    logger.info(f"Selected miner {job_route.miner.hotkey_ss58} for job {organic_job_request.uuid}")
    job = await execute_organic_job_request_on_worker(organic_job_request, job_route)
    logger.info(
        f"Job {organic_job_request.uuid} finished with status: {job.status} (comment={job.comment})"
    )

    if job.status == OrganicJob.Status.FAILED:
        await report_miner_failed_job(job)
