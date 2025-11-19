import logging

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
from compute_horde_validator.validator.tasks import (
    execute_organic_job,
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


async def verify_request_or_fail(job_request: SignedRequest) -> None:
    """
    Check that the signer is in validator whitelist and that the signature is
    valid.

    Args:
        job_request (SignedRequest): The signed job request.

    Raises:
        JobRequestVerificationFailed: If the job request is not valid.
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

    Args:
        cheated_job_request (V0JobCheated): The cheated job request.
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


async def verify_and_submit_organic_job_request(job_request: OrganicJobRequest) -> None:
    """
    Verify and submit an organic job request as a Celery task.

    Args:
        job_request (OrganicJobRequest): The organic job request.

    Raises:
        JobRequestVerificationFailed: If the job request cannot be verified.
    """
    await verify_request_or_fail(job_request)
    logger.debug(f"Received signed job request: {job_request}")

    # Notify facilitator that the job request has been received
    try:
        await safe_send_local_message(
            channel=JOB_STATUS_UPDATE_CHANNEL,
            message=JobStatusUpdate(uuid=job_request.uuid, status=JobStatus.RECEIVED),
        )
    except LocalChannelSendError as exc:
        # Not sending this job update shouldn't abort the job itself
        logger.error(str(exc))

    await execute_organic_job(job_request=job_request)
