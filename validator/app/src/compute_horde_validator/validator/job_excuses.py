import logging
from datetime import datetime, timedelta

from compute_horde.receipts import Receipt
from compute_horde.receipts.schemas import JobStartedReceiptPayload
from compute_horde.utils import BAC_VALIDATOR_SS58_ADDRESS, ValidatorInfo
from compute_horde_core.executor_class import ExecutorClass
from django.conf import settings

from compute_horde_validator.validator.models import MetagraphSnapshot, MinerManifest
from compute_horde_validator.validator.synthetic_jobs.utils import get_validator_infos

logger = logging.getLogger(__name__)


async def filter_valid_excuse_receipts(
    receipts_to_check: list[Receipt],
    check_time: datetime,
    declined_job_uuid: str,
    declined_job_executor_class: ExecutorClass,
    declined_job_is_synthetic: bool,
    miner_hotkey: str,
    minimum_validator_stake_for_excuse: float,
    active_validators: list[ValidatorInfo] | None = None,
) -> list[Receipt]:
    if not receipts_to_check:
        return []

    if active_validators is None:
        metagraph = await MetagraphSnapshot.aget_latest()
        active_validators = get_validator_infos(metagraph)

    allowed_validators = {
        validator_info.hotkey
        for validator_info in active_validators
        if (
            validator_info.stake >= minimum_validator_stake_for_excuse
            or validator_info.hotkey == BAC_VALIDATOR_SS58_ADDRESS
        )
    }
    # Note: valid jobs by BAC validator are always excused (for easier testing subnet of development)

    # Vali should probably trust itself in any case.
    allowed_validators.add(settings.BITTENSOR_WALLET().get_hotkey().ss58_address)

    # We need time leeway so that if the miner receives multiple jobs in a short time, a slight
    # time difference caused by clock desync and network latencies doesn't cause the miner to lose
    # score when they accept an organic job and get a synthetic job request a couple of seconds
    # "from the past"
    leeway = timedelta(seconds=2)

    # Reject duplicate receipts - use the receipts' validator signature as unique ID
    seen_receipts: set[str] = set()

    valid_receipts: list[Receipt] = []
    for receipt in receipts_to_check:
        if (
            isinstance(receipt.payload, JobStartedReceiptPayload)
            and (receipt.payload.is_organic if declined_job_is_synthetic else True)
            and receipt.payload.miner_hotkey == miner_hotkey
            and receipt.payload.job_uuid != declined_job_uuid
            and receipt.payload.validator_hotkey in allowed_validators
            and receipt.payload.job_uuid not in seen_receipts
            and receipt.payload.executor_class == declined_job_executor_class
            and receipt.payload.timestamp < check_time
            and check_time
            < receipt.payload.timestamp + timedelta(seconds=receipt.payload.ttl) + leeway
            and receipt.verify_validator_signature(throw=False)
        ):
            seen_receipts.add(receipt.payload.job_uuid)
            valid_receipts.append(receipt)

    return valid_receipts


async def get_expected_miner_executor_count(
    check_time: datetime,
    miner_hotkey: str,
    executor_class: ExecutorClass,
) -> int:
    latest_manifest = (
        await MinerManifest.objects.filter(
            miner__hotkey=miner_hotkey,
            executor_class=executor_class,
            created_at__lte=check_time,
        )
        .order_by("created_at")
        .only("online_executor_count")
        .afirst()
    )

    if latest_manifest is None:
        logger.warning(
            f"Cannot check expected miner executor count: "
            f"manifest not found "
            f"({miner_hotkey} {executor_class} {check_time})"
        )
        return 0

    return latest_manifest.online_executor_count
