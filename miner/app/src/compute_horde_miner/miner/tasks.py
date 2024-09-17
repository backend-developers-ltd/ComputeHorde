import datetime

from celery.utils.log import get_task_logger
from compute_horde.dynamic_config import sync_dynamic_config
from compute_horde.receipts import (
    JobFinishedReceiptPayload,
    JobStartedReceiptPayload,
    get_miner_receipts,
)
from compute_horde.utils import get_validators
from constance import config
from django.conf import settings
from django.utils.timezone import now

from compute_horde_miner.celery import app
from compute_horde_miner.miner import quasi_axon
from compute_horde_miner.miner.models import JobFinishedReceipt, JobStartedReceipt, Validator
from compute_horde_miner.miner.receipt_store.current import receipts_store

logger = get_task_logger(__name__)

RECEIPTS_MAX_RETENTION_PERIOD = datetime.timedelta(days=2)
RECEIPTS_MAX_SERVED_PERIOD = datetime.timedelta(days=1)


@app.task
def announce_address_and_port():
    if not config.SERVING:
        logger.warning("Not announcing address and port, SERVING is disabled in constance config")
        return

    quasi_axon.announce_address_and_port()


@app.task
def fetch_validators():
    debug_validator_keys = set(
        Validator.objects.filter(debug=True, active=True).values_list("public_key", flat=True)
    )

    validators = get_validators(
        netuid=settings.BITTENSOR_NETUID, network=settings.BITTENSOR_NETWORK
    )
    validator_keys = {v.hotkey for v in validators} | debug_validator_keys

    to_activate = []
    to_deactivate = []
    to_create = []
    for validator in Validator.objects.all():
        if validator.public_key in validator_keys:
            to_activate.append(validator)
            validator.active = True
            validator_keys.remove(validator.public_key)
        else:
            validator.active = False
            to_deactivate.append(validator)
    for key in validator_keys:
        to_create.append(Validator(public_key=key, active=True))

    Validator.objects.bulk_create(to_create)
    Validator.objects.bulk_update(to_activate + to_deactivate, ["active"])
    logger.info(
        f"Fetched validators. Activated: {len(to_activate)}, deactivated: {len(to_deactivate)}, "
        f"created: {len(to_create)}"
    )


async def prepare_receipts():
    receipts = []
    job_started_receipts = JobStartedReceipt.objects.order_by("time_accepted").filter(
        time_accepted__gt=now() - RECEIPTS_MAX_SERVED_PERIOD
    )
    receipts += [jr.to_receipt() async for jr in job_started_receipts]

    job_finished_receipts = JobFinishedReceipt.objects.order_by("time_started").filter(
        time_started__gt=now() - RECEIPTS_MAX_SERVED_PERIOD
    )
    receipts += [jr.to_receipt() async for jr in job_finished_receipts]

    receipts_store.store(receipts)


@app.task
def clear_old_receipts():
    JobFinishedReceipt.objects.filter(
        time_started__lt=now() - RECEIPTS_MAX_RETENTION_PERIOD
    ).delete()
    JobStartedReceipt.objects.filter(
        time_accepted__lt=now() - RECEIPTS_MAX_RETENTION_PERIOD
    ).delete()


@app.task
def get_receipts_from_old_miner():
    if not config.MIGRATING:
        return

    if not config.OLD_MINER_IP:
        logger.warning("OLD_MINER_IP is not configured!")
        return

    logger.info("Fetching data from old miner server")

    hotkey = settings.BITTENSOR_WALLET().hotkey.ss58_address
    receipts = get_miner_receipts(hotkey, config.OLD_MINER_IP, config.OLD_MINER_PORT)

    tolerance = datetime.timedelta(hours=1)

    latest_job_started_receipt = (
        JobStartedReceipt.objects.filter(miner_hotkey=hotkey).order_by("-time_accepted").first()
    )
    job_started_receipt_cutoff_time = (
        latest_job_started_receipt.time_accepted - tolerance if latest_job_started_receipt else None
    )
    job_started_receipt_to_create = [
        JobStartedReceipt(
            job_uuid=receipt.payload.job_uuid,
            miner_hotkey=receipt.payload.miner_hotkey,
            validator_hotkey=receipt.payload.validator_hotkey,
            executor_class=receipt.payload.executor_class,
            time_accepted=receipt.payload.time_accepted,
            max_timeout=receipt.payload.max_timeout,
        )
        for receipt in receipts
        if isinstance(receipt.payload, JobStartedReceiptPayload)
        and (
            job_started_receipt_cutoff_time is None
            or receipt.payload.time_accepted > job_started_receipt_cutoff_time
        )
    ]
    if job_started_receipt_to_create:
        JobStartedReceipt.objects.bulk_create(job_started_receipt_to_create, ignore_conflicts=True)

    latest_job_finished_receipt = (
        JobFinishedReceipt.objects.filter(miner_hotkey=hotkey).order_by("-time_started").first()
    )
    job_finished_receipt_cutoff_time = (
        latest_job_finished_receipt.time_started - tolerance
        if latest_job_finished_receipt
        else None
    )
    job_finished_receipt_to_create = [
        JobFinishedReceipt(
            job_uuid=receipt.payload.job_uuid,
            miner_hotkey=receipt.payload.miner_hotkey,
            validator_hotkey=receipt.payload.validator_hotkey,
            time_started=receipt.payload.time_started,
            time_took_us=receipt.payload.time_took_us,
            score_str=receipt.payload.score_str,
        )
        for receipt in receipts
        if isinstance(receipt.payload, JobFinishedReceiptPayload)
        and (
            job_finished_receipt_cutoff_time is None
            or receipt.payload.time_started > job_finished_receipt_cutoff_time
        )
    ]
    if job_finished_receipt_to_create:
        JobFinishedReceipt.objects.bulk_create(
            job_finished_receipt_to_create, ignore_conflicts=True
        )


@app.task
def fetch_dynamic_config() -> None:
    sync_dynamic_config(
        config_url=f"https://raw.githubusercontent.com/backend-developers-ltd/compute-horde-dynamic-config/master/miner-config-{settings.DYNAMIC_CONFIG_ENV}.json",
        namespace=config,
    )
