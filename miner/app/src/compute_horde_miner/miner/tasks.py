import datetime

from celery.utils.log import get_task_logger
from compute_horde.dynamic_config import sync_dynamic_config
from compute_horde.receipts.models import JobAcceptedReceipt, JobFinishedReceipt, JobStartedReceipt
from compute_horde.receipts.schemas import (
    JobAcceptedReceiptPayload,
    JobFinishedReceiptPayload,
    JobStartedReceiptPayload,
)
from compute_horde.receipts.transfer import get_miner_receipts
from compute_horde.utils import get_validators
from constance import config
from django.conf import settings
from django.utils.timezone import now

from compute_horde_miner.celery import app
from compute_horde_miner.miner import quasi_axon
from compute_horde_miner.miner.models import Validator
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


@app.task
def prepare_receipts():
    receipts = []

    for model in [JobStartedReceipt, JobAcceptedReceipt, JobFinishedReceipt]:
        db_objects = model.objects.order_by("timestamp").filter(  # type: ignore[attr-defined]
            timestamp__gt=now() - RECEIPTS_MAX_SERVED_PERIOD
        )
        for db_object in db_objects:
            try:
                receipts.append(db_object.to_receipt())
            except Exception as e:
                logger.error(f"Skipping job started receipt for job {db_object.job_uuid}: {e}")

    logger.info(f"Stored receipts: {len(receipts)}")

    receipts_store.store(receipts)


@app.task
def clear_old_receipts():
    for model in [JobStartedReceipt, JobAcceptedReceipt, JobFinishedReceipt]:
        model.objects.filter(timestamp__lt=now() - RECEIPTS_MAX_RETENTION_PERIOD).delete()  # type: ignore[attr-defined]


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
        JobStartedReceipt.objects.filter(miner_hotkey=hotkey).order_by("-timestamp").first()
    )
    job_started_receipt_cutoff_time = (
        latest_job_started_receipt.timestamp - tolerance if latest_job_started_receipt else None
    )
    job_started_receipt_to_create = [
        JobStartedReceipt(
            job_uuid=receipt.payload.job_uuid,
            miner_hotkey=receipt.payload.miner_hotkey,
            validator_hotkey=receipt.payload.validator_hotkey,
            validator_signature=receipt.validator_signature,
            miner_signature=receipt.miner_signature,
            timestamp=receipt.payload.timestamp,
            executor_class=receipt.payload.executor_class,
            max_timeout=receipt.payload.max_timeout,
            is_organic=receipt.payload.is_organic,
            ttl=receipt.payload.ttl,
        )
        for receipt in receipts
        if isinstance(receipt.payload, JobStartedReceiptPayload)
        and (
            job_started_receipt_cutoff_time is None
            or receipt.payload.timestamp > job_started_receipt_cutoff_time
        )
    ]
    if job_started_receipt_to_create:
        JobStartedReceipt.objects.bulk_create(job_started_receipt_to_create, ignore_conflicts=True)

    latest_job_accepted_receipt = (
        JobAcceptedReceipt.objects.filter(miner_hotkey=hotkey).order_by("-timestamp").first()
    )
    job_accepted_receipt_cutoff_time = (
        latest_job_accepted_receipt.timestamp - tolerance if latest_job_accepted_receipt else None
    )
    job_accepted_receipt_to_create = [
        JobAcceptedReceipt(
            job_uuid=receipt.payload.job_uuid,
            miner_hotkey=receipt.payload.miner_hotkey,
            validator_hotkey=receipt.payload.validator_hotkey,
            validator_signature=receipt.validator_signature,
            miner_signature=receipt.miner_signature,
            timestamp=receipt.payload.timestamp,
            time_accepted=receipt.payload.time_accepted,
            ttl=receipt.payload.ttl,
        )
        for receipt in receipts
        if isinstance(receipt.payload, JobAcceptedReceiptPayload)
        and (
            job_accepted_receipt_cutoff_time is None
            or receipt.payload.timestamp > job_accepted_receipt_cutoff_time
        )
    ]
    if job_accepted_receipt_to_create:
        JobAcceptedReceipt.objects.bulk_create(
            job_accepted_receipt_to_create, ignore_conflicts=True
        )

    latest_job_finished_receipt = (
        JobFinishedReceipt.objects.filter(miner_hotkey=hotkey).order_by("-timestamp").first()
    )
    job_finished_receipt_cutoff_time = (
        latest_job_finished_receipt.timestamp - tolerance if latest_job_finished_receipt else None
    )
    job_finished_receipt_to_create = [
        JobFinishedReceipt(
            job_uuid=receipt.payload.job_uuid,
            miner_hotkey=receipt.payload.miner_hotkey,
            validator_hotkey=receipt.payload.validator_hotkey,
            validator_signature=receipt.validator_signature,
            miner_signature=receipt.miner_signature,
            timestamp=receipt.payload.timestamp,
            time_started=receipt.payload.time_started,
            time_took_us=receipt.payload.time_took_us,
            score_str=receipt.payload.score_str,
        )
        for receipt in receipts
        if isinstance(receipt.payload, JobFinishedReceiptPayload)
        and (
            job_finished_receipt_cutoff_time is None
            or receipt.payload.timestamp > job_finished_receipt_cutoff_time
        )
    ]
    if job_finished_receipt_to_create:
        JobFinishedReceipt.objects.bulk_create(
            job_finished_receipt_to_create, ignore_conflicts=True
        )


@app.task
def fetch_dynamic_config() -> None:
    # if same key exists in both places, common config wins
    sync_dynamic_config(
        config_url=f"https://raw.githubusercontent.com/backend-developers-ltd/compute-horde-dynamic-config/master/miner-config-{settings.DYNAMIC_CONFIG_ENV}.json",
        namespace=config,
    )
    sync_dynamic_config(
        config_url=f"https://raw.githubusercontent.com/backend-developers-ltd/compute-horde-dynamic-config/master/common-config-{settings.DYNAMIC_CONFIG_ENV}.json",
        namespace=config,
    )
