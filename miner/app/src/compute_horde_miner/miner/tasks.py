import datetime

from celery.utils.log import get_task_logger
from compute_horde.receipts import get_miner_receipts
from compute_horde.utils import get_validators
from constance import config
from django.conf import settings
from django.utils.timezone import now

from compute_horde_miner.celery import app
from compute_horde_miner.miner import quasi_axon
from compute_horde_miner.miner.models import JobReceipt, Validator
from compute_horde_miner.miner.receipt_store.current import receipts_store

logger = get_task_logger(__name__)

RECEIPTS_MAX_RETENTION_PERIOD = datetime.timedelta(days=7)


@app.task
def announce_address_and_port():
    if not config.SERVING:
        logger.warning("Not announcing address and port, SERVING is disabled in constance config")
        return

    quasi_axon.announce_address_and_port()


@app.task
def fetch_validators():
    validators = get_validators(
        netuid=settings.BITTENSOR_NETUID, network=settings.BITTENSOR_NETWORK
    )
    validator_keys = {validator.hotkey for validator in validators}
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
    receipts = [jr.to_receipt() for jr in JobReceipt.objects.all()]
    receipts_store.store(receipts)


@app.task
def clear_old_receipts():
    JobReceipt.objects.filter(time_started__lt=now() - RECEIPTS_MAX_RETENTION_PERIOD).delete()


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

    latest_job_receipt = (
        JobReceipt.objects.filter(miner_hotkey=hotkey).order_by("-time_started").first()
    )
    tolerance = datetime.timedelta(minutes=15)
    cutoff_time = latest_job_receipt.time_started - tolerance if latest_job_receipt else None

    to_create = [
        JobReceipt(
            validator_signature=receipt.validator_signature,
            miner_signature=receipt.miner_signature,
            job_uuid=receipt.payload.job_uuid,
            miner_hotkey=receipt.payload.miner_hotkey,
            validator_hotkey=receipt.payload.validator_hotkey,
            time_started=receipt.payload.time_started,
            time_took_us=receipt.payload.time_took_us,
            score_str=receipt.payload.score_str,
        )
        for receipt in receipts
        if cutoff_time is None or receipt.payload.time_started > cutoff_time
    ]

    if not to_create:
        logger.info("All receipts already exist in this db. Migration is complete.")
        return

    JobReceipt.objects.bulk_create(to_create, ignore_conflicts=True)
