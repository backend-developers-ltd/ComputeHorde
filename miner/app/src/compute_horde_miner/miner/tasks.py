import datetime

from asgiref.sync import async_to_sync
from celery.utils.log import get_task_logger
from compute_horde.dynamic_config import sync_dynamic_config
from compute_horde.receipts.store.local import LocalFilesystemPagedReceiptStore
from compute_horde.utils import get_validators
from constance import config
from django.conf import settings

from compute_horde_miner.celery import app
from compute_horde_miner.miner import eviction, quasi_axon
from compute_horde_miner.miner.models import Validator
from compute_horde_miner.miner.receipts import current_store

logger = get_task_logger(__name__)

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
def evict_old_data():
    async_to_sync(eviction.evict_all)()


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


@app.task
def archive_receipt_pages():
    store = async_to_sync(current_store)()
    if not isinstance(store, LocalFilesystemPagedReceiptStore):
        logger.info(f"Skipping archival: incompatible store: {type(store)}")
        return

    store.archive_old_pages()
