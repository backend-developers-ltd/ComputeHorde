import datetime

import bittensor
from asgiref.sync import async_to_sync
from celery.utils.log import get_task_logger
from compute_horde.dynamic_config import fetch_dynamic_configs_from_contract, sync_dynamic_config
from compute_horde.receipts.store.local import LocalFilesystemPagedReceiptStore
from compute_horde.smart_contracts.map_contract import get_dynamic_config_types_from_settings
from compute_horde.utils import get_validators
from constance import config
from django.conf import settings

from compute_horde_miner.celery import app
from compute_horde_miner.miner import eviction, quasi_axon
from compute_horde_miner.miner.executor_manager import current
from compute_horde_miner.miner.manifest_commitment import (
    commit_manifest_to_subtensor,
    has_manifest_changed,
)
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
    if settings.USE_CONTRACT_CONFIG:
        dynamic_configs = get_dynamic_config_types_from_settings()
        fetch_dynamic_configs_from_contract(
            dynamic_configs,
            settings.CONFIG_CONTRACT_ADDRESS,
            namespace=config,
        )
        return

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


@app.task
def commit_manifest_to_chain():
    """
    Periodically checks and commits manifest if changed.

    Steps:
    1. Get current manifest from executor_manager
    2. Get on-chain commitment via subtensor.get_commitment()
    3. Compare: has_manifest_changed()
    4. If changed:
       a. Commit to subtensor
       b. Log success/failure

    Error handling:
    - Rate limit: Skip silently, will retry next cycle
    - Connection errors: Log warning, retry next cycle
    - Format errors: Log critical error
    """
    if not getattr(config, "MANIFEST_COMMITMENT_ENABLED", True):
        logger.debug("Manifest commitment is disabled")
        return

    try:
        # Get current manifest
        manifest = async_to_sync(current.executor_manager.get_manifest)()

        if not manifest:
            logger.debug("Empty manifest, skipping commitment")
            return

        # Get on-chain commitment
        wallet = settings.BITTENSOR_WALLET()
        subtensor = bittensor.subtensor(network=settings.BITTENSOR_NETWORK)

        try:
            # Get commitment for this miner's hotkey
            chain_commitment = subtensor.get_commitment(
                netuid=settings.BITTENSOR_NETUID,
                hotkey=wallet.hotkey.ss58_address,
            )
        except Exception as e:
            logger.warning(f"Failed to get on-chain commitment: {e}")
            chain_commitment = None

        # Check if manifest has changed
        if has_manifest_changed(manifest, chain_commitment):
            logger.info("Manifest has changed, committing to chain")
            success = commit_manifest_to_subtensor(
                manifest, wallet, subtensor, settings.BITTENSOR_NETUID
            )
            if success:
                logger.info("Successfully committed manifest to chain")
            else:
                logger.warning("Failed to commit manifest to chain")
        else:
            logger.debug("Manifest unchanged, skipping commitment")

    except Exception as e:
        logger.error(f"Error in commit_manifest_to_chain: {e}", exc_info=True)
