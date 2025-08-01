import asyncio
import time
from datetime import datetime, timezone


from . import utils
import utils.blocks
import utils.manifests

try:
    import turbobt
except ImportError:
    turbobt = None

from asgiref.sync import async_to_sync
from celery import shared_task
from celery.utils.log import get_task_logger
from compute_horde_validator.celery import app
from compute_horde.miner_client.organic import OrganicMinerClient
from django.conf import settings
from django.db import transaction
from django.utils.timezone import now

from compute_horde_validator.validator.locks import Lock, LockType, Locked
from compute_horde_validator.validator.models import Miner, SystemEvent
from compute_horde_validator.validator.tasks import get_keypair, get_manifests_from_miners, _get_metagraph_for_sync

from .models.internal import Block, BlockAllowance
from .settings import BLOCK_LOOKBACK


logger = get_task_logger(__name__)

@app.task(
    time_limit=utils.blocks.MAX_RUN_TIME + 30,
)
def scan_blocks_and_calculate_allowance():
    with transaction.atomic(using=settings.DEFAULT_DB_ALIAS):
        try:
            with Lock(LockType.ALLOWANCE_FETCHING, 5.0):
                utils.blocks.scan_blocks_and_calculate_allowance()
        except Locked:
            logger.debug("Another thread already fetching blocks")
        except utils.blocks.TimesUpError:
            scan_blocks_and_calculate_allowance.delay()


@app.task()
def sync_manifests():
    try:
        utils.manifests.sync_manifests()
    except Exception as e:
        msg = f"Failed to sync manifests: {e}"
        logger.error(msg, exc_info=True)
        SystemEvent.objects.create(
            event_type=SystemEvent.EventType.COMPUTE_TIME_ALLOWANCE,
            event_subtype=SystemEvent.EventSubType.FAILURE,
            data={
                "error": str(e),
            },
        )


# clean up old allowance blocks
# clean up old reservations - report system events if there are any undone ones
# fetch blocks and store stuff - including ddos shield
# fetch manifests


# I'm gonna need some system events for diagnostics of which blocks were scraped and how between various valis
