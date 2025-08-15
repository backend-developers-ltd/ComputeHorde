import logging

from asgiref.sync import async_to_sync
from compute_horde.receipts.models import JobFinishedReceipt
from compute_horde.subtensor import get_cycle_containing_block
from django.conf import settings

from compute_horde_validator.celery import app
from compute_horde_validator.validator.models import MetagraphSnapshot
from compute_horde_validator.validator.models.allowance.internal import Block
from compute_horde_validator.validator.receipts.default import Receipts

logger = logging.getLogger(__name__)


@app.task(name="compute_horde_validator.validator.receipts.scrape_receipts_from_miners")
def scrape_receipts_from_miners() -> None:
    """
    Periodic receipts scraping task.

    - Determines serving miners from the latest metagraph snapshot
    - Scrapes receipts for the current cycle up to the latest snapshot block

    Returns the number of receipts retrieved in the call.
    """
    try:
        metagraph = MetagraphSnapshot.get_latest()
    except Exception:
        logger.warning("No metagraph snapshot available for receipts scraping")
        return

    miner_hotkeys = metagraph.get_serving_hotkeys()
    if not miner_hotkeys:
        logger.info("No serving miners found for receipts scraping")
        return

    current_block = metagraph.block
    current_cycle = get_cycle_containing_block(
        block=current_block, netuid=settings.BITTENSOR_NETUID
    )

    latest_receipt = JobFinishedReceipt.objects.order_by("-timestamp").first()

    if latest_receipt:
        try:
            cycle_start_block = Block.objects.get(block_number=current_cycle.start)
            current_cycle_start_timestamp = cycle_start_block.creation_timestamp

            # If the latest receipt is newer than the current cycle start, we've already scraped this cycle
            if latest_receipt.timestamp >= current_cycle_start_timestamp:
                logger.debug("Already scraped receipts for cycle %s, skipping", current_cycle.start)
                return
        except Block.DoesNotExist:
            # If the cycle start block doesn't exist in our database, proceed with scraping
            logger.debug(
                "Cycle start block %s not found in database, proceeding with scraping",
                current_cycle.start,
            )

    logger.info(
        "New cycle detected or first run, scraping receipts for cycle %s-%s",
        current_cycle.start,
        current_cycle.stop,
    )

    try:
        scraped = async_to_sync(Receipts().scrape_receipts_from_miners)(
            miner_hotkeys=miner_hotkeys,
            start_block=current_cycle.start,
            end_block=current_block,
        )
        logger.info(
            "Scraped %d receipts for cycle %s-%s",
            len(scraped),
            current_cycle.start,
            current_cycle.stop,
        )

    except Exception as e:
        logger.error("Failed to scrape receipts: %s", e, exc_info=True)
        return
