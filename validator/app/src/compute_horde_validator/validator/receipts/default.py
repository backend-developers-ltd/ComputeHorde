import datetime
import logging

from compute_horde.receipts import Receipt
from compute_horde.receipts.models import JobFinishedReceipt, receipt_to_django_model
from compute_horde.receipts.schemas import JobFinishedReceiptPayload

from compute_horde_validator.validator.receipts.exceptions import (
    ReceiptsGenerationError,
)
from compute_horde_validator.validator.receipts.interface import ReceiptsBase

logger = logging.getLogger(__name__)


class Receipts(ReceiptsBase):
    """
    Default implementation of receipts manager.
    """

    def get_completed_job_receipts_for_block_range(
        self, start_block: int, end_block: int
    ) -> list[Receipt]:
        """
        Get all receipts for jobs that were completed between the specified blocks.

        Args:
            start_block: Start block (inclusive)
            end_block: End block (exclusive)

        Returns:
            List of receipts for completed jobs in the block range
        """
        # TODO: Implement block-based filtering
        # For now, return all job finished receipts
        finished_receipts = list(JobFinishedReceipt.objects.all())

        # Convert to Receipt objects
        receipts = []
        for receipt in finished_receipts:
            receipts.append(receipt.to_receipt())

        return receipts

    async def scrape_receipts_from_miners(
        self, miner_hotkeys: list[str], start_block: int, end_block: int
    ) -> list[Receipt]:
        """
        Scrape receipts from specified miners.

        Args:
            miner_hotkeys: List of miner hotkeys to scrape receipts from

        Returns:
            List of scraped receipts
        """
        # TODO: Implement actual scraping logic
        logger.info(f"Scraping receipts from {len(miner_hotkeys)} miners")
        return []

    def create_job_finished_receipt(
        self,
        job_uuid: str,
        miner_hotkey: str,
        validator_hotkey: str,
        time_started: int,
        time_took_us: int,
        score_str: str,
    ) -> JobFinishedReceipt:
        """
        Create a job finished receipt for a completed job.

        Args:
            job_uuid: UUID of the job
            miner_hotkey: Hotkey of the miner
            validator_hotkey: Hotkey of the validator
            time_started: Timestamp when job started
            time_took_us: Time taken in microseconds
            score_str: Score as string

        Returns:
            Created job finished receipt
        """
        try:
            payload = JobFinishedReceiptPayload(
                job_uuid=job_uuid,
                miner_hotkey=miner_hotkey,
                validator_hotkey=validator_hotkey,
                timestamp=datetime.datetime.now(),
                time_started=datetime.datetime.fromtimestamp(time_started),
                time_took_us=time_took_us,
                score_str=score_str,
            )

            # TODO: Add proper signature generation
            validator_signature = "placeholder_signature"

            receipt = JobFinishedReceipt.from_payload(payload, validator_signature)

            return receipt
        except Exception as e:
            raise ReceiptsGenerationError(f"Failed to create job finished receipt: {e}") from e

    def save_receipt(self, receipt: Receipt) -> None:
        """
        Save a receipt to the database.

        Args:
            receipt: Receipt to save
        """
        try:
            django_model = receipt_to_django_model(receipt)
            django_model.save()
            logger.info(f"Saved receipt for job {receipt.payload.job_uuid}")
        except Exception as e:
            raise ReceiptsGenerationError(f"Failed to save receipt: {e}") from e
