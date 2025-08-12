from abc import ABC, abstractmethod

from compute_horde.receipts import Receipt
from compute_horde.receipts.models import JobFinishedReceipt


class ReceiptsBase(ABC):
    """
    Abstract interface for receipts management.

    This class defines the interface for managing receipts in the validator.
    """

    @abstractmethod
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
        pass

    @abstractmethod
    async def scrape_receipts_from_miners(
        self, miner_hotkeys: list[str], start_block: int, end_block: int
    ) -> list[Receipt]:
        """
        Scrape receipts from specified miners within a block range.

        Args:
            miner_hotkeys: List of miner hotkeys to scrape receipts from
            start_block: Start block (inclusive) - if None, scrapes all available receipts
            end_block: End block (exclusive) - if None, scrapes all available receipts

        Returns:
            List of scraped receipts
        """
        pass

    @abstractmethod
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
        pass

    @abstractmethod
    def save_receipt(self, receipt: Receipt) -> None:
        """
        Save a receipt to the database.

        Args:
            receipt: Receipt to save
        """
        pass
