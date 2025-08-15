import datetime
from abc import ABC, abstractmethod

from compute_horde.receipts import Receipt
from compute_horde.receipts.models import JobFinishedReceipt, JobStartedReceipt
from compute_horde.receipts.schemas import JobStartedReceiptPayload


class ReceiptsBase(ABC):
    """
    Abstract interface for receipts management.

    This class defines the interface for managing receipts in the validator.
    """

    @abstractmethod
    async def scrape_receipts_from_miners(
        self, miner_hotkeys: list[str], start_block: int, end_block: int
    ) -> list[Receipt]:
        """
        Scrape receipts from miners for a block range.

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
        time_started: datetime.datetime,
        time_took_us: int,
        score_str: str,
    ) -> JobFinishedReceipt:
        """
        Create a job finished receipt for a completed job.

        Args:
            job_uuid: UUID of the job
            miner_hotkey: Hotkey of the miner
            validator_hotkey: Hotkey of the validator
            time_started: Time the job started
            time_took_us: Time the job took in microseconds
            score_str: Score of the job

        Returns:
            JobFinishedReceipt
        """
        pass

    @abstractmethod
    def create_job_started_receipt(
        self,
        job_uuid: str,
        miner_hotkey: str,
        validator_hotkey: str,
        executor_class: str,
        is_organic: bool,
        ttl: int,
    ) -> tuple[JobStartedReceiptPayload, str]:
        """
        Create a job started receipt payload.

        Args:
            job_uuid: UUID of the job
            miner_hotkey: Hotkey of the miner
            validator_hotkey: Hotkey of the validator
            executor_class: Executor class for the job
            is_organic: Whether this is an organic job
            ttl: Time to live in seconds

        Returns:
            Tuple of (payload, signature_hex)
        """
        pass

    @abstractmethod
    def get_valid_job_started_receipts_for_miner(
        self, miner_hotkey: str, at_time: datetime.datetime
    ) -> list[JobStartedReceipt]:
        """
        Get valid job started receipts for a miner at a specific time.

        Args:
            miner_hotkey: Miner's hotkey
            at_time: Time to check validity against

        Returns:
            List of valid receipts
        """
        pass

    @abstractmethod
    def get_job_finished_receipts_for_miner(
        self, miner_hotkey: str, job_uuids: list[str]
    ) -> list[JobFinishedReceipt]:
        """
        Get job finished receipts for specific jobs from a miner.

        Args:
            miner_hotkey: Miner's hotkey
            job_uuids: List of job UUIDs to get receipts for

        Returns:
            List of receipts
        """
        pass

    @abstractmethod
    def get_job_started_receipt_by_uuid(self, job_uuid: str) -> JobStartedReceipt | None:
        """
        Get a job started receipt by UUID.

        Args:
            job_uuid: UUID of the job

        Returns:
            Receipt if found, None otherwise
        """
        pass

    @abstractmethod
    async def get_completed_job_receipts_for_block_range(
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
