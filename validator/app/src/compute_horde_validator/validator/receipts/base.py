import datetime
from abc import ABC, abstractmethod

from compute_horde.receipts import Receipt
from compute_horde.receipts.models import JobFinishedReceipt, JobStartedReceipt
from compute_horde.receipts.schemas import JobStartedReceiptPayload
from typing_extensions import deprecated


class ReceiptsBase(ABC):
    """
    Abstract interface for receipts management.

    This class defines the interface for managing receipts in the validator.
    """

    @abstractmethod
    async def run_receipts_transfer(
        self,
        daemon: bool,
        debug_miner_hotkey: str | None,
        debug_miner_ip: str | None,
        debug_miner_port: int | None,
    ) -> None:
        """
        Run the receipts transfer loop (or a single iteration).

        Args:
            daemon: If True, run indefinitely; otherwise perform a single transfer
            debug_miner_hotkey: Explicit miner hotkey to fetch from (debug only)
            debug_miner_ip: Explicit miner IP to fetch from (debug only)
            debug_miner_port: Explicit miner port to fetch from (debug only)
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
        Create a job started receipt.

        Args:
            job_uuid: UUID of the job
            miner_hotkey: Hotkey of the miner
            validator_hotkey: Hotkey of the validator
            executor_class: Executor class for the job
            is_organic: Whether the job is organic
            ttl: Time to live for the receipt

        Returns:
            Tuple of (payload, signature)
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
        Create a job finished receipt.

        Args:
            job_uuid: UUID of the job
            miner_hotkey: Hotkey of the miner
            validator_hotkey: Hotkey of the validator
            time_started: When the job started
            time_took_us: How long the job took in microseconds
            score_str: Score string for the job

        Returns:
            Created JobFinishedReceipt
        """
        pass

    @deprecated("Use database queries instead")
    @abstractmethod
    async def get_valid_job_started_receipts_for_miner(
        self, miner_hotkey: str, at_time: datetime.datetime
    ) -> list[JobStartedReceipt]:
        """
        Get valid job started receipts for a miner at a specific time.

        Args:
            miner_hotkey: Hotkey of the miner
            at_time: Time to check validity at

        Returns:
            List of valid JobStartedReceipt objects
        """
        pass

    @deprecated("Use database queries instead")
    @abstractmethod
    async def get_job_finished_receipts_for_miner(
        self, miner_hotkey: str, job_uuids: list[str]
    ) -> list[JobFinishedReceipt]:
        """
        Get job finished receipts for a miner and specific job UUIDs.

        Args:
            miner_hotkey: Hotkey of the miner
            job_uuids: List of job UUIDs to get receipts for

        Returns:
            List of JobFinishedReceipt objects
        """
        pass

    @deprecated("Use database queries instead")
    @abstractmethod
    async def get_job_started_receipt_by_uuid(self, job_uuid: str) -> JobStartedReceipt | None:
        """
        Get a job started receipt by UUID.

        Args:
            job_uuid: UUID of the job

        Returns:
            JobStartedReceipt if found, None otherwise
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
