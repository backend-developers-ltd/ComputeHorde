import datetime
from abc import ABC, abstractmethod

from compute_horde.receipts.models import JobFinishedReceipt
from compute_horde.receipts.schemas import JobStartedReceiptPayload
from compute_horde_core.executor_class import ExecutorClass

from .types import JobSpendingInfo


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
        executor_class: ExecutorClass,
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

    @abstractmethod
    def get_finished_jobs_for_block_range(
        self,
        start_block: int,
        end_block: int,
        executor_class: ExecutorClass,
        organic_only: bool = False,
    ) -> list[JobSpendingInfo]:
        """
        Returns tuples for jobs whose finish (receipt) timestamp falls within the
        given block range [start_block, end_block), filtered by executor class and
        optionally whether the job was organic or not.
        """
        pass

    @abstractmethod
    def get_busy_executor_count(
        self, executor_class: ExecutorClass, at_time: datetime.datetime
    ) -> dict[str, int]:
        """
        Return counts of ongoing jobs per miner at the given time for the executor_class.

        A job counts as ongoing if its JobStartedReceipt is valid at at_time and there
        is no JobFinishedReceipt with timestamp <= at_time for the same job_uuid.
        """
        pass
