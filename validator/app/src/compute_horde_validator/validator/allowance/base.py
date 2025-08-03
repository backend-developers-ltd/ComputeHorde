"""Abstract base class for allowance management."""

from abc import ABC, abstractmethod

from compute_horde_core.executor_class import ExecutorClass
from compute_horde_validator.validator.allowance.types import ss58_address, reservation_id, block_ids, Miner, Neuron


class AllowanceBase(ABC):
    """
    Abstract base class for allowance management.

    This class defines the interface for managing miner allowances, including
    finding suitable miners, reserving and spending allowances, and validating
    foreign receipts.
    """

    @abstractmethod
    def find_miners_with_allowance(
        self,
        allowance_seconds: float,
        executor_class: ExecutorClass,
        job_start_block: int,
    ) -> list[tuple[ss58_address, float]]:
        """
        Find miners that have at least the required amount of allowance left.

        Returns miners sorted by:
        1. Earliest unspent/unreserved block
        2. Highest total allowance percentage left

        Args:
            allowance_seconds: The minimum allowance amount required
            executor_class: executor class
            job_start_block: used to determine which blocks can be used for the reservation, as per block expiry rules

        Returns:
            List of miners that meet the allowance requirements, sorted appropriately (see README).
            The returned list is always not empty. If there are no miners with enough allowance,
            NotEnoughAllowanceException is raised. The returned miners are present in the subnet's metagraph snapshot
            kept by this module.
        """
        pass

    @abstractmethod
    def reserve_allowance(
        self,
        miner: ss58_address,
        executor_class: ExecutorClass,
        allowance_seconds: float,
        job_start_block: int,
    ) -> tuple[reservation_id, block_ids]:
        """
        Reserve allowance for a specific miner. The reservation will auto expire after
        `allowance_seconds + settings.RESERVATION_MARGIN_SECONDS` seconds.

        This is used for temporary allowance reservation for pending jobs.

        Args:
            miner: hotkey of the miner
            executor_class: When the reservation expires
            allowance_seconds: Amount of allowance to reserve (in seconds)
            job_start_block: used to determine which blocks can be used for the reservation, as per block expiry rules

        Returns:
            Id of the reservation, which can be used to cancel the reservation, or make it a permanent spend.

        raises CannotReserveAllowanceException if there is not enough allowance from the miner.
        """
        pass

    @abstractmethod
    def undo_allowance_reservation(self, reservation_id_: reservation_id) -> None:
        """
        Undo a previously made allowance reservation.

        This releases the reserved allowance back to the available pool.

        raises ReservationNotFound if the reservation is not found.
        """
        pass

    @abstractmethod
    def spend_allowance(self, reservation_id_: reservation_id) -> None:
        """
        Spend allowance (make a reservation permanent).

         Args:
             reservation_id_: reservation_id

         raises ReservationNotFound if the reservation is not found.
        """
        pass

    @abstractmethod
    def validate_foreign_receipt(self):
        """
        Interface to be decided.
        Validate jobs from other validators in terms of allowance.

        This ensures that foreign receipts are valid and don't violate
        allowance constraints.
        """
        pass

    @abstractmethod
    def get_manifests(self) -> dict[ss58_address, dict[ExecutorClass, int]]:
        """
        Return the latest manifests for all miners.
        """
        pass

    @abstractmethod
    def miners(self) -> list[Miner]:
        """
        Return neurons that have their axon addresses set.
        """

    @abstractmethod
    def neurons(self, block: int) -> list[Neuron]:
        """
        Return all neurons in the subnet for a given block. raises NeuronSnapshotMissing if the snapshot is missing.
        """
