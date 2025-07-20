"""Abstract base class for allowance management."""

from abc import ABC, abstractmethod

import pydantic

from compute_horde_core.executor_class import ExecutorClass

ss58_address = str
reservation_id = int


class AllowanceException(Exception):
    pass


class NeuronSnapshotMissing(AllowanceException):
    pass


class ReservationNotFound(AllowanceException):
    pass


class CannotReserveAllowanceException(AllowanceException):
    """Exception raised when there is not enough allowance from a particular miner."""


class NotEnoughAllowanceException(AllowanceException):
    """Exception raised when there is not enough allowance."""

    def __init__(
        self,
        highest_available_allowance: float,
        highest_available_allowance_ss58: ss58_address,
        highest_unspent_allowance: float,
        highest_unspent_allowance_ss58: ss58_address,
    ):
        """
        :param highest_available_allowance: highest number of executor-seconds available
        :param highest_available_allowance_ss58: hotkey of the miner with highest number of executor-seconds available
        :param highest_unspent_allowance: highest number of executor-seconds unspent (free or reserved)
        :param highest_unspent_allowance_ss58: hotkey of the miner with highest number of executor-seconds unspent
        """
        self.highest_available_allowance = highest_available_allowance
        self.highest_available_allowance_ss58 = highest_available_allowance_ss58
        self.highest_unspent_allowance = highest_unspent_allowance
        self.highest_unspent_allowance_ss58 = highest_unspent_allowance_ss58


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
        required_allowance: float,
        executor_class: ExecutorClass,
        job_start_block: int,
    ) -> list(tuple(ss58_address, float)):
        """
        Find miners that have at least the required amount of allowance left.

        Returns miners sorted by:
        1. Earliest unspent/unreserved block
        2. Highest total allowance percentage left

        Args:
            required_allowance: The minimum allowance amount required (in seconds)
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
        amount: float,
        job_start_block: int,
    ) -> reservation_id:
        """
        Reserve allowance for a specific miner. The reservation will auto expire after `amount` seconds.

        This is used for temporary allowance reservation for pending jobs.

        Args:
            miner: hotkey of the miner
            executor_class: When the reservation expires
            amount: Amount of allowance to reserve (in seconds)
            job_start_block: used to determine which blocks can be used for the reservation, as per block expiry rules

        Returns:
            Id of the reservation, which can be used to cancel the reservation, or make it a permanent spend.

        raises CannotReserveAllowanceException if there is not enough allowance from the miner.
        """
        pass

    @abstractmethod
    def undo_allowance_reservation(self, reservation_id: reservation_id) -> None:
        """
        Undo a previously made allowance reservation.

        This releases the reserved allowance back to the available pool.

        raises ReservationNotFound if the reservation is not found.
        """
        pass

    @abstractmethod
    def spend_allowance(self, reservation_id: reservation_id) -> None:
        """
        Spend allowance (make a reservation permanent).

         Args:
             reservation_id: reservation_id

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

    class Miner(pydantic.BaseModel):
        address: str
        hotkey_ss58: ss58_address

    @abstractmethod
    def miners(self) -> list[Miner]:
        """
        Return neurons that have their axon addresses set.
        """

    class Neuron(pydantic.BaseModel):
        hotkey_ss58: ss58_address
        coldkey: ss58_address

    @abstractmethod
    def neurons(self, block: int) -> list[Neuron]:
        """
        Return all neurons in the subnet for a given block. raises NeuronSnapshotMissing if the snapshot is missing.
        """
