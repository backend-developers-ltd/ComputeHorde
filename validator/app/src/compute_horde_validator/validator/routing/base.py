"""Abstract base class for allowance management."""

from abc import ABC, abstractmethod

from compute_horde.fv_protocol.facilitator_requests import OrganicJobRequest

from compute_horde_validator.validator.allowance.types import Miner


class JobRouteBase(ABC):
    """
    Abstract base class for a job route.
    """

    def __init__(self, *, miner: Miner):
        self.miner = miner

    @abstractmethod
    def spend_allowance(self) -> None:
        pass

    @abstractmethod
    def undo_allowance_reservation(self) -> None:
        pass


class RoutingBase(ABC):
    """
    Abstract base class for job routing.

    This class defines the interface for routing organic jobs to miners.
    """

    @abstractmethod
    async def pick_miner_for_job_request(self, request: OrganicJobRequest) -> JobRouteBase:
        """
        Filters miners based on compute time allowance and minimum collateral requirements.
        Creates a reservation for the selected miner and returns a JobRouteBase with that miner.
        """
