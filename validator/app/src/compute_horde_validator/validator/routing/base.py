"""Abstract base class for allowance management."""

from abc import ABC, abstractmethod

from compute_horde.fv_protocol.facilitator_requests import OrganicJobRequest

from compute_horde_validator.validator.allowance.types import Miner
from compute_horde_validator.validator.routing.types import JobRoute


class RoutingBase(ABC):
    """
    Abstract base class for job routing.

    This class defines the interface for routing organic jobs to miners.
    """

    @abstractmethod
    async def pick_miner_for_job_request(self, request: OrganicJobRequest) -> JobRoute:
        """
        Filters miners based on compute time allowance and minimum collateral requirements.
        Creates a preliminary reservation for the selected miner and returns the miner with:
        - Highest percentage of remaining allowance
        - Highest collateral as a tiebreaker
        - Available executors (less ongoing jobs than online executor count)
        - Sufficient remaining time in the current cycle
        """


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
