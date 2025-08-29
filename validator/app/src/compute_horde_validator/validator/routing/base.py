"""Abstract base class for allowance management."""

from abc import ABC, abstractmethod

from compute_horde.fv_protocol.facilitator_requests import OrganicJobRequest

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
        Creates a reservation for the selected miner and returns a JobRoute with that miner.
        """
