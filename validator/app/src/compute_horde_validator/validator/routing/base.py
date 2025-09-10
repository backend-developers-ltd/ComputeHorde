"""Abstract base class for allowance management."""

from abc import ABC, abstractmethod

from compute_horde.fv_protocol.facilitator_requests import OrganicJobRequest
from compute_horde_core.executor_class import ExecutorClass

from compute_horde_validator.validator.routing.types import JobRoute, MinerIncidentType


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

    @abstractmethod
    async def report_miner_incident(
        self,
        type: MinerIncidentType,
        hotkey_ss58address: str,
        job_uuid: str,
        executor_class: ExecutorClass,
    ) -> None:
        """
        Records a miner incident such as job rejection or failure.
        """
