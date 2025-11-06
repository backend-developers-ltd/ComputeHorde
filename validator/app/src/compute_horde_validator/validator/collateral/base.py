from abc import ABC, abstractmethod

from .types import MinerCollateral


class CollateralBase(ABC):
    """
    Abstract interface for collateral management.
    """

    @abstractmethod
    def list_miners_with_sufficient_collateral(self, min_amount_wei: int) -> list[MinerCollateral]:
        """
        Return miners whose collateral is at least min_amount_wei.
        Args:
            min_amount_wei: Minimum amount of Wei required for a miner.
        Returns:
            List of miners with sufficient collateral.
        """
        pass

    @abstractmethod
    def slash_collateral(
        self,
        miner_hotkey: str,
        url: str,
    ) -> None:
        """Slash collateral from a miner.
        Args:
            miner_hotkey: SS58 address of the miner to slash.
            url: URL containing information about the slash.
        Returns:
            None
        """
        pass
