from abc import ABC, abstractmethod

from .types import MinerCollateral, SlashedEvent


class CollateralBase(ABC):
    """
    Abstract interface for collateral management.
    """

    @abstractmethod
    def list_miners_with_sufficient_collateral(self, min_amount_wei: int) -> list[MinerCollateral]:
        """
        Return miners whose collateral is at least min_amount_wei.
        """
        pass

    @abstractmethod
    def slash_collateral(
        self,
        *,
        miner_hotkey: str | None = None,
        evm_address: str | None = None,
        amount_wei: int | None = None,
        url: str = "",
    ) -> SlashedEvent:
        """
        Slash collateral on-chain for the given miner.
        Either miner_hotkey or evm_address must be provided.
        """
        pass


