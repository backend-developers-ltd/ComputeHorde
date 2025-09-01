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
        w3,
        contract_address: str,
        miner_address: str,
        amount_wei: int,
        url: str,
    ) -> SlashedEvent:
        """
        Slash collateral on-chain for the given miner.
        """
        pass

    @abstractmethod
    def get_collateral_contract_address(self) -> str | None:
        """
        Return the current collateral contract address or None if unavailable.
        """
        pass

    @abstractmethod
    def get_miner_collateral(
        self,
        w3,
        contract_address: str,
        miner_address: str,
        block_identifier: int | None = None,
    ) -> int:
        """
        Return miner collateral in Wei from chain.
        """
        pass

    @abstractmethod
    async def get_evm_key_associations(
        self, subtensor, netuid: int, block_hash: str | None = None
    ) -> dict[int, str]:
        """
        Return uid->evm_address associations from subtensor.
        """
        pass
