from abc import ABC, abstractmethod

from web3 import Web3

from .types import MinerCollateral, SlashedEvent


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
        w3: Web3,
        contract_address: str,
        miner_address: str,
        amount_wei: int,
        url: str,
    ) -> SlashedEvent:
        """Slash collateral from a miner.
        Args:
            w3: Web3 instance to use for blockchain interaction.
            contract_address: Address of the Collateral contract.
            miner_address: EVM address of the miner to slash.
            amount_wei: Amount of Wei to slash.
            url: URL containing information about the slash.
        Returns:
            Transaction receipt with slash event details
        """
        pass

    @abstractmethod
    async def get_collateral_contract_address(self) -> str | None:
        """
        Return the current collateral contract address or None if unavailable.
        """
        pass
