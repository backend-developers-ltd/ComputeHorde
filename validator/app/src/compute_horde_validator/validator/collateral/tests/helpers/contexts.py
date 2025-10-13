from __future__ import annotations

from dataclasses import dataclass
from typing import Any, cast
from unittest.mock import Mock

from web3 import Web3

from compute_horde_validator.validator.collateral import tasks as collateral_tasks
from compute_horde_validator.validator.collateral.tasks import CollateralTaskDependencies

from .env import CollateralTestEnvironment

__all__ = [
    "CollateralTestEnvironment",
    "CollateralTaskHarness",
]


@dataclass
class CollateralTaskHarness:
    env: CollateralTestEnvironment
    neurons: list[Any]
    block_number: int
    block_hash: str
    associations: dict[int, str]

    def run(self) -> None:
        deps = CollateralTaskDependencies(
            fetch_metagraph=self._fetch_metagraph,
            fetch_evm_key_associations=self._fetch_associations,
            web3=lambda _network: cast(Web3, self.env.web3),
            collateral=lambda: self.env.collateral,
            system_events=lambda: self.env.system_events,
        )
        collateral_tasks.sync_collaterals.run(deps=deps)

    def _fetch_metagraph(self, _bittensor: Any) -> tuple[list[Any], Any, Any]:
        block = Mock(number=self.block_number, hash=self.block_hash)
        subnet_state = Mock()
        return self.neurons, subnet_state, block

    def _fetch_associations(
        self, _subtensor: Any, netuid: int, block_hash: str | None
    ) -> dict[int, str]:
        self.env.fetch_log.append({"netuid": netuid, "block_hash": block_hash})
        return self.associations
