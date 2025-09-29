from __future__ import annotations

from dataclasses import dataclass
from typing import Any, cast

from web3 import Web3

from compute_horde_validator.validator.allowance import MetagraphSnapshotData
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
            fetch_block_hash=lambda block_number: self.block_hash,
            fetch_evm_key_associations=self._fetch_associations,
            web3=lambda _network: cast(Web3, self.env.web3),
            collateral=lambda: self.env.collateral,
            system_events=lambda: self.env.system_events,
        )
        collateral_tasks.sync_collaterals.run(deps=deps)

    def _fetch_metagraph(self) -> MetagraphSnapshotData:
        return MetagraphSnapshotData(
            neurons=list(self.neurons),
            subnet_state={},
            block_number=self.block_number,
        )

    def _fetch_associations(
        self, _subtensor: Any, netuid: int, block_hash: str | None
    ) -> dict[int, str]:
        self.env.fetch_log.append({"netuid": netuid, "block_hash": block_hash})
        return self.associations
