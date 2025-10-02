from __future__ import annotations

from dataclasses import dataclass
from typing import Any, cast
from unittest.mock import Mock, patch

from web3 import Web3

from compute_horde_validator.validator.allowance.types import MetagraphData
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
            fetch_evm_key_associations=self._fetch_associations,
            web3=lambda _network: cast(Web3, self.env.web3),
            collateral=lambda: self.env.collateral,
            system_events=lambda: self.env.system_events,
        )

        metagraph = MetagraphData(
            block=self.block_number,
            block_hash=self.block_hash,
            total_stake=[],
            uids=[],
            hotkeys=[n.hotkey for n in self.neurons],
            serving_hotkeys=[],
        )

        mock_supertensor = Mock()
        mock_supertensor.get_metagraph.return_value = metagraph
        mock_supertensor.list_neurons.return_value = self.neurons
        mock_supertensor.bittensor.subtensor = Mock()

        with patch(
            "compute_horde_validator.validator.collateral.tasks.supertensor",
            return_value=mock_supertensor,
        ):
            collateral_tasks.sync_collaterals(deps=deps)

    def _fetch_associations(
        self, _subtensor: Any, netuid: int, block_hash: str | None
    ) -> dict[int, str]:
        self.env.fetch_log.append({"netuid": netuid, "block_hash": block_hash})
        return self.associations
