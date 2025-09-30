from __future__ import annotations

from dataclasses import dataclass
from typing import Any, cast

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
            fetch_metagraph=self._fetch_metagraph,
            fetch_block_hash=lambda block_number: self.block_hash,
            fetch_evm_key_associations=self._fetch_associations,
            web3=lambda _network: cast(Web3, self.env.web3),
            collateral=lambda: self.env.collateral,
            system_events=lambda: self.env.system_events,
        )
        collateral_tasks.sync_collaterals.run(deps=deps)

    def _fetch_metagraph(self) -> MetagraphData:
        neurons = list(self.neurons)
        uids = [getattr(neuron, "uid", index) for index, neuron in enumerate(neurons)]
        hotkeys = [
            getattr(neuron, "hotkey", f"hotkey-{index}") for index, neuron in enumerate(neurons)
        ]
        coldkeys = [getattr(neuron, "coldkey", None) for neuron in neurons]
        serving_hotkeys = [
            neuron.hotkey
            for neuron in neurons
            if getattr(getattr(neuron, "axon_info", None), "ip", None) not in (None, "0.0.0.0")
        ]
        stakes_length = len(neurons)
        zeros = [0.0] * stakes_length

        return MetagraphData(
            block=self.block_number,
            neurons=neurons,
            subnet_state={
                "alpha_stake": zeros.copy(),
                "tao_stake": zeros.copy(),
                "total_stake": zeros.copy(),
            },
            alpha_stake=zeros.copy(),
            tao_stake=zeros.copy(),
            total_stake=zeros.copy(),
            uids=uids,
            hotkeys=hotkeys,
            coldkeys=coldkeys,
            serving_hotkeys=serving_hotkeys,
        )

    def _fetch_associations(
        self, _subtensor: Any, netuid: int, block_hash: str | None
    ) -> dict[int, str]:
        self.env.fetch_log.append({"netuid": netuid, "block_hash": block_hash})
        return self.associations
