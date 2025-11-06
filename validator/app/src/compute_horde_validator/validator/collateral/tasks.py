import logging
from collections.abc import Callable
from decimal import Decimal
from typing import Any

import turbobt
from asgiref.sync import async_to_sync
from compute_horde.smart_contracts.utils import get_web3_connection
from django.conf import settings
from web3 import Web3

from compute_horde_validator.celery import app
from compute_horde_validator.validator.models import Miner, SystemEvent

from ..allowance.default import allowance
from ..allowance.utils.supertensor import supertensor
from .default import Collateral, collateral

logger = logging.getLogger(__name__)


class CollateralTaskDependencies:
    def __init__(
        self,
        *,
        fetch_evm_key_associations: Callable[[turbobt.Subtensor, int, str | None], dict[int, str]]
        | None = None,
        web3: Callable[[str], Web3] | None = None,
        collateral: Callable[[], Any] | None = None,
        system_events: Callable[[], Any] | None = None,
    ) -> None:
        self._fetch_evm_key_associations = (
            fetch_evm_key_associations or self._default_fetch_evm_key_associations
        )
        self._web3 = web3 or self._default_web3
        self._collateral = collateral or self._default_collateral
        self._system_events = system_events or self._default_system_events

    def fetch_evm_key_associations(
        self, subtensor: turbobt.Subtensor, netuid: int, block_hash: str | None
    ) -> dict[int, str]:
        return self._fetch_evm_key_associations(subtensor, netuid, block_hash)

    def web3(self, network: str) -> Web3:
        return self._web3(network)

    def collateral(self) -> Any:
        return self._collateral()

    def system_events(self) -> Any:
        return self._system_events()

    def _default_fetch_evm_key_associations(
        self, subtensor: turbobt.Subtensor, netuid: int, block_hash: str | None
    ) -> dict[int, str]:
        return get_evm_key_associations(
            subtensor=subtensor,
            netuid=netuid,
            block_hash=block_hash,
        )

    def _default_web3(self, network: str) -> Web3:
        return get_web3_connection(network=network)

    def _default_collateral(self) -> Collateral:
        return collateral()

    def _default_system_events(self) -> Any:
        return SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS)


def get_miner_collateral(
    w3: Web3,
    contract_address: str,
    miner_address: str,
    block_identifier: int | None = None,
) -> int:
    """
    Query the collateral amount for a given miner address.
    Args:
        w3: Web3 instance to use for blockchain interaction.
        contract_address: Address of the Collateral contract.
        miner_address: EVM address of the miner to query.
        block_identifier: Block number to query the latest block. Defaults to None, which queries the latest block.
    Returns:
        Collateral amount in Wei
    """
    abi = collateral()._get_collateral_abi()
    contract_checksum_address = w3.to_checksum_address(contract_address)
    miner_checksum_address = w3.to_checksum_address(miner_address)

    contract = w3.eth.contract(address=contract_checksum_address, abi=abi)
    collateral_amount: int = contract.functions.collaterals(miner_checksum_address).call(
        block_identifier=block_identifier
    )
    return collateral_amount


def get_evm_key_associations(
    subtensor: turbobt.Subtensor, netuid: int, block_hash: str | None = None
) -> dict[int, str]:
    """
    Retrieve all EVM key associations for a specific subnet.
    Arguments:
        subtensor (turbobt.Subtensor): The Subtensor object to use for querying the network.
        netuid (int): The NetUID for which to retrieve EVM key associations.
        block_hash (str | None, optional): The block hash to query. Defaults to None, which queries the latest block.
    Returns:
        dict: A dictionary mapping UIDs (int) to their associated EVM key addresses (str).
    """
    associations = async_to_sync(subtensor.subtensor_module.AssociatedEvmAddress.fetch)(
        netuid,
        block_hash=block_hash,
    )
    return {uid: evm_address for (netuid, uid), (evm_address, block) in associations}


@app.task
def sync_collaterals(deps: CollateralTaskDependencies | None = None) -> None:
    """
    Synchronizes miner evm addresses and collateral amounts.
    """
    deps = deps or CollateralTaskDependencies()
    try:
        metagraph = allowance().get_metagraph()
    except Exception as e:
        msg = f"Error getting metagraph data for collateral sync: {e}"
        logger.warning(msg)
        deps.system_events().create(
            type=SystemEvent.EventType.COLLATERAL_SYNCING,
            subtype=SystemEvent.EventSubType.FAILURE,
            long_description=msg,
            data={"error": str(e)},
        )
        return

    block_hash = metagraph.block_hash
    block_number = metagraph.block
    hotkeys = metagraph.hotkeys

    associations = deps.fetch_evm_key_associations(
        supertensor().bittensor.subtensor,
        settings.BITTENSOR_NETUID,
        block_hash,
    )
    miners = Miner.objects.filter(hotkey__in=hotkeys)
    w3 = deps.web3(settings.BITTENSOR_NETWORK)
    collateral_client = deps.collateral()
    contract_address = collateral_client._get_collateral_contract_address()

    to_update = []
    for miner in miners:
        if not miner.uid:
            continue

        evm_address = associations.get(miner.uid)
        miner.evm_address = evm_address
        to_update.append(miner)

        if not miner.evm_address:
            continue

        if contract_address:
            try:
                collateral_wei = get_miner_collateral(
                    w3, contract_address, miner.evm_address, block_number
                )
                miner.collateral_wei = Decimal(collateral_wei)
            except Exception as e:
                msg = f"Error while fetching miner collateral: {e}"
                logger.warning(msg)
                deps.system_events().create(
                    type=SystemEvent.EventType.COLLATERAL_SYNCING,
                    subtype=SystemEvent.EventSubType.GETTING_MINER_COLLATERAL_FAILED,
                    long_description=msg,
                    data={
                        "block": block_number,
                        "miner_hotkey": miner.hotkey,
                        "evm_address": evm_address,
                    },
                )

    Miner.objects.bulk_update(to_update, fields=["evm_address", "collateral_wei"])
