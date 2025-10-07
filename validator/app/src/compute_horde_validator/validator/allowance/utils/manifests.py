import asyncio
import ipaddress
import itertools
import logging
import operator
from functools import reduce

from compute_horde.manifest_utils import parse_commitment_string
from compute_horde.miner_client.organic import OrganicMinerClient
from compute_horde_core.executor_class import ExecutorClass
from constance import config
from django.conf import settings as django_settings
from django.db import transaction
from django.db.models import Min, Q

from ...dynamic_config import get_miner_max_executors_per_class_sync
from ...locks import Lock, LockType
from ...models.allowance.internal import AllowanceMinerManifest, BlockAllowance, MinerAddress
from .. import settings
from ..types import ss58_address
from ..utils.supertensor import supertensor

logger = logging.getLogger(__name__)


def get_manifests(
    block_number: int, hotkeys: list[ss58_address]
) -> dict[tuple[ss58_address, ExecutorClass], int]:
    """
    Args:
        block_number: Block number to filter by (get manifests older than this block)
        hotkeys: List of hotkeys to get manifests for

    Returns:
        Dictionary mapping (hotkey, ExecutorClass) tuples to executor_count
        Missing hotkeys/executor_classes are filled with zeros
    """
    if not hotkeys:
        return {}

    # Get the newest manifest for each hotkey-executor_class combination
    # that has block_number <= the given block_number
    manifests = (
        AllowanceMinerManifest.objects.filter(
            miner_ss58address__in=hotkeys,
            block_number__lte=block_number,
        )
        .values("miner_ss58address", "executor_class", "executor_count", "block_number")
        .order_by("miner_ss58address", "executor_class", "-block_number")
        .distinct("miner_ss58address", "executor_class")
    )

    # Build result dictionary with tuple keys
    result = {}

    # Fill in the manifest data
    for manifest in manifests:
        hotkey = manifest["miner_ss58address"]
        executor_class = ExecutorClass(manifest["executor_class"])
        executor_count = manifest["executor_count"]

        result[(hotkey, executor_class)] = executor_count

    # Fill with zeros for missing executor classes
    all_executor_classes = list(ExecutorClass)
    for hotkey in hotkeys:
        for executor_class in all_executor_classes:
            key = (hotkey, executor_class)
            if key not in result:
                result[key] = 0

    return result


def get_current_manifests() -> dict[ss58_address, dict[ExecutorClass, int]]:
    """
    Returns:
        Dictionary mapping hotkey to dict of ExecutorClass and executor_count
    """
    # Get the newest manifest for each hotkey-executor_class combination
    manifests = (
        AllowanceMinerManifest.objects.values(
            "miner_ss58address", "executor_class", "executor_count", "block_number"
        )
        .order_by("miner_ss58address", "executor_class", "-block_number")
        .distinct("miner_ss58address", "executor_class")
    )

    result: dict[ss58_address, dict[ExecutorClass, int]] = {}

    # Fill in the manifest data
    for manifest in manifests:
        hotkey = manifest["miner_ss58address"]
        executor_class = ExecutorClass(manifest["executor_class"])
        executor_count = manifest["executor_count"]

        try:
            executor_dict = result[hotkey]
        except KeyError:
            result[hotkey] = {}
            executor_dict = result[hotkey]
        executor_dict[executor_class] = executor_count

    return result


def get_manifest_drops(
    block_number: int, hotkeys: list[ss58_address]
) -> dict[tuple[ss58_address, ExecutorClass], int]:
    """
    Find hotkey-executorClass pairs that have been dropped since the given block number.
    Args:
        block_number: Block number to filter by (only look for manifests newer than this block)
        hotkeys: List of hotkeys to scan

    Returns:
        Dictionary mapping (hotkey, executor_class) tuples to the earliest block number when the drop happened
    """
    if not hotkeys:
        return {}

    # Get all manifest records that are drops (is_drop=True)
    # and have block_number >= the given block_number (newer than or equal to the block)
    # We need to find the earliest block_number for each (hotkey, executor_class) combination

    drops = (
        AllowanceMinerManifest.objects.filter(
            miner_ss58address__in=hotkeys,
            block_number__gt=block_number,
            is_drop=True,
        )
        .values("miner_ss58address", "executor_class")
        .annotate(earliest_block=Min("block_number"))
    )

    # Convert to dictionary with tuple keys and earliest block values
    result = {}
    for drop in drops:
        hotkey = drop["miner_ss58address"]
        executor_class = ExecutorClass(drop["executor_class"])
        earliest_block = drop["earliest_block"]
        result[(hotkey, executor_class)] = earliest_block

    return result


def event_loop():
    try:
        return asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop


def fetch_manifests_from_commitments(
    hotkeys: list[ss58_address],
) -> dict[tuple[ss58_address, ExecutorClass], int]:
    """
    Fetch manifests from Bittensor knowledge commitments.

    Only includes results for miners that have commitments on chain.
    """
    subtensor_instance = supertensor().subtensor()
    netuid = django_settings.BITTENSOR_NETUID

    result = {}
    for hotkey in hotkeys:
        try:
            # Get commitment from chain
            commitment = subtensor_instance.get_commitment(
                netuid=netuid,
                hotkey=hotkey,
            )

            if commitment:
                # Parse commitment string to manifest dict
                manifest = parse_commitment_string(commitment)

                # Add all executor classes from manifest to result
                for executor_class, count in manifest.items():
                    result[(hotkey, executor_class)] = count

                logger.debug(f"Fetched commitment for {hotkey}: {commitment}")
            else:
                logger.debug(f"No commitment found for {hotkey}")

        except Exception as e:
            logger.warning(f"Failed to get commitment for {hotkey}: {e}")
            continue

    logger.info(f"Fetched {len(result)} manifest entries from {len(hotkeys)} hotkeys via commitments")
    return result


def sync_manifests():
    block = supertensor().get_current_block()
    neurons = supertensor().get_shielded_neurons()
    max_executors_per_class = get_miner_max_executors_per_class_sync()

    # Check if we should read from knowledge commitments or via WebSocket
    if config.DYNAMIC_USE_MANIFEST_COMMITMENTS:
        logger.info("Fetching manifests from Bittensor knowledge commitments")
        hotkeys = [n.hotkey for n in neurons]
        new_manifests = fetch_manifests_from_commitments(hotkeys)
        # For commitments, we don't have miner addresses, so we create a minimal list
        miners = [
            (
                n.hotkey,
                getattr(n.axon_info, "shield_address", str(n.axon_info.ip)),
                n.axon_info.port,
            )
            for n in neurons
            if n.axon_info.port
        ]
    else:
        logger.info("Fetching manifests from miners via WebSocket")
        miners = [
            (
                n.hotkey,
                getattr(n.axon_info, "shield_address", str(n.axon_info.ip)),
                n.axon_info.port,
            )
            for n in neurons
            if n.axon_info.port
        ]
        new_manifests = event_loop().run_until_complete(fetch_manifests_from_miners(miners))
    with transaction.atomic():
        with Lock(LockType.ALLOWANCE_BLOCK_INJECTION, 10.0):
            # This will throw an error if the lock cannot be obtained in 10.0s and that's correct
            MinerAddress.objects.all().delete()
            MinerAddress.objects.bulk_create(
                [
                    MinerAddress(
                        hotkey_ss58address=miner[0],
                        address=miner[1],
                        ip_version=6 if isinstance(miner[1], ipaddress.IPv6Address) else 4,
                        port=miner[2],
                    )
                    for miner in miners
                ]
            )
            old_manifests = get_manifests(block, [n.hotkey for n in neurons])
            manifests_to_inject = []
            block_allowances_to_invalidate = []
            # Use a set to avoid duplicates when combining new and old manifests
            all_manifest_keys = set(itertools.chain(new_manifests.keys(), old_manifests.keys()))
            for miner_hotkey, executor_class in all_manifest_keys:
                is_drop = False
                if new_manifests.get((miner_hotkey, executor_class), 0) < old_manifests.get(
                    (miner_hotkey, executor_class), 0
                ):
                    is_drop = True
                    block_allowances_to_invalidate.append((miner_hotkey, executor_class.value))
                manifests_to_inject.append(
                    AllowanceMinerManifest(
                        miner_ss58address=miner_hotkey,
                        block_number=block,
                        success=(miner_hotkey, executor_class) in new_manifests,
                        executor_class=executor_class.value,
                        is_drop=is_drop,
                        executor_count=min(
                            new_manifests.get((miner_hotkey, executor_class), 0),
                            max_executors_per_class.get(executor_class, float("inf")),
                        ),
                    )
                )
            if manifests_to_inject:
                AllowanceMinerManifest.objects.bulk_create(manifests_to_inject)
                logger.info(f"Created {len(manifests_to_inject)} manifests for block {block}")
            if block_allowances_to_invalidate:
                updated_count = BlockAllowance.objects.filter(
                    reduce(
                        operator.or_,
                        [
                            Q(miner_ss58=miner, executor_class=executor_class)
                            for miner, executor_class in block_allowances_to_invalidate
                        ],
                    )
                ).update(invalidated_at_block=block)
                logger.info(f"Invalidated {updated_count} block allowances for block {block}")


async def fetch_manifests_from_miners(
    miners: list[tuple[ss58_address, str, int]],
) -> dict[tuple[ss58_address, ExecutorClass], int]:
    """Only includes results for miners that have replied successfully."""

    my_keypair = supertensor().wallet().get_hotkey()

    miner_clients = [
        OrganicMinerClient(
            miner_hotkey=miner[0],
            miner_address=miner[1],
            miner_port=miner[2],
            job_uuid="ignore",
            my_keypair=my_keypair,
        )
        for miner in miners
    ]

    try:
        # workaround for circular imports through `validator/tasks.py`
        from ...clean_me_up import get_single_manifest

        logger.info(f"Scraping manifests for {len(miner_clients)} miners")
        tasks = [
            asyncio.create_task(
                get_single_manifest(client, settings.MANIFEST_FETCHING_TIMEOUT),
                name=f"{client.miner_hotkey}.get_manifest",
            )
            for client in miner_clients
        ]
        results = await asyncio.gather(*tasks)

        # Process results and build the manifest dictionary
        result_manifests = {}
        for hotkey, manifest in results:
            for executor_class in ExecutorClass:
                if manifest is not None:
                    result_manifests[hotkey, executor_class] = manifest.get(executor_class, 0)

        return result_manifests

    finally:
        close_tasks = [
            asyncio.create_task(client.close(), name=f"{client.miner_hotkey}.close")
            for client in miner_clients
        ]
        await asyncio.gather(*close_tasks, return_exceptions=True)
