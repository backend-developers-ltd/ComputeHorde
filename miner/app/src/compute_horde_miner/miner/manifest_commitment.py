import logging
import time

import bittensor
from compute_horde.manifest_utils import (
    extract_manifest_payload,
    format_manifest_commitment,
    merge_manifest,
)
from compute_horde_core.executor_class import ExecutorClass

logger = logging.getLogger(__name__)

# Maximum size for the entire commitment (including other_data + envelope)
# This is the limit enforced by Bittensor subtensor.commit (Raw extrinsic)
MAX_COMMITMENT_BYTES = 128

# UID cache: {(hotkey, netuid): (uid, timestamp)}
_uid_cache: dict[tuple[str, int], tuple[int, float]] = {}
_UID_CACHE_TTL = 300  # 5 minutes


def _get_uid(hotkey: str, netuid: int, subtensor: bittensor.subtensor) -> int | None:
    """
    Get UID for hotkey, using cache if available, otherwise fetch from metagraph.

    Returns None if hotkey not found in metagraph.
    """
    cache_key = (hotkey, netuid)
    current_time = time.time()

    if cache_key in _uid_cache:
        cached_uid, timestamp = _uid_cache[cache_key]
        if current_time - timestamp < _UID_CACHE_TTL:
            logger.debug(f"Using cached UID {cached_uid} for hotkey {hotkey}")
            return cached_uid
        else:
            del _uid_cache[cache_key]

    logger.debug(f"Fetching metagraph to find UID for hotkey {hotkey}")
    metagraph = subtensor.metagraph(netuid)
    uid: int | None = None
    for neuron in metagraph.neurons:
        if neuron.hotkey == hotkey:
            uid = neuron.uid
            break

    if uid is not None:
        _uid_cache[cache_key] = (uid, current_time)
        logger.debug(f"Cached UID {uid} for hotkey {hotkey}")

    return uid


def commit_manifest_to_subtensor(
    manifest: dict[ExecutorClass, int],
    wallet: bittensor.wallet,
    subtensor: bittensor.subtensor,
    netuid: int,
) -> bool:
    """
    Commits manifest to knowledge commitments.

    Returns True if successful, False otherwise.

    Note: Empty manifests are allowed to support "pausing" a miner.
    """
    hotkey = wallet.hotkey.ss58_address
    uid = _get_uid(hotkey, netuid, subtensor)

    if uid is None:
        logger.error(
            f"Could not find uid for hotkey {hotkey} on netuid {netuid}, using empty commitment"
        )
        return False

    current_commitment = subtensor.get_commitment(netuid=netuid, uid=uid)

    new_manifest_string = format_manifest_commitment(manifest)

    if current_commitment:
        _, current_manifest_payload = extract_manifest_payload(current_commitment)
        if current_manifest_payload == new_manifest_string:
            logger.info("Manifest unchanged, skipping commit")
            return True

    try:
        merged_commitment = merge_manifest(current_commitment, new_manifest_string)
    except Exception as e:
        logger.error(f"Failed to merge manifest with existing commitment: {e}, aborting commit")
        return False

    commitment_bytes = len(merged_commitment.encode("utf-8"))
    if commitment_bytes > MAX_COMMITMENT_BYTES:
        logger.error(
            f"Merged commitment size ({commitment_bytes} bytes) exceeds limit ({MAX_COMMITMENT_BYTES} bytes), "
        )
        return False

    if current_commitment:
        other_data, old_manifest = extract_manifest_payload(current_commitment)
        logger.info(
            f"Updating manifest commitment: old='{old_manifest or '(empty)'}' -> new='{new_manifest_string}'"
        )
    else:
        logger.info(f"Committing manifest to chain: {new_manifest_string or '(empty)'}")

    success = subtensor.commit(wallet, netuid, merged_commitment)

    if success:
        logger.info("Successfully committed manifest to chain")
        return True

    logger.warning("Failed to commit manifest to chain")
    return False


__all__ = [
    "commit_manifest_to_subtensor",
]
