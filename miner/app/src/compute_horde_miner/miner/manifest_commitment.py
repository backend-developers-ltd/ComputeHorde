import logging
import re

import bittensor
from compute_horde.manifest_utils import (
    format_manifest_commitment,
    parse_commitment_string,
)
from compute_horde_core.executor_class import ExecutorClass

logger = logging.getLogger(__name__)

# Envelope pattern for manifest commitments: <<CHM:...>>
MANIFEST_ENVELOPE_PATTERN = re.compile(r"<<CHM:([^>]*)>>")


def extract_manifest_payload(raw_commitment: str) -> tuple[str, str]:
    """
    Extract manifest payload from raw commitment data.

    Returns (other_data, manifest_payload) where:
    - other_data: everything except the manifest envelope
    - manifest_payload: the content inside <<CHM:...>>
    """
    match = MANIFEST_ENVELOPE_PATTERN.search(raw_commitment)
    if not match:
        return raw_commitment, ""

    manifest_payload = match.group(1)
    other_data = raw_commitment[: match.start()] + raw_commitment[match.end() :]
    other_data = other_data.strip()

    return other_data, manifest_payload


def merge_manifest(raw_commitment: str, new_manifest: str) -> str:
    """
    Merge new manifest into raw commitment.

    If an existing manifest envelope is found, it's replaced in-place.
    Otherwise, the new envelope is appended to the end.

    Args:
        raw_commitment: The current commitment string (may be empty)
        new_manifest: The new manifest string (from format_manifest_commitment)

    Returns:
        The merged commitment string with manifest envelope
    """
    if not raw_commitment:
        return f"<<CHM:{new_manifest}>>"

    other_data, _ = extract_manifest_payload(raw_commitment)

    new_envelope = f"<<CHM:{new_manifest}>>"

    if other_data:
        return f"{other_data}{new_envelope}"
    else:
        return new_envelope


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
    try:
        try:
            metagraph = subtensor.metagraph(netuid)
            hotkey = wallet.hotkey.ss58_address
            uid = None
            for neuron in metagraph.neurons:
                if neuron.hotkey == hotkey:
                    uid = neuron.uid
                    break

            if uid is not None:
                current_commitment = subtensor.get_commitment(netuid=netuid, uid=uid) or ""
            else:
                logger.debug(
                    f"Could not find uid for hotkey {hotkey} on netuid {netuid}, using empty commitment"
                )
                current_commitment = ""
        except Exception as e:
            logger.debug(f"Failed to read current commitment: {e}, will commit without merging")
            current_commitment = ""

        new_manifest_string = format_manifest_commitment(manifest)

        try:
            merged_commitment = merge_manifest(current_commitment, new_manifest_string)
        except Exception as e:
            logger.error(f"Failed to merge manifest with existing commitment: {e}, aborting commit")
            return False

        if current_commitment and current_commitment != merged_commitment:
            other_data, old_manifest = extract_manifest_payload(current_commitment)
            logger.info(
                f"Updating manifest commitment: old='{old_manifest or '(empty)'}' -> new='{new_manifest_string}' "
                f"(preserving other_data)"
            )
        else:
            logger.info(f"Committing manifest to chain: {new_manifest_string or '(empty)'}")

        success = subtensor.commit(wallet, netuid, merged_commitment)

        if success:
            logger.info("Successfully committed manifest to chain")
            return True

        logger.warning("Failed to commit manifest to chain")
        return False
    except Exception as e:
        logger.error(f"Error committing manifest to subtensor: {e}", exc_info=True)
        return False


__all__ = [
    "parse_commitment_string",
    "commit_manifest_to_subtensor",
    "extract_manifest_payload",
    "merge_manifest",
]
