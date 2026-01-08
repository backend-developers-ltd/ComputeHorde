import logging

from compute_horde.manifest_utils import (
    extract_manifest_payload,
    format_manifest_commitment,
    merge_manifest,
)
from compute_horde_core.executor_class import ExecutorClass
from pylon_client.v1 import PylonClient, PylonResponseException

logger = logging.getLogger(__name__)

# Maximum size for the entire commitment (including other_data + envelope)
# This is the limit enforced by Bittensor subtensor.commit (Raw extrinsic)
MAX_COMMITMENT_BYTES = 128


def commit_manifest(manifest: dict[ExecutorClass, int], pylon_client: PylonClient) -> bool:
    """
    Commits manifest to knowledge commitments.

    Returns True if successful, False otherwise.

    Note: Empty manifests are allowed to support "pausing" a miner.
    """
    response = pylon_client.identity.get_own_commitment()
    current_commitment = response.commitment

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
        _, old_manifest = extract_manifest_payload(current_commitment)
        logger.info(
            f"Updating manifest commitment: old='{old_manifest or '(empty)'}' -> new='{new_manifest_string}'"
        )
    else:
        logger.info(f"Committing manifest to chain: {new_manifest_string or '(empty)'}")

    try:
        pylon_client.identity.set_commitment(merged_commitment.encode("utf-8"))
    except PylonResponseException as e:
        logger.warning("Failed to commit manifest to chain", exc_info=e)
        return False

    logger.info("Successfully committed manifest to chain")
    return True


__all__ = [
    "commit_manifest",
]
