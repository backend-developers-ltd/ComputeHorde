import logging
import re

from compute_horde.manifest_utils import (
    extract_manifest_payload,
    format_manifest_commitment,
    merge_manifest,
)
from compute_horde_core.executor_class import ExecutorClass
from django.conf import settings
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
    response = pylon_client.v1.identity.get_own_commitment()
    try:
        current_commitment = (
            bytes.fromhex(response.commitment.removeprefix("0x")).decode("utf-8")
            if response.commitment
            else ""
        )
    except ValueError:
        logger.exception("Failed to decode existing commitment, aborting commit")
        return False

    new_manifest_string = format_manifest_commitment(manifest)

    try:
        # Preserve shield and FactoryHorde (mechanism 1) envelopes, discarding old manifests and hex garbage.
        preserved = "".join(re.findall(r"<[DF]:[^<>]*>", current_commitment))
        merged_commitment = merge_manifest(preserved, new_manifest_string)
    except Exception as e:
        logger.error(f"Failed to merge manifest with existing commitment: {e}, aborting commit")
        return False

    if (
        current_commitment == merged_commitment
        and response.block.number - response.commitment_block_number
        < settings.MANIFEST_COMMITMENT_REFRESH_BLOCKS
    ):
        logger.info("Manifest unchanged, skipping commit")
        return True

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
        pylon_client.v1.identity.set_commitment(merged_commitment.encode("utf-8"))
    except PylonResponseException as e:
        logger.warning("Failed to commit manifest to chain", exc_info=e)
        return False

    logger.info("Successfully committed manifest to chain")
    return True


__all__ = [
    "commit_manifest",
]
