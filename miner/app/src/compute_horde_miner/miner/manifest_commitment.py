import logging

import bittensor
from compute_horde.manifest_utils import (
    format_manifest_commitment,
    parse_commitment_string,
)
from compute_horde_core.executor_class import ExecutorClass

logger = logging.getLogger(__name__)

# Re-export for backward compatibility
__all__ = [
    "format_manifest_commitment",
    "parse_commitment_string",
    "has_manifest_changed",
    "commit_manifest_to_subtensor",
]


def has_manifest_changed(
    current_manifest: dict[ExecutorClass, int], chain_commitment: str | None
) -> bool:
    """
    Compare current manifest with on-chain commitment.

    Returns True if different or if no commitment exists.
    """
    if not chain_commitment:
        return True

    chain_manifest = parse_commitment_string(chain_commitment)
    return current_manifest != chain_manifest


def commit_manifest_to_subtensor(
    manifest: dict[ExecutorClass, int],
    wallet: bittensor.wallet,
    subtensor: bittensor.subtensor,
    netuid: int,
) -> bool:
    """
    Commits manifest to knowledge commitments.

    Returns True if successful, False otherwise.

    Steps:
    1. Format manifest to commitment string
    2. Validate length <= 128 chars
    3. Call subtensor.commit(wallet, netuid, commitment_string)
    4. Handle rate limiting (100 blocks)

    Note: Empty manifests are allowed to support "pausing" a miner.
    """
    try:
        # Format manifest to commitment string (empty manifests result in empty string)
        commitment_string = format_manifest_commitment(manifest)

        logger.info(f"Committing manifest to chain: {commitment_string or '(empty)'}")

        # Commit to subtensor (empty string is valid for pausing)
        # Note: subtensor.commit() is rate limited to 100 blocks
        success = subtensor.commit(wallet, netuid, commitment_string)

        if success:
            logger.info("Successfully committed manifest to chain")
            return True
        else:
            logger.warning("Failed to commit manifest to chain")
            return False

    except ValueError as e:
        logger.error(f"Invalid manifest format: {e}")
        return False
    except Exception as e:
        logger.error(f"Error committing manifest to subtensor: {e}", exc_info=True)
        return False
