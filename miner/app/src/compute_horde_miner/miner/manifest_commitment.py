import logging

import bittensor
from compute_horde_core.executor_class import ExecutorClass

logger = logging.getLogger(__name__)

MAX_COMMITMENT_LENGTH = 128


def format_manifest_commitment(manifest: dict[ExecutorClass, int]) -> str:
    """
    Converts manifest dict to commitment string format.

    Example: "spin_up-4min.gpu-24gb=2;always_on.gpu-24gb=3"

    - Sorted by executor class name for consistency
    - No trailing semicolon
    - Max 128 chars

    Raises:
        ValueError: If the resulting string exceeds MAX_COMMITMENT_LENGTH
    """
    if not manifest:
        return ""

    # Sort by executor class value (string) for consistency
    sorted_items = sorted(manifest.items(), key=lambda x: x[0].value)

    # Format as key=value pairs joined by semicolon
    parts = [f"{executor_class.value}={count}" for executor_class, count in sorted_items]
    commitment_string = ";".join(parts)

    if len(commitment_string) > MAX_COMMITMENT_LENGTH:
        raise ValueError(
            f"Commitment string length {len(commitment_string)} exceeds maximum {MAX_COMMITMENT_LENGTH}"
        )

    return commitment_string


def parse_commitment_string(commitment: str) -> dict[ExecutorClass, int]:
    """
    Parses commitment string back to manifest dict.

    Returns empty dict if invalid/empty.
    """
    if not commitment or not commitment.strip():
        return {}

    manifest = {}
    try:
        # Split by semicolon
        pairs = commitment.split(";")
        for pair in pairs:
            if "=" not in pair:
                logger.warning(f"Invalid commitment pair (missing '='): {pair}")
                continue

            key, value = pair.split("=", 1)
            key = key.strip()
            value = value.strip()

            # Try to parse as ExecutorClass
            try:
                executor_class = ExecutorClass(key)
                count = int(value)
                manifest[executor_class] = count
            except (ValueError, KeyError) as e:
                logger.warning(f"Invalid commitment pair {pair}: {e}")
                continue

    except Exception as e:
        logger.error(f"Error parsing commitment string '{commitment}': {e}")
        return {}

    return manifest


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
