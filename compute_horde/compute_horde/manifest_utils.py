"""
Utilities for working with miner manifests and knowledge commitments.

These functions are shared between miner and validator for consistent
manifest format and parsing.
"""

import logging

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
