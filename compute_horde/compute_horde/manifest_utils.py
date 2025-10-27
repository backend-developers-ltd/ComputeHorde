"""
Utilities for working with miner manifests.
"""

import logging

from compute_horde_core.executor_class import ExecutorClass

logger = logging.getLogger(__name__)

MAX_MANIFEST_COMMITMENT_LENGTH = 128


def _iter_pairs(text: str):
    for fragment in text.split(";"):
        fragment = fragment.strip()
        if "=" not in fragment:
            continue
        key, value = (part.strip() for part in fragment.split("=", 1))
        if key and value:
            yield key, value


def format_manifest_commitment(manifest: dict[ExecutorClass, int]) -> str:
    """
    Converts manifest dict to commitment string format.

    Example: "spin_up-4min.gpu-24gb=2;always_on.gpu-24gb=3"

    Raises:
        ValueError: If the resulting string exceeds MAX_MANIFEST_COMMITMENT_LENGTH
    """
    if not manifest:
        return ""

    sorted_items = sorted(manifest.items(), key=lambda x: x[0].value)

    parts = [f"{executor_class.value}={count}" for executor_class, count in sorted_items]
    commitment_string = ";".join(parts)

    if len(commitment_string) > MAX_MANIFEST_COMMITMENT_LENGTH:
        raise ValueError(
            f"Commitment string length {len(commitment_string)} exceeds maximum {MAX_MANIFEST_COMMITMENT_LENGTH}"
        )

    return commitment_string


def parse_commitment_string(commitment: str) -> dict[ExecutorClass, int]:
    """
    Parses commitment string back to manifest dict.

    Returns empty dict if invalid/empty.
    """
    if not commitment or not commitment.strip():
        return {}

    manifest: dict[ExecutorClass, int] = {}
    try:
        for key, value in _iter_pairs(commitment.strip()):
            try:
                executor_class = ExecutorClass(key)
                manifest[executor_class] = int(value)
            except (ValueError, KeyError):
                logger.debug(f"Ignoring non-manifest commitment pair: {key}={value}")
    except Exception as exc:
        logger.error(f"Error parsing commitment string '{commitment}': {exc}")
        return {}

    return manifest
