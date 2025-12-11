"""
Utilities for working with miner manifests.
"""

import logging
import re

from compute_horde_core.executor_class import ExecutorClass

logger = logging.getLogger(__name__)

MAX_MANIFEST_COMMITMENT_BYTES = 128

# Envelope pattern for manifest commitments: <M:...>
MANIFEST_ENVELOPE_PATTERN = re.compile(r"<M:([^>]*)>")

EXECUTOR_CLASS_TO_SHORT: dict[ExecutorClass, str] = {
    ExecutorClass.always_on__gpu_24gb: "A",
    ExecutorClass.spin_up_4min__gpu_24gb: "S",
    ExecutorClass.always_on__llm__a6000: "L",
    ExecutorClass.always_on__test: "T",
    ExecutorClass.always_on__cpu__8c__16gb: "C",
}

SHORT_TO_EXECUTOR_CLASS: dict[str, ExecutorClass] = {
    v: k for k, v in EXECUTOR_CLASS_TO_SHORT.items()
}


def _iter_pairs(text: str):
    for fragment in text.split(";"):
        fragment = fragment.strip()
        if len(fragment) < 2:
            logger.debug(f"Ignoring malformed commitment fragment (too short): '{fragment}'")
            continue
        key = fragment[0]
        value = fragment[1:]
        if key and value:
            yield key, value


def format_manifest_commitment(manifest: dict[ExecutorClass, int]) -> str:
    """
    Converts manifest dict to compact commitment string format using short codes.

    Example: "S2;A3"

    Raises:
        ValueError: If the resulting string exceeds MAX_MANIFEST_COMMITMENT_BYTES
    """
    if not manifest:
        return ""

    sorted_items = sorted(manifest.items(), key=lambda x: EXECUTOR_CLASS_TO_SHORT[x[0]])

    parts = [
        f"{EXECUTOR_CLASS_TO_SHORT[executor_class]}{count}"
        for executor_class, count in sorted_items
    ]
    commitment_string = ";".join(parts)

    commitment_bytes = len(commitment_string.encode("utf-8"))
    if commitment_bytes > MAX_MANIFEST_COMMITMENT_BYTES:
        raise ValueError(
            f"Commitment string size {commitment_bytes} bytes exceeds maximum {MAX_MANIFEST_COMMITMENT_BYTES} bytes"
        )

    return commitment_string


def parse_commitment_string(commitment: str) -> dict[ExecutorClass, int]:
    """
    Parses commitment string back to manifest dict.

    Uses compact format (e.g., "A3;S2").

    Returns empty dict if invalid/empty.
    """
    if not commitment or not commitment.strip():
        return {}

    manifest: dict[ExecutorClass, int] = {}
    try:
        for key, value in _iter_pairs(commitment.strip()):
            try:
                executor_class = SHORT_TO_EXECUTOR_CLASS[key]
                manifest[executor_class] = int(value)
            except (ValueError, KeyError):
                logger.debug(f"Ignoring malformed commitment pair: {key}{value}")
    except Exception as exc:
        logger.error(f"Error parsing commitment string '{commitment}': {exc}")
        return {}

    return manifest


def extract_manifest_payload(raw_commitment: str) -> tuple[str, str]:
    """
    Extract manifest payload from raw commitment data.

    Returns (other_data, manifest_payload) where:
    - other_data: everything except manifest envelopes
    - manifest_payload: the content inside the first <M:...> (empty string if no envelope)
    """
    match = MANIFEST_ENVELOPE_PATTERN.search(raw_commitment)
    if not match:
        return raw_commitment, ""

    manifest_payload = match.group(1)
    other_data = MANIFEST_ENVELOPE_PATTERN.sub("", raw_commitment)
    other_data = other_data.strip()

    return other_data, manifest_payload


def merge_manifest(raw_commitment: str, new_manifest: str) -> str:
    """
    Merge new manifest into raw commitment.

    The manifest envelope is ALWAYS appended at the end of the commitment string.
    If an existing manifest envelope is found anywhere in the commitment, it's removed first.

    Args:
        raw_commitment: The current commitment string (may be empty)
        new_manifest: The new manifest string (from format_manifest_commitment)

    Returns:
        The merged commitment string with manifest envelope at the end
    """
    if not raw_commitment:
        return f"<M:{new_manifest}>"

    other_data, _ = extract_manifest_payload(raw_commitment)

    new_envelope = f"<M:{new_manifest}>"

    if other_data:
        return f"{other_data}{new_envelope}"
    else:
        return new_envelope
