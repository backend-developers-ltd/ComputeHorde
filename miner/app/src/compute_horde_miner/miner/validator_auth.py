"""
Replay protection for validator authentication frames: a frame is accepted only when its
timestamp is current and its signature has not been accepted before.
"""

import datetime as dt
import re
import time

from compute_horde.protocol_messages import ValidatorAuthForMiner
from django.db import IntegrityError

from compute_horde_miner.miner.models import ConsumedValidatorAuth

AUTH_MESSAGE_MAX_AGE = 10
AUTH_MESSAGE_MAX_CLOCK_SKEW = 5
# Consumed signatures are kept well past the age limit, after which the timestamp check alone
# rejects the frame.
CONSUMED_AUTH_RETENTION = dt.timedelta(minutes=1)
# How sign_blob writes a signature, in any letter case. The verifier also strips repeated "0x"
# prefixes, which would give one signature many more spellings.
SIGNATURE_FORMAT = re.compile(r"0x[0-9a-fA-F]{128}")


def auth_timestamp_error(msg: ValidatorAuthForMiner) -> str | None:
    now = time.time()
    if msg.timestamp < now - AUTH_MESSAGE_MAX_AGE:
        return "msg too old"
    if msg.timestamp > now + AUTH_MESSAGE_MAX_CLOCK_SKEW:
        return "msg timestamp is in the future"
    return None


def auth_signature_bytes(msg: ValidatorAuthForMiner) -> bytes | None:
    """The frame's 64-byte signature, or None when it is not spelled as "0x" and 128 hex digits."""
    if not SIGNATURE_FORMAT.fullmatch(msg.signature):
        return None
    return bytes.fromhex(msg.signature[2:])


async def consume_auth_signature(msg: ValidatorAuthForMiner, signature: bytes) -> bool:
    """
    Records the frame's signature, as returned by auth_signature_bytes, as used; False means it
    was used before, so the frame is a replay. Call it only for a frame whose signature
    verified, so that unauthenticated clients cannot fill the table.
    """
    await ConsumedValidatorAuth.objects.filter(
        timestamp__lt=dt.datetime.now(dt.UTC) - CONSUMED_AUTH_RETENTION
    ).adelete()
    try:
        await ConsumedValidatorAuth.objects.acreate(
            signature=signature.hex(),
            validator_hotkey=msg.validator_hotkey,
            timestamp=dt.datetime.fromtimestamp(msg.timestamp, dt.UTC),
        )
    except IntegrityError:
        return False
    return True
