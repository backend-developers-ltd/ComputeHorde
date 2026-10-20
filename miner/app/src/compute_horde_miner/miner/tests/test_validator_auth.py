import datetime as dt
import time

import pytest

from compute_horde_miner.miner.models import ConsumedValidatorAuth
from compute_horde_miner.miner.tests.validator import auth_message
from compute_horde_miner.miner.validator_auth import (
    AUTH_MESSAGE_MAX_AGE,
    AUTH_MESSAGE_MAX_CLOCK_SKEW,
    CONSUMED_AUTH_RETENTION,
    auth_signature_bytes,
    auth_timestamp_error,
    consume_auth_signature,
)

SIGNATURE_HEX = "0123456789abcdef" * 8


@pytest.mark.parametrize(
    ("timestamp_offset", "error"),
    [
        (0, None),
        (-AUTH_MESSAGE_MAX_AGE + 1, None),
        (-AUTH_MESSAGE_MAX_AGE - 1, "msg too old"),
        (AUTH_MESSAGE_MAX_CLOCK_SKEW - 1, None),
        (AUTH_MESSAGE_MAX_CLOCK_SKEW + 1, "msg timestamp is in the future"),
    ],
)
def test_auth_timestamp_error(timestamp_offset: int, error: str | None):
    assert auth_timestamp_error(auth_message(int(time.time()) + timestamp_offset)) == error


@pytest.mark.parametrize(
    ("signature", "expected"),
    [
        pytest.param("0x" + SIGNATURE_HEX, bytes.fromhex(SIGNATURE_HEX), id="canonical"),
        pytest.param("0x" + SIGNATURE_HEX.upper(), bytes.fromhex(SIGNATURE_HEX), id="upper_case"),
        pytest.param("0x0x" + SIGNATURE_HEX, None, id="double_prefix"),
        pytest.param("0x" * 64 + SIGNATURE_HEX, None, id="repeated_prefix"),
        pytest.param("0X" + SIGNATURE_HEX, None, id="upper_case_prefix"),
        pytest.param(SIGNATURE_HEX, None, id="no_prefix"),
        pytest.param("0x" + SIGNATURE_HEX + "\n", None, id="trailing_newline"),
        pytest.param("0x" + SIGNATURE_HEX[:-2], None, id="too_short"),
        pytest.param("0x" + SIGNATURE_HEX + "00", None, id="too_long"),
    ],
)
def test_auth_signature_bytes(signature: str, expected: bytes | None):
    msg = auth_message().model_copy(update={"signature": signature})

    assert auth_signature_bytes(msg) == expected


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_consume_auth_signature_is_single_use_and_prunes_expired_rows():
    expired = dt.datetime.now(dt.UTC) - CONSUMED_AUTH_RETENTION - dt.timedelta(seconds=1)
    await ConsumedValidatorAuth.objects.acreate(
        signature="expired", validator_hotkey="hotkey", timestamp=expired
    )
    msg = auth_message()
    signature = auth_signature_bytes(msg)
    assert signature is not None

    assert await consume_auth_signature(msg, signature) is True
    assert await consume_auth_signature(msg, signature) is False

    consumed = [
        row
        async for row in ConsumedValidatorAuth.objects.values(
            "signature", "validator_hotkey", "timestamp"
        )
    ]
    assert consumed == [
        {
            "signature": signature.hex(),
            "validator_hotkey": msg.validator_hotkey,
            "timestamp": dt.datetime.fromtimestamp(msg.timestamp, dt.UTC),
        }
    ]
