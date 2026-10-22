import time
from collections.abc import Callable
from typing import Any

import pytest
from compute_horde.protocol_consts import TRUSTED_MINER_FAKE_KEY

from compute_horde_miner.miner.models import Validator
from compute_horde_miner.miner.tests.validator import (
    MANIFEST_MESSAGE,
    auth_message,
    fake_validator,
    miner_hotkey,
    organic_client_reconnect_greeting,
    received_output,
    validator_hotkey,
)

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.django_db(transaction=True),
    pytest.mark.usefixtures("authentication_on", "mock_executor_manager_class"),
]


def rejected(reason: str) -> list[dict[str, Any]]:
    return [
        {
            "message_type": "GenericError",
            "details": f"Validator {validator_hotkey()} not authenticated due to: {reason}",
        },
        {"type": "websocket.close", "code": 1000},
    ]


async def authentication_response(frame: str) -> list[dict[str, Any]]:
    async with fake_validator() as communicator:
        await communicator.send_to(frame)
        return await received_output(communicator, 1)


async def test_replayed_auth_frame_is_rejected(validator: Validator):
    frame = auth_message().model_dump_json()
    async with fake_validator() as first, fake_validator() as second:
        await first.send_to(frame)
        assert await received_output(first, 1) == [MANIFEST_MESSAGE]

        await second.send_to(frame)
        assert await received_output(second, 1) == rejected("auth message already used")


@pytest.mark.parametrize(
    ("respell", "reason"),
    [
        pytest.param(
            lambda signature: "0x" + signature[2:].upper(), "auth message already used", id="upper"
        ),
        pytest.param(lambda signature: "0x" + signature, "malformed signature", id="0x0x"),
        pytest.param(
            lambda signature: "0x" * 63 + signature, "malformed signature", id="256_characters"
        ),
        pytest.param(
            lambda signature: "0x" * 70 + signature, "malformed signature", id="270_characters"
        ),
    ],
)
async def test_respelled_signature_of_used_auth_frame_is_rejected(
    validator: Validator, respell: Callable[[str], str], reason: str
):
    frame = auth_message()
    respelled = frame.model_copy(update={"signature": respell(frame.signature)})

    async with fake_validator() as first, fake_validator() as second:
        await first.send_to(frame.model_dump_json())
        assert await received_output(first, 1) == [MANIFEST_MESSAGE]

        await second.send_to(respelled.model_dump_json())
        assert await received_output(second, 1) == rejected(reason)


async def test_auth_frames_with_same_timestamp_are_both_accepted(validator: Validator):
    timestamp = int(time.time())
    first_frame, second_frame = auth_message(timestamp), auth_message(timestamp)
    assert first_frame.signature != second_frame.signature

    async with fake_validator() as first, fake_validator() as second:
        await first.send_to(first_frame.model_dump_json())
        await second.send_to(second_frame.model_dump_json())
        assert (await received_output(first, 1), await received_output(second, 1)) == (
            [MANIFEST_MESSAGE],
            [MANIFEST_MESSAGE],
        )


@pytest.mark.parametrize(
    ("timestamp_offset", "reason"),
    [
        pytest.param(60, "msg timestamp is in the future", id="future"),
        pytest.param(-60, "msg too old", id="stale"),
    ],
)
async def test_auth_frame_outside_validity_window_is_rejected(
    validator: Validator, timestamp_offset: int, reason: str
):
    frame = auth_message(int(time.time()) + timestamp_offset).model_dump_json()
    assert await authentication_response(frame) == rejected(reason)


async def test_reconnect_greeting_authenticates_each_reopened_socket_once(validator: Validator):
    greeting = organic_client_reconnect_greeting()

    first_frame = greeting()
    assert await authentication_response(first_frame) == [MANIFEST_MESSAGE]
    reconnect_frame = greeting()
    assert await authentication_response(reconnect_frame) == [MANIFEST_MESSAGE]

    assert [
        await authentication_response(replayed) for replayed in (first_frame, reconnect_frame)
    ] == [rejected("auth message already used")] * 2


async def test_local_miner_accepts_auth_frame_addressed_to_the_trusted_miner(
    validator: Validator, settings
):
    settings.IS_LOCAL_MINER = True
    frame = auth_message(to_miner=TRUSTED_MINER_FAKE_KEY).model_dump_json()

    assert await authentication_response(frame) == [MANIFEST_MESSAGE]


async def test_local_miner_rejects_auth_frame_addressed_to_another_miner(
    validator: Validator, settings
):
    settings.IS_LOCAL_MINER = True
    frame = auth_message().model_dump_json()

    assert await authentication_response(frame) == rejected(
        f"wrong miner hotkey ({TRUSTED_MINER_FAKE_KEY}!={miner_hotkey()})"
    )
