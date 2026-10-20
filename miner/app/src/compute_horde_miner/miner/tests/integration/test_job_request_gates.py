import contextlib
import datetime as dt
import json
import uuid
from collections.abc import AsyncIterator, Callable
from typing import Any
from unittest import mock
from unittest.mock import AsyncMock

import pytest
from channels.testing import WebsocketCommunicator
from compute_horde.executor_class import DEFAULT_EXECUTOR_CLASS
from compute_horde.protocol_consts import TRUSTED_MINER_FAKE_KEY, JobRejectionReason
from compute_horde.protocol_messages import (
    V0DeclineJobRequest,
    V0InitialJobRequest,
    V0JobRequest,
    ValidatorSignedMessage,
    parse_validator_to_miner_message,
)
from compute_horde.receipts.models import JobStartedReceipt
from compute_horde.test_wallet import get_test_misc_wallet
from compute_horde.utils import canonical_json, sign_blob
from compute_horde_core.executor_class import ExecutorClass
from django.utils import timezone
from pytest_mock import MockerFixture

from compute_horde_miner.miner.miner_consumer.validator_interface import MinerValidatorConsumer
from compute_horde_miner.miner.models import AcceptedJob, Validator
from compute_horde_miner.miner.tests.validator import (
    MANIFEST_MESSAGE,
    WEBSOCKET_TIMEOUT,
    authenticate,
    fake_validator,
    initial_job_request,
    job_request,
    job_started_receipt,
    miner_hotkey,
    organic_client_reconnect_greeting,
    received_output,
    validator_keypair,
    wait_until,
)

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.django_db(transaction=True),
    pytest.mark.usefixtures("authentication_on"),
]

OTHER_EXECUTOR_CLASS = next(c for c in ExecutorClass if c != DEFAULT_EXECUTOR_CLASS)
STALE_RECEIPT_AGE = dt.timedelta(minutes=11)
NO_STATE: dict[str, Any] = {
    "accepted_jobs": [],
    "job_started_receipts": [],
    "reserved_executor_classes": [],
    "joined_executor_groups": 0,
}


@pytest.fixture
def executor_manager(mocker: MockerFixture) -> AsyncMock:
    manager = AsyncMock()
    manager.get_manifest.return_value = {DEFAULT_EXECUTOR_CLASS: 1}
    manager.reserve_executor_class.return_value = None
    manager.wait_for_executor_reservation.return_value = None
    manager.get_executor_public_address.return_value = None
    mocker.patch("compute_horde_miner.miner.executor_manager.current.executor_manager", manager)
    return manager


@pytest.fixture
def group_add(mocker: MockerFixture) -> AsyncMock:
    return mocker.patch.object(MinerValidatorConsumer, "group_add", AsyncMock())


@pytest.fixture
def send_job_request(mocker: MockerFixture) -> AsyncMock:
    return mocker.patch.object(MinerValidatorConsumer, "send_job_request", AsyncMock())


@pytest.fixture
def job_uuid() -> str:
    return str(uuid.uuid4())


@pytest.fixture
def local_miner(settings) -> None:
    settings.IS_LOCAL_MINER = True


async def miner_state(executor_manager: AsyncMock, group_add: AsyncMock) -> dict[str, Any]:
    return {
        "accepted_jobs": [
            job
            async for job in AcceptedJob.objects.values("job_uuid", "status", "full_job_details")
        ],
        "job_started_receipts": [
            job_uuid
            async for job_uuid in JobStartedReceipt.objects.values_list("job_uuid", flat=True)
        ],
        "reserved_executor_classes": [
            call.args[1] for call in executor_manager.reserve_executor_class.await_args_list
        ],
        "joined_executor_groups": len(group_add.await_args_list),
    }


def accepted_job_state(job_uuid: str, full_job_details: dict[str, Any] | None) -> dict[str, Any]:
    return {
        "accepted_jobs": [
            {
                "job_uuid": uuid.UUID(job_uuid),
                "status": AcceptedJob.Status.WAITING_FOR_EXECUTOR
                if full_job_details is None
                else AcceptedJob.Status.RUNNING,
                "full_job_details": full_job_details,
            }
        ],
        "job_started_receipts": [uuid.UUID(job_uuid)],
        "reserved_executor_classes": [DEFAULT_EXECUTOR_CLASS],
        "joined_executor_groups": 1,
    }


def accept(job_uuid: str) -> dict[str, Any]:
    return {"message_type": "V0AcceptJobRequest", "job_uuid": job_uuid}


def decline(job_uuid: str, reason: JobRejectionReason, message: str) -> dict[str, Any]:
    return V0DeclineJobRequest(job_uuid=job_uuid, reason=reason, message=message).model_dump(
        mode="json"
    )


async def wait_for_forwarding(send_job_request: AsyncMock) -> None:
    async def forwarded() -> bool:
        return bool(send_job_request.await_args_list)

    await wait_until(forwarded)


@contextlib.asynccontextmanager
async def authenticated_validator(
    to_miner: str | None = None,
) -> AsyncIterator[WebsocketCommunicator]:
    async with fake_validator() as communicator:
        await authenticate(communicator, to_miner)
        assert await communicator.receive_json_from(timeout=WEBSOCKET_TIMEOUT) == MANIFEST_MESSAGE
        yield communicator


@contextlib.asynccontextmanager
async def accepted_job(job_uuid: str) -> AsyncIterator[WebsocketCommunicator]:
    async with authenticated_validator() as communicator:
        await communicator.send_to(initial_job_request(job_uuid).model_dump_json())
        assert await received_output(communicator, 1) == [accept(job_uuid)]
        yield communicator


def signed_content(msg: ValidatorSignedMessage) -> dict[str, Any]:
    return json.loads(msg.model_dump_json(exclude={"validator_signature"}))


def signed_frame(content: dict[str, Any]) -> str:
    """Frame of the JSON object signed the way a validator signs a job request."""
    signature = sign_blob(validator_keypair(), canonical_json(content))
    return json.dumps({**content, "validator_signature": signature})


def reserved_executor_token(executor_manager: AsyncMock) -> str:
    [executor_token] = [
        call.args[0] for call in executor_manager.reserve_executor_class.await_args_list
    ]
    return executor_token


def received(frame: str) -> ValidatorSignedMessage:
    """The message the miner makes of the frame, signed content included."""
    msg = parse_validator_to_miner_message(frame)
    assert isinstance(msg, ValidatorSignedMessage)
    return msg


def receipt_with_bad_signature(job_uuid: str) -> tuple[dict[str, Any], str]:
    return (
        {"job_started_receipt_signature": "0x" + "00" * 64},
        "job started receipt could not be verified",
    )


def receipt_with_malformed_signature(job_uuid: str) -> tuple[dict[str, Any], str]:
    return {
        "job_started_receipt_signature": "gibberish"
    }, "job started receipt could not be verified"


def receipt_signed_by_another_key(job_uuid: str) -> tuple[dict[str, Any], str]:
    payload, signature = job_started_receipt(job_uuid, signer=get_test_misc_wallet().hotkey)
    return (
        {"job_started_receipt_payload": payload, "job_started_receipt_signature": signature},
        "job started receipt could not be verified",
    )


def receipt_for_another_miner(job_uuid: str) -> tuple[dict[str, Any], str]:
    payload, signature = job_started_receipt(
        job_uuid, miner_hotkey=get_test_misc_wallet().hotkey.ss58_address
    )
    return (
        {"job_started_receipt_payload": payload, "job_started_receipt_signature": signature},
        "job started receipt could not be verified",
    )


def receipt_for_another_job(job_uuid: str) -> tuple[dict[str, Any], str]:
    other_job_uuid = str(uuid.uuid4())
    payload, signature = job_started_receipt(other_job_uuid)
    return (
        {"job_started_receipt_payload": payload, "job_started_receipt_signature": signature},
        f"job started receipt is for job {other_job_uuid}",
    )


def receipt_for_another_executor_class(job_uuid: str) -> tuple[dict[str, Any], str]:
    payload, signature = job_started_receipt(job_uuid, executor_class=OTHER_EXECUTOR_CLASS)
    return (
        {"job_started_receipt_payload": payload, "job_started_receipt_signature": signature},
        f"job started receipt is for executor class {OTHER_EXECUTOR_CLASS}",
    )


def receipt_dated_ago(
    job_uuid: str, age: dt.timedelta, to_miner: str | None = None
) -> dict[str, Any]:
    """Initial job request fields carrying a receipt dated the given time before now."""
    payload, signature = job_started_receipt(
        job_uuid, miner_hotkey=to_miner or miner_hotkey(), timestamp=timezone.now() - age
    )
    return {"job_started_receipt_payload": payload, "job_started_receipt_signature": signature}


def receipt_too_old(job_uuid: str) -> tuple[dict[str, Any], str]:
    return receipt_dated_ago(job_uuid, STALE_RECEIPT_AGE), "job started receipt is too old"


def receipt_from_the_future(job_uuid: str) -> tuple[dict[str, Any], str]:
    return (
        receipt_dated_ago(job_uuid, -dt.timedelta(minutes=1)),
        "job started receipt is dated in the future",
    )


@pytest.mark.parametrize(
    "bad_receipt",
    [
        receipt_with_bad_signature,
        receipt_with_malformed_signature,
        receipt_signed_by_another_key,
        receipt_for_another_miner,
        receipt_for_another_job,
        receipt_for_another_executor_class,
        receipt_too_old,
        receipt_from_the_future,
    ],
    ids=lambda f: f.__name__,
)
async def test_initial_job_request_with_bad_receipt_is_declined_without_state(
    validator: Validator,
    job_uuid: str,
    executor_manager: AsyncMock,
    group_add: AsyncMock,
    bad_receipt: Callable[[str], tuple[dict[str, Any], str]],
):
    overrides, expected_message = bad_receipt(job_uuid)

    async with authenticated_validator() as communicator:
        await communicator.send_to(initial_job_request(job_uuid, **overrides).model_dump_json())
        assert await received_output(communicator, 1) == [
            decline(job_uuid, JobRejectionReason.INVALID_RECEIPT, expected_message)
        ]

    assert await miner_state(executor_manager, group_add) == NO_STATE


async def test_initial_job_request_delayed_by_reconnects_is_accepted(
    validator: Validator, job_uuid: str, executor_manager: AsyncMock, group_add: AsyncMock
):
    overrides = receipt_dated_ago(job_uuid, dt.timedelta(minutes=5))

    async with authenticated_validator() as communicator:
        await communicator.send_to(initial_job_request(job_uuid, **overrides).model_dump_json())
        assert await received_output(communicator, 1) == [accept(job_uuid)]

    assert await miner_state(executor_manager, group_add) == accepted_job_state(job_uuid, None)


async def test_repeated_initial_job_request_is_declined_without_new_state(
    validator: Validator, job_uuid: str, executor_manager: AsyncMock, group_add: AsyncMock
):
    frame = initial_job_request(job_uuid).model_dump_json()

    async with authenticated_validator() as communicator:
        await communicator.send_to(frame)
        assert await received_output(communicator, 1) == [accept(job_uuid)]
        await communicator.send_to(frame)
        assert await received_output(communicator, 1) == [
            decline(
                job_uuid, JobRejectionReason.INVALID_RECEIPT, "job started receipt was already used"
            )
        ]

    assert await miner_state(executor_manager, group_add) == accepted_job_state(job_uuid, None)


@pytest.mark.usefixtures("local_miner")
async def test_local_miner_accepts_receipt_for_the_trusted_miner_without_storing_it(
    validator: Validator, job_uuid: str, executor_manager: AsyncMock, group_add: AsyncMock
):
    async with authenticated_validator(TRUSTED_MINER_FAKE_KEY) as communicator:
        await communicator.send_to(
            initial_job_request(job_uuid, TRUSTED_MINER_FAKE_KEY).model_dump_json()
        )
        assert await received_output(communicator, 1) == [accept(job_uuid)]

    assert await miner_state(executor_manager, group_add) == {
        **accepted_job_state(job_uuid, None),
        "job_started_receipts": [],
    }


@pytest.mark.parametrize(
    ("bad_receipt", "expected_message"),
    [
        pytest.param(
            lambda job_uuid: {},
            "job started receipt could not be verified",
            id="receipt_for_another_miner",
        ),
        pytest.param(
            lambda job_uuid: {
                "to_miner": TRUSTED_MINER_FAKE_KEY,
                "job_started_receipt_signature": "0x" + "00" * 64,
            },
            "job started receipt could not be verified",
            id="receipt_with_bad_signature",
        ),
        pytest.param(
            lambda job_uuid: receipt_dated_ago(job_uuid, STALE_RECEIPT_AGE, TRUSTED_MINER_FAKE_KEY),
            "job started receipt is too old",
            id="receipt_too_old",
        ),
    ],
)
@pytest.mark.usefixtures("local_miner")
async def test_local_miner_declines_bad_receipt_without_state(
    validator: Validator,
    job_uuid: str,
    executor_manager: AsyncMock,
    group_add: AsyncMock,
    bad_receipt: Callable[[str], dict[str, Any]],
    expected_message: str,
):
    request = initial_job_request(job_uuid, **bad_receipt(job_uuid))

    async with authenticated_validator(TRUSTED_MINER_FAKE_KEY) as communicator:
        await communicator.send_to(request.model_dump_json())
        assert await received_output(communicator, 1) == [
            decline(job_uuid, JobRejectionReason.INVALID_RECEIPT, expected_message)
        ]

    assert await miner_state(executor_manager, group_add) == NO_STATE


@pytest.mark.usefixtures("local_miner")
async def test_local_miner_declines_repeated_initial_job_request(
    validator: Validator, job_uuid: str, executor_manager: AsyncMock, group_add: AsyncMock
):
    frame = initial_job_request(job_uuid, TRUSTED_MINER_FAKE_KEY).model_dump_json()

    async with authenticated_validator(TRUSTED_MINER_FAKE_KEY) as communicator:
        await communicator.send_to(frame)
        assert await received_output(communicator, 1) == [accept(job_uuid)]
        await communicator.send_to(frame)
        assert await received_output(communicator, 1) == [
            decline(job_uuid, JobRejectionReason.INVALID_RECEIPT, "job was already accepted")
        ]

    assert await miner_state(executor_manager, group_add) == {
        **accepted_job_state(job_uuid, None),
        "job_started_receipts": [],
    }


def unsigned(msg: ValidatorSignedMessage) -> ValidatorSignedMessage:
    return msg.model_copy(update={"validator_signature": None})


def signed_by_another_key(msg: ValidatorSignedMessage) -> ValidatorSignedMessage:
    return msg.signed(get_test_misc_wallet().hotkey)


def changed_after_signing(msg: ValidatorSignedMessage) -> ValidatorSignedMessage:
    return msg.model_copy(update={"docker_image": "something else"})


BAD_SIGNATURES = [
    pytest.param(unsigned, "missing validator signature", id="unsigned"),
    pytest.param(signed_by_another_key, "invalid validator signature", id="signed_by_another_key"),
    pytest.param(changed_after_signing, "invalid validator signature", id="changed_after_signing"),
]


@pytest.mark.parametrize(("tamper", "expected_message"), BAD_SIGNATURES)
async def test_initial_job_request_without_valid_signature_is_declined_without_state(
    validator: Validator,
    job_uuid: str,
    executor_manager: AsyncMock,
    group_add: AsyncMock,
    tamper: Callable[[V0InitialJobRequest], V0InitialJobRequest],
    expected_message: str,
):
    async with authenticated_validator() as communicator:
        await communicator.send_to(tamper(initial_job_request(job_uuid)).model_dump_json())
        assert await received_output(communicator, 1) == [
            decline(job_uuid, JobRejectionReason.INVALID_SIGNATURE, expected_message)
        ]

    assert await miner_state(executor_manager, group_add) == NO_STATE


async def test_signed_initial_job_request_is_accepted(
    validator: Validator, job_uuid: str, executor_manager: AsyncMock, group_add: AsyncMock
):
    async with accepted_job(job_uuid):
        pass

    assert await miner_state(executor_manager, group_add) == accepted_job_state(job_uuid, None)


async def test_unsigned_initial_job_request_is_accepted_when_signatures_are_not_required(
    validator: Validator,
    job_uuid: str,
    executor_manager: AsyncMock,
    group_add: AsyncMock,
    settings,
):
    settings.REQUIRE_SIGNED_JOB_REQUESTS = False

    async with authenticated_validator() as communicator:
        await communicator.send_to(unsigned(initial_job_request(job_uuid)).model_dump_json())
        assert await received_output(communicator, 1) == [accept(job_uuid)]

    assert await miner_state(executor_manager, group_add) == accepted_job_state(job_uuid, None)


async def test_wrongly_signed_initial_job_request_is_declined_even_when_signatures_are_not_required(
    validator: Validator,
    job_uuid: str,
    executor_manager: AsyncMock,
    group_add: AsyncMock,
    settings,
):
    settings.REQUIRE_SIGNED_JOB_REQUESTS = False

    async with authenticated_validator() as communicator:
        await communicator.send_to(
            signed_by_another_key(initial_job_request(job_uuid)).model_dump_json()
        )
        assert await received_output(communicator, 1) == [
            decline(job_uuid, JobRejectionReason.INVALID_SIGNATURE, "invalid validator signature")
        ]

    assert await miner_state(executor_manager, group_add) == NO_STATE


@pytest.mark.parametrize(("tamper", "expected_message"), BAD_SIGNATURES)
async def test_job_request_without_valid_signature_is_not_forwarded(
    validator: Validator,
    job_uuid: str,
    executor_manager: AsyncMock,
    group_add: AsyncMock,
    send_job_request: AsyncMock,
    tamper: Callable[[V0JobRequest], V0JobRequest],
    expected_message: str,
):
    async with accepted_job(job_uuid) as communicator:
        await communicator.send_to(tamper(job_request(job_uuid)).model_dump_json())
        assert await received_output(communicator, 1) == [
            decline(job_uuid, JobRejectionReason.INVALID_SIGNATURE, expected_message)
        ]

    assert send_job_request.await_args_list == []
    assert await miner_state(executor_manager, group_add) == accepted_job_state(job_uuid, None)


async def test_job_request_for_unknown_job_is_not_forwarded(
    validator: Validator,
    job_uuid: str,
    executor_manager: AsyncMock,
    group_add: AsyncMock,
    send_job_request: AsyncMock,
):
    other_job_uuid = str(uuid.uuid4())

    async with accepted_job(job_uuid) as communicator:
        await communicator.send_to(job_request(other_job_uuid).model_dump_json())
        assert await received_output(communicator, 1) == [
            {
                "message_type": "GenericError",
                "details": f"Received JobRequest for unknown job_uuid: {other_job_uuid}",
            }
        ]

    assert send_job_request.await_args_list == []
    assert await miner_state(executor_manager, group_add) == accepted_job_state(job_uuid, None)


async def test_signed_job_request_is_forwarded_unchanged(
    validator: Validator,
    job_uuid: str,
    executor_manager: AsyncMock,
    group_add: AsyncMock,
    send_job_request: AsyncMock,
):
    frame = job_request(job_uuid).model_dump_json()

    async with accepted_job(job_uuid) as communicator:
        await communicator.send_to(frame)
        await wait_for_forwarding(send_job_request)
        assert await received_output(communicator) == []

    assert send_job_request.await_args_list == [
        mock.call(reserved_executor_token(executor_manager), received(frame))
    ]
    assert await miner_state(executor_manager, group_add) == accepted_job_state(
        job_uuid, json.loads(frame)
    )


async def test_job_request_with_a_field_unknown_to_the_sender_is_forwarded(
    validator: Validator,
    job_uuid: str,
    executor_manager: AsyncMock,
    group_add: AsyncMock,
    send_job_request: AsyncMock,
):
    content = signed_content(job_request(job_uuid, env={}))
    del content["env"]
    frame = signed_frame(content)

    async with accepted_job(job_uuid) as communicator:
        await communicator.send_to(frame)
        await wait_for_forwarding(send_job_request)
        assert await received_output(communicator) == []

    assert send_job_request.await_args_list == [
        mock.call(reserved_executor_token(executor_manager), received(frame))
    ]


async def test_initial_job_request_with_a_field_unknown_to_the_miner_is_accepted(
    validator: Validator, job_uuid: str, executor_manager: AsyncMock, group_add: AsyncMock
):
    frame = signed_frame({**signed_content(initial_job_request(job_uuid)), "priority": 7})

    async with authenticated_validator() as communicator:
        await communicator.send_to(frame)
        assert await received_output(communicator, 1) == [accept(job_uuid)]

    assert await miner_state(executor_manager, group_add) == accepted_job_state(job_uuid, None)


async def test_job_request_with_a_duplicate_key_runs_the_signed_value(
    validator: Validator,
    job_uuid: str,
    executor_manager: AsyncMock,
    group_add: AsyncMock,
    send_job_request: AsyncMock,
):
    signed = job_request(job_uuid, docker_image="signed").model_dump_json()
    # pydantic's JSON validation picks the first of these two keys, json.loads the last one.
    frame = signed.replace(
        '"docker_image":"signed"', '"docker_image":"injected","docker_image":"signed"'
    )
    assert frame != signed

    async with accepted_job(job_uuid) as communicator:
        await communicator.send_to(frame)
        await wait_for_forwarding(send_job_request)
        assert await received_output(communicator) == []

    assert send_job_request.await_args_list == [
        mock.call(reserved_executor_token(executor_manager), received(signed))
    ]


@pytest.mark.parametrize(
    ("frame", "error"),
    [
        pytest.param(
            {"text_data": "not json"}, "Expecting value: line 1 column 1 (char 0)", id="text"
        ),
        pytest.param({"bytes_data": b"{}"}, "expected a text frame", id="binary"),
        pytest.param(
            {"text_data": '{"message_type": "GenericError", "details": ' + "1" * 5000 + "}"},
            "Exceeds the limit (4300 digits) for integer string conversion: value has 5000"
            " digits; use sys.set_int_max_str_digits() to increase the limit",
            id="huge_int",
        ),
        pytest.param(
            {"text_data": "[" * 100_000 + "]" * 100_000},
            "JSON is nested too deeply",
            id="deep_nesting",
        ),
    ],
)
async def test_malformed_frame_is_reported(validator: Validator, frame: dict[str, Any], error: str):
    async with authenticated_validator() as communicator:
        await communicator.send_to(**frame)
        assert await received_output(communicator, 1) == [
            {"message_type": "GenericError", "details": f"Malformed message: {error}"}
        ]


async def test_signed_job_request_is_forwarded_on_a_reopened_socket(
    validator: Validator,
    job_uuid: str,
    executor_manager: AsyncMock,
    group_add: AsyncMock,
    send_job_request: AsyncMock,
):
    greeting = organic_client_reconnect_greeting()
    request = job_request(job_uuid)

    async with fake_validator() as dropped:
        await dropped.send_to(greeting())
        await dropped.send_to(initial_job_request(job_uuid).model_dump_json())
        assert await received_output(dropped, 2) == [MANIFEST_MESSAGE, accept(job_uuid)]

    async with fake_validator() as reopened:
        await reopened.send_to(greeting())
        await reopened.send_to(request.model_dump_json())
        await wait_for_forwarding(send_job_request)
        assert await received_output(reopened, 1) == [MANIFEST_MESSAGE]

    assert send_job_request.await_args_list == [
        mock.call(reserved_executor_token(executor_manager), received(request.model_dump_json()))
    ]
    assert await miner_state(executor_manager, group_add) == {
        **accepted_job_state(job_uuid, request.model_dump(mode="json")),
        # The reopened socket subscribes to the job again.
        "joined_executor_groups": 2,
    }
