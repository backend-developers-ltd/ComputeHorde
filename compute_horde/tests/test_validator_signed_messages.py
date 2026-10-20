import datetime
import json
import uuid

import pytest
from compute_horde_core.executor_class import ExecutorClass
from pydantic import TypeAdapter, create_model

from compute_horde.executor_class import DEFAULT_EXECUTOR_CLASS
from compute_horde.protocol_messages import (
    GenericError,
    V0InitialJobRequest,
    V0JobAcceptedReceiptRequest,
    V0JobFinishedReceiptRequest,
    V0JobRequest,
    V0MainHotkeyMessage,
    ValidatorAuthForMiner,
    ValidatorSignedMessage,
    ValidatorToMinerMessage,
    parse_validator_to_miner_message,
)
from compute_horde.receipts.schemas import JobStartedReceiptPayload
from compute_horde.utils import canonical_json, sign_blob

JOB_UUID = "b4793a02-33a2-4a49-b4e2-4a7b903847e7"
OTHER_EXECUTOR_CLASS = next(c for c in ExecutorClass if c != DEFAULT_EXECUTOR_CLASS)


@pytest.fixture
def initial_job_request(validator_keypair, miner_keypair) -> V0InitialJobRequest:
    receipt = JobStartedReceiptPayload(
        job_uuid=JOB_UUID,
        miner_hotkey=miner_keypair.ss58_address,
        validator_hotkey=validator_keypair.ss58_address,
        timestamp=datetime.datetime(2025, 1, 2, 3, 4, 5, 678901, tzinfo=datetime.UTC),
        executor_class=DEFAULT_EXECUTOR_CLASS,
        is_organic=True,
        ttl=30,
    )
    return V0InitialJobRequest(
        job_uuid=JOB_UUID,
        executor_class=DEFAULT_EXECUTOR_CLASS,
        docker_image="image:v1",
        timeout_seconds=60,
        volume={"volume_type": "inline", "contents": "aW5wdXQ=", "relative_path": "input"},
        job_started_receipt_payload=receipt,
        job_started_receipt_signature=sign_blob(validator_keypair, receipt.blob_for_signing()),
        streaming_details={"public_key": "certificate"},
        executor_timing={
            "allowed_leeway": 1,
            "download_time_limit": 2,
            "execution_time_limit": 3,
            "upload_time_limit": 4,
            "streaming_start_time_limit": 5,
        },
    )


@pytest.fixture
def job_request() -> V0JobRequest:
    return V0JobRequest(
        job_uuid=JOB_UUID,
        executor_class=DEFAULT_EXECUTOR_CLASS,
        docker_image="image:v1",
        raw_script="print(1)",
        docker_run_options_preset="nvidia_all",
        docker_run_cmd=["python", "-c", "print(1)"],
        env={"B": "2", "A": "1"},
        volume={"volume_type": "inline", "contents": "aW5wdXQ="},
        output_upload={
            "output_upload_type": "zip_and_http_post",
            "url": "http://localhost/upload",
            "form_fields": {"key": "value"},
        },
        artifacts_dir="/artifacts",
    )


@pytest.fixture(params=["initial_job_request", "job_request"])
def message(request) -> ValidatorSignedMessage:
    return request.getfixturevalue(request.param)


INITIAL_JOB_REQUEST_CHANGES = {
    "message_type": "V0JobRequest",
    "job_uuid": str(uuid.uuid4()),
    "executor_class": OTHER_EXECUTOR_CLASS,
    "docker_image": "image:v2",
    "timeout_seconds": 61,
    "volume": None,
    "job_started_receipt_payload": JobStartedReceiptPayload(
        job_uuid=JOB_UUID,
        miner_hotkey="miner",
        validator_hotkey="validator",
        timestamp=datetime.datetime(2025, 1, 2, 3, 4, 5, 678901, tzinfo=datetime.UTC),
        executor_class=DEFAULT_EXECUTOR_CLASS,
        is_organic=False,
        ttl=30,
    ),
    "job_started_receipt_signature": "0x" + "00" * 64,
    "streaming_details": None,
    "executor_timing": None,
}
JOB_REQUEST_CHANGES = {
    "message_type": "V0InitialJobRequest",
    "job_uuid": str(uuid.uuid4()),
    "executor_class": OTHER_EXECUTOR_CLASS,
    "docker_image": "image:v2",
    "raw_script": None,
    "docker_run_options_preset": "none",
    "docker_run_cmd": ["python", "-c", "print(2)"],
    "env": {"A": "1", "B": "2", "C": "3"},
    "volume": None,
    "output_upload": None,
    "artifacts_dir": None,
}
FIELD_CHANGES = [
    pytest.param("initial_job_request", field, value, id=f"initial_job_request-{field}")
    for field, value in INITIAL_JOB_REQUEST_CHANGES.items()
] + [
    pytest.param("job_request", field, value, id=f"job_request-{field}")
    for field, value in JOB_REQUEST_CHANGES.items()
]


@pytest.mark.parametrize(
    ("message_class", "changes"),
    [
        (V0InitialJobRequest, INITIAL_JOB_REQUEST_CHANGES),
        (V0JobRequest, JOB_REQUEST_CHANGES),
    ],
)
def test_field_changes_cover_every_signed_field(message_class, changes):
    assert set(changes) == set(message_class.model_fields) - {"validator_signature"}


def test_blob_for_signing_is_canonical_and_excludes_the_signature(message, validator_keypair):
    blob = message.blob_for_signing()
    content = json.loads(blob)

    assert "validator_signature" not in content
    assert blob == json.dumps(content, sort_keys=True, separators=(",", ":"))
    assert message.signed(validator_keypair).blob_for_signing() == blob
    assert type(message).model_validate_json(message.model_dump_json()).blob_for_signing() == blob


def test_signature_covers_the_sent_json_object_without_the_signature(message, validator_keypair):
    sent = json.loads(message.signed(validator_keypair).model_dump_json())
    del sent["validator_signature"]

    assert message.blob_for_signing() == canonical_json(sent)


def test_received_message_verifies_and_keeps_every_field(message, validator_keypair):
    signed = message.signed(validator_keypair)

    received = parse_validator_to_miner_message(signed.model_dump_json())

    assert received.is_signed_by(validator_keypair.ss58_address)
    assert (type(received), received.model_dump()) == (type(signed), signed.model_dump())


def test_received_message_with_a_field_unknown_to_the_receiver_verifies(message, validator_keypair):
    newer_model = create_model(
        f"Newer{type(message).__name__}", __base__=type(message), priority=(int, 0)
    )
    sent = newer_model(**message.model_dump(), priority=7).signed(validator_keypair)
    sent_content = json.loads(sent.model_dump_json())

    received = parse_validator_to_miner_message(json.dumps(sent_content))
    stripped = parse_validator_to_miner_message(
        json.dumps({key: value for key, value in sent_content.items() if key != "priority"})
    )

    assert (type(received), received.is_signed_by(validator_keypair.ss58_address)) == (
        type(message),
        True,
    )
    assert not stripped.is_signed_by(validator_keypair.ss58_address)


@pytest.mark.parametrize(
    ("message_fixture", "field"),
    [("initial_job_request", "executor_timing"), ("job_request", "env")],
)
def test_received_message_without_a_field_unknown_to_the_sender_verifies(
    request, validator_keypair, message_fixture, field
):
    message = request.getfixturevalue(message_fixture)
    sent_content = json.loads(message.model_dump_json(exclude={"validator_signature", field}))
    signature = sign_blob(validator_keypair, canonical_json(sent_content))

    received = parse_validator_to_miner_message(
        json.dumps({**sent_content, "validator_signature": signature})
    )

    assert received.is_signed_by(validator_keypair.ss58_address)
    assert received.model_dump() == {
        **message.model_dump(),
        field: type(message).model_fields[field].get_default(call_default_factory=True),
        "validator_signature": signature,
    }


# Pairs of a signed key-value pair of the job_request fixture and a duplicate of its key.
DUPLICATE_KEYS = [
    ('"docker_image":"image:v1"', '"docker_image":"injected"'),
    ('"url":"http://localhost/upload"', '"url":"http://injected"'),
]


def frames_with_duplicate_keys(wire: str, injected_first: bool) -> list[str]:
    frames = []
    for signed_pair, injected_pair in DUPLICATE_KEYS:
        assert wire.count(signed_pair) == 1
        pairs = (injected_pair, signed_pair) if injected_first else (signed_pair, injected_pair)
        frames.append(wire.replace(signed_pair, ",".join(pairs)))
    return frames


def received_job_request_values(frames: list[str], validator_hotkey: str) -> list[tuple]:
    return [
        (msg.docker_image, msg.output_upload.url, msg.is_signed_by(validator_hotkey))
        for msg in map(parse_validator_to_miner_message, frames)
    ]


def test_value_injected_before_a_signed_key_is_ignored(job_request, validator_keypair):
    wire = job_request.signed(validator_keypair).model_dump_json()

    frames = frames_with_duplicate_keys(wire, injected_first=True)

    assert received_job_request_values(frames, validator_keypair.ss58_address) == [
        ("image:v1", "http://localhost/upload", True),
        ("image:v1", "http://localhost/upload", True),
    ]


def test_value_injected_after_a_signed_key_fails_the_signature(job_request, validator_keypair):
    wire = job_request.signed(validator_keypair).model_dump_json()

    frames = frames_with_duplicate_keys(wire, injected_first=False)

    assert received_job_request_values(frames, validator_keypair.ss58_address) == [
        ("injected", "http://localhost/upload", False),
        ("image:v1", "http://injected", False),
    ]


def test_parsing_matches_pydantic_json_validation(
    initial_job_request, job_request, validator_keypair, miner_keypair, receipts
):
    _, accepted_receipt, finished_receipt = receipts
    messages = [
        GenericError(details="error"),
        ValidatorAuthForMiner(
            validator_hotkey=validator_keypair.ss58_address,
            miner_hotkey=miner_keypair.ss58_address,
            timestamp=1_700_000_000,
            signature="0x00",
        ),
        initial_job_request.signed(validator_keypair),
        job_request.signed(validator_keypair),
        V0JobAcceptedReceiptRequest(
            payload=accepted_receipt.payload, signature=accepted_receipt.validator_signature
        ),
        V0JobFinishedReceiptRequest(
            payload=finished_receipt.payload, signature=finished_receipt.validator_signature
        ),
        V0MainHotkeyMessage(main_hotkey=miner_keypair.ss58_address),
    ]
    wires = [message.model_dump_json() for message in messages]

    assert [
        (type(parsed), parsed.model_dump())
        for parsed in map(parse_validator_to_miner_message, wires)
    ] == [
        (type(parsed), parsed.model_dump())
        for parsed in map(TypeAdapter(ValidatorToMinerMessage).validate_json, wires)
    ]


@pytest.mark.parametrize(
    "frame",
    [
        "not json",
        "[1, 2]",
        '{"message_type": "Unknown"}',
        pytest.param(
            '{"message_type": "GenericError", "details": ' + "1" * 5000 + "}", id="huge_int"
        ),
        pytest.param("[" * 100_000 + "]" * 100_000, id="deep_nesting"),
    ],
)
def test_malformed_frame_raises(frame):
    with pytest.raises(ValueError):
        parse_validator_to_miner_message(frame)


def test_signature_is_bound_to_the_signing_key(message, validator_keypair, miner_keypair):
    assert not message.signed(validator_keypair).is_signed_by(miner_keypair.ss58_address)


@pytest.mark.parametrize("signature", [None, "", "gibberish", "0x1234"])
def test_missing_or_malformed_signature_is_invalid(message, validator_keypair, signature):
    tampered = message.model_copy(update={"validator_signature": signature})

    assert not tampered.is_signed_by(validator_keypair.ss58_address)


@pytest.mark.parametrize(("message_fixture", "field", "value"), FIELD_CHANGES)
def test_signature_is_invalid_after_changing_any_field(
    request, validator_keypair, message_fixture, field, value
):
    signed = request.getfixturevalue(message_fixture).signed(validator_keypair)
    assert signed.is_signed_by(validator_keypair.ss58_address)

    changed = signed.model_copy(update={field: value})

    assert changed != signed
    assert not changed.is_signed_by(validator_keypair.ss58_address)


@pytest.mark.parametrize(
    ("message_fixture", "field", "value"),
    # A frame whose message type changed is not parsed as the same message at all.
    [change for change in FIELD_CHANGES if change.values[1] != "message_type"],
)
def test_received_signature_is_invalid_after_changing_any_field(
    request, validator_keypair, message_fixture, field, value
):
    signed = request.getfixturevalue(message_fixture).signed(validator_keypair)
    frame = signed.model_copy(update={field: value}).model_dump_json()

    assert not parse_validator_to_miner_message(frame).is_signed_by(validator_keypair.ss58_address)
