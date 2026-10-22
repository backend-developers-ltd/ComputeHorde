import asyncio
import json
import time
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from typing import Any

import bittensor
from channels.testing import WebsocketCommunicator
from compute_horde.executor_class import DEFAULT_EXECUTOR_CLASS
from compute_horde.miner_client.organic import OrganicMinerClient
from compute_horde.protocol_messages import (
    V0InitialJobRequest,
    V0JobRequest,
    ValidatorAuthForMiner,
)
from compute_horde.receipts.schemas import JobStartedReceiptPayload
from compute_horde.test_wallet import get_test_miner_wallet, get_test_validator_wallet
from compute_horde.transport import WSTransport
from compute_horde.utils import sign_blob
from django.utils import timezone

from compute_horde_miner import asgi

WEBSOCKET_TIMEOUT = 3
# What the miner sends an authenticated validator when it has one executor of the default class.
MANIFEST_MESSAGE = {
    "message_type": "V0ExecutorManifestRequest",
    "manifest": {DEFAULT_EXECUTOR_CLASS: 1},
}


def validator_hotkey() -> str:
    return get_test_validator_wallet().hotkey.ss58_address


def miner_hotkey() -> str:
    return get_test_miner_wallet().hotkey.ss58_address


def validator_keypair() -> bittensor.Keypair:
    return get_test_validator_wallet().hotkey


@asynccontextmanager
async def fake_validator(
    validator_key: str | None = None,
) -> AsyncIterator[WebsocketCommunicator]:
    """
    Validator socket to the miner, opened under the test validator wallet's hotkey unless
    another key is given.
    """
    communicator = WebsocketCommunicator(
        application=asgi.application,
        path=f"v0.1/validator_interface/{validator_key or validator_hotkey()}",
    )
    connected, _ = await communicator.connect(timeout=WEBSOCKET_TIMEOUT)
    assert connected
    try:
        yield communicator
    finally:
        await communicator.disconnect()


def auth_message(
    timestamp: int | None = None, to_miner: str | None = None
) -> ValidatorAuthForMiner:
    """
    Authentication frame of the test validator, signed for real, for the test miner unless
    another miner hotkey is given.
    """
    msg = ValidatorAuthForMiner(
        validator_hotkey=validator_hotkey(),
        miner_hotkey=to_miner or miner_hotkey(),
        timestamp=int(time.time()) if timestamp is None else timestamp,
        signature="",
    )
    msg.signature = sign_blob(validator_keypair(), msg.blob_for_signing())
    return msg


async def authenticate(channel: WebsocketCommunicator, to_miner: str | None = None) -> None:
    await channel.send_to(auth_message(to_miner=to_miner).model_dump_json())


def organic_client_reconnect_greeting() -> Callable[[], str]:
    """
    What the test validator's OrganicMinerClient sends first on every connection to the test
    miner that its transport reopens. Needs a running event loop.
    """
    client = OrganicMinerClient(
        miner_hotkey=miner_hotkey(),
        miner_address="localhost",
        miner_port=8000,
        job_uuid=str(uuid.uuid4()),
        my_keypair=validator_keypair(),
    )
    assert isinstance(client.transport, WSTransport)
    assert client.transport.reconnect_greeting is not None
    return client.transport.reconnect_greeting


def job_started_receipt(
    job_uuid: str,
    signer: bittensor.Keypair | None = None,
    **overrides: Any,
) -> tuple[JobStartedReceiptPayload, str]:
    """Receipt of the test validator for the test miner; overrides replace payload fields."""
    payload = JobStartedReceiptPayload(
        **{
            "job_uuid": job_uuid,
            "miner_hotkey": miner_hotkey(),
            "validator_hotkey": validator_hotkey(),
            "timestamp": timezone.now(),
            "executor_class": DEFAULT_EXECUTOR_CLASS,
            "is_organic": True,
            "ttl": 60,
            **overrides,
        }
    )
    return payload, sign_blob(signer or validator_keypair(), payload.blob_for_signing())


def initial_job_request(
    job_uuid: str, to_miner: str | None = None, **overrides: Any
) -> V0InitialJobRequest:
    """
    Signed by the test validator, with a receipt for the test miner unless another miner hotkey
    is given; overrides replace fields before signing.
    """
    payload, signature = job_started_receipt(job_uuid, miner_hotkey=to_miner or miner_hotkey())
    return V0InitialJobRequest(
        **{
            "job_uuid": job_uuid,
            "executor_class": DEFAULT_EXECUTOR_CLASS,
            "docker_image": "it's teeeeests",
            "timeout_seconds": 60,
            "job_started_receipt_payload": payload,
            "job_started_receipt_signature": signature,
            **overrides,
        }
    ).signed(validator_keypair())


def job_request(job_uuid: str, **overrides: Any) -> V0JobRequest:
    """Signed by the test validator; overrides replace fields before signing."""
    return V0JobRequest(
        **{
            "job_uuid": job_uuid,
            "executor_class": DEFAULT_EXECUTOR_CLASS,
            "docker_image": "it's teeeeests again",
            "docker_run_cmd": [],
            "env": {"SOME_ENV_VAR": "some value"},
            "docker_run_options_preset": "none",
            "volume": {"volume_type": "inline", "contents": "nonsense"},
            "output_upload": {
                "output_upload_type": "zip_and_http_post",
                "url": "http://localhost/output/upload",
                "form_fields": {},
            },
            **overrides,
        }
    ).signed(validator_keypair())


async def received_output(
    communicator: WebsocketCommunicator, expected_count: int = 0, quiet_period: float = 0.5
) -> list[dict[str, Any]]:
    """
    The next expected_count frames the socket receives, waiting up to WEBSOCKET_TIMEOUT for
    each, plus everything else that arrives until nothing has for quiet_period seconds: sent
    frames as parsed JSON, other events (such as websocket.close) as raw ASGI messages.
    """
    outputs: list[dict[str, Any]] = []
    while len(outputs) < expected_count or not await communicator.receive_nothing(
        timeout=quiet_period
    ):
        output = await communicator.receive_output(timeout=WEBSOCKET_TIMEOUT)
        outputs.append(json.loads(output["text"]) if output["type"] == "websocket.send" else output)
    return outputs


async def wait_until(
    condition: Callable[[], Awaitable[bool]], timeout: float = WEBSOCKET_TIMEOUT
) -> None:
    """Polls the condition until it holds; fails when it still does not after the timeout."""
    deadline = time.monotonic() + timeout
    while not await condition():
        assert time.monotonic() < deadline, "condition not met in time"
        await asyncio.sleep(0.05)
