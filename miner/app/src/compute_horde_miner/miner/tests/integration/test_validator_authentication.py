import asyncio
import contextlib
import time
import uuid
from collections.abc import AsyncIterator
from typing import Any
from unittest import mock

import pytest
import pytest_asyncio
from channels.layers import get_channel_layer
from channels.testing import WebsocketCommunicator
from compute_horde.protocol_messages import V0JobFinishedRequest, V0StreamingJobReadyRequest
from pytest_mock import MockerFixture

from compute_horde_miner import asgi
from compute_horde_miner.miner.miner_consumer.layer_utils import ValidatorInterfaceMixin
from compute_horde_miner.miner.miner_consumer.validator_interface import MinerValidatorConsumer
from compute_horde_miner.miner.models import AcceptedJob, Validator
from compute_horde_miner.miner.tests.validator import (
    MANIFEST_MESSAGE,
    WEBSOCKET_TIMEOUT,
    received_output,
)

pytestmark = [pytest.mark.asyncio, pytest.mark.django_db(transaction=True)]

SHORT_AUTH_TIMEOUT = 1


def _patch_auth_timeout(mocker: MockerFixture, seconds: float) -> float:
    mocker.patch(
        "compute_horde_miner.miner.miner_consumer.validator_interface.AUTH_TIMEOUT", seconds
    )
    return seconds


@pytest.fixture
def generous_auth_timeout(mocker: MockerFixture) -> float:
    return _patch_auth_timeout(mocker, 30)


@pytest.fixture
def short_auth_timeout(mocker: MockerFixture) -> float:
    return _patch_auth_timeout(mocker, SHORT_AUTH_TIMEOUT)


@pytest.fixture
def connect_finished(mocker: MockerFixture) -> asyncio.Event:
    """
    Set once MinerValidatorConsumer.connect() has run to completion; the websocket handshake
    alone finishes before connect() does.
    """
    finished = asyncio.Event()
    original_connect = MinerValidatorConsumer.connect

    async def connect(self: MinerValidatorConsumer) -> None:
        await original_connect(self)
        finished.set()

    mocker.patch.object(MinerValidatorConsumer, "connect", connect)
    return finished


@pytest_asyncio.fixture
async def running_job(validator: Validator) -> AcceptedJob:
    return await AcceptedJob.objects.acreate(
        validator=validator,
        job_uuid=uuid.uuid4(),
        executor_token=f"running-{uuid.uuid4()}",
        status=AcceptedJob.Status.RUNNING,
        initial_job_details={},
    )


@pytest_asyncio.fixture
async def job_awaiting_payload(validator: Validator) -> AcceptedJob:
    return await AcceptedJob.objects.acreate(
        validator=validator,
        job_uuid=uuid.uuid4(),
        executor_token=f"awaiting-{uuid.uuid4()}",
        status=AcceptedJob.Status.WAITING_FOR_PAYLOAD,
        initial_job_details={},
    )


@contextlib.asynccontextmanager
async def validator_socket(validator_key: str) -> AsyncIterator[WebsocketCommunicator]:
    communicator = WebsocketCommunicator(
        asgi.application, f"v0.1/validator_interface/{validator_key}"
    )
    connected, _ = await communicator.connect(timeout=WEBSOCKET_TIMEOUT)
    assert connected
    try:
        yield communicator
    finally:
        await communicator.disconnect()


async def authenticate(communicator: WebsocketCommunicator, validator_key: str) -> None:
    await communicator.send_json_to(
        {
            "message_type": "ValidatorAuthForMiner",
            "validator_hotkey": validator_key,
            "miner_hotkey": "some key",
            "timestamp": int(time.time()),
            "signature": "gibberish",
        }
    )


def executor_messages(
    job: AcceptedJob,
) -> list[tuple[str, V0StreamingJobReadyRequest | V0JobFinishedRequest]]:
    job_uuid = str(job.job_uuid)
    return [
        (
            "streaming_job.ready",
            V0StreamingJobReadyRequest(
                job_uuid=job_uuid,
                executor_token=job.executor_token,
                public_key="streaming server certificate",
                ip="10.0.0.1",
                port=4321,
            ),
        ),
        (
            "executor.finished",
            V0JobFinishedRequest(
                job_uuid=job_uuid,
                docker_process_stdout="customer stdout",
                docker_process_stderr="customer stderr",
                artifacts={"/artifacts/result": "cmVzdWx0"},
                upload_results={},
            ),
        ),
    ]


def relayed_executor_messages(job: AcceptedJob) -> list[dict[str, Any]]:
    streaming_ready, finished = (msg for _, msg in executor_messages(job))
    return [
        {
            **streaming_ready.model_dump(),
            "executor_token": None,
            "miner_signature": mock.ANY,
        },
        finished.model_dump(),
    ]


async def send_executor_messages(job: AcceptedJob) -> None:
    group = ValidatorInterfaceMixin.group_name(job.executor_token)
    for event_type, msg in executor_messages(job):
        await get_channel_layer().group_send(group, {"type": event_type, **msg.model_dump()})


def executor_ready_message(job: AcceptedJob) -> dict[str, Any]:
    return {
        "message_type": "V0ExecutorReadyRequest",
        "job_uuid": str(job.job_uuid),
        "executor_token": None,
    }


async def test_unauthenticated_socket_receives_nothing(
    validator: Validator,
    running_job: AcceptedJob,
    job_awaiting_payload: AcceptedJob,
    generous_auth_timeout: float,
    connect_finished: asyncio.Event,
    mock_executor_manager_class,
):
    async with validator_socket(validator.public_key) as communicator:
        await asyncio.wait_for(connect_finished.wait(), WEBSOCKET_TIMEOUT)
        await send_executor_messages(running_job)
        assert await received_output(communicator) == []


async def test_authenticated_socket_receives_executor_messages(
    validator: Validator,
    running_job: AcceptedJob,
    job_awaiting_payload: AcceptedJob,
    generous_auth_timeout: float,
    mock_executor_manager_class,
):
    async with validator_socket(validator.public_key) as communicator:
        await authenticate(communicator, validator.public_key)
        assert await received_output(communicator, 2) == [
            MANIFEST_MESSAGE,
            executor_ready_message(job_awaiting_payload),
        ]
        await send_executor_messages(running_job)
        assert await received_output(communicator, 2) == relayed_executor_messages(running_job)


async def test_unauthenticated_socket_is_closed_after_auth_timeout(
    validator: Validator,
    short_auth_timeout: float,
    mock_executor_manager_class,
):
    async with validator_socket(validator.public_key) as communicator:
        assert await received_output(communicator, quiet_period=short_auth_timeout * 2) == [
            {
                "message_type": "GenericError",
                "details": (
                    f"Validator {validator.public_key} did not authenticate"
                    f" within {short_auth_timeout}s"
                ),
            },
            {"type": "websocket.close", "code": 1000},
        ]


async def test_authenticated_socket_stays_open_past_auth_timeout(
    validator: Validator,
    short_auth_timeout: float,
    mock_executor_manager_class,
):
    async with validator_socket(validator.public_key) as communicator:
        await authenticate(communicator, validator.public_key)
        assert await received_output(communicator, quiet_period=short_auth_timeout * 2) == [
            MANIFEST_MESSAGE
        ]
