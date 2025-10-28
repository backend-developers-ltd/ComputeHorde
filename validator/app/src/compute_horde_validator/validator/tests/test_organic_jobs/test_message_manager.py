import asyncio
import json
from typing import Any

import pytest
from channels.layers import get_channel_layer
from compute_horde.fv_protocol.facilitator_requests import Response
from compute_horde.fv_protocol.validator_requests import JobStatusUpdate, V0Heartbeat
from compute_horde.transport import StubTransport
from pydantic import BaseModel

from compute_horde_validator.validator.models import SystemEvent
from compute_horde_validator.validator.organic_jobs.facilitator_client.constants import (
    CHEATED_JOB_REPORT_CHANNEL,
    HEARTBEAT_CHANNEL,
    JOB_REQUEST_CHANNEL,
    JOB_STATUS_UPDATE_CHANNEL,
)
from compute_horde_validator.validator.organic_jobs.facilitator_client.facilitator_connector import (
    MessageManager,
)
from compute_horde_validator.validator.organic_jobs.facilitator_client.metrics import (
    VALIDATOR_FC_COMPONENT_STATE,
    VALIDATOR_FC_MESSAGE_QUEUE_LENGTH,
    VALIDATOR_FC_MESSAGE_SEND_FAILURES,
    VALIDATOR_FC_MESSAGES_RECEIVED,
    VALIDATOR_FC_MESSAGES_SENT,
)


async def wait_until(predicate, timeout: float = 10.0, interval: float = 0.01) -> None:
    end_time = asyncio.get_event_loop().time() + timeout
    while True:
        if predicate():
            return
        if asyncio.get_event_loop().time() > end_time:
            raise TimeoutError("wait_until timed out")
        await asyncio.sleep(interval)


async def wait_until_async(predicate, timeout: float = 10.0, interval: float = 0.01) -> None:
    end_time = asyncio.get_event_loop().time() + timeout
    while True:
        if await predicate():
            return
        if asyncio.get_event_loop().time() > end_time:
            raise TimeoutError("wait_until timed out")
        await asyncio.sleep(interval)


def counter_value(counter, labels: dict[str, Any]) -> float:
    return counter.labels(**labels)._value.get()  # type: ignore[attr-defined]


def gauge_value(gauge, labels: dict[str, Any] | None = None) -> float:
    child = gauge if labels is None else gauge.labels(**labels)
    return child._value.get()  # type: ignore[attr-defined]


class MockConnectionManager:
    def __init__(self, transport_layer: StubTransport):
        self.transport_layer = transport_layer

    def is_connected_and_authenticated(self) -> bool:
        return True

    async def receive(self) -> str:
        return await self.transport_layer.receive()

    async def send(self, message: str) -> None:
        await self.transport_layer.send(message)


@pytest.mark.asyncio
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
async def test_message_manager_transport_receive_and_forward_local(job_request, cheated_job):
    # Prepare incoming messages over transport layer
    stub = StubTransport(
        "stub",
        messages=[
            Response(status="success").model_dump_json(),
            job_request.model_dump_json(),
            cheated_job.model_dump_json(),
        ],
    )

    message_manager = MessageManager(connection_manager=MockConnectionManager(transport_layer=stub))

    # Baseline counters
    received_before = {
        "Response": counter_value(VALIDATOR_FC_MESSAGES_RECEIVED, {"message_type": "Response"}),
        "V2JobRequest": counter_value(
            VALIDATOR_FC_MESSAGES_RECEIVED, {"message_type": "V2JobRequest"}
        ),
        "V0JobCheated": counter_value(
            VALIDATOR_FC_MESSAGES_RECEIVED, {"message_type": "V0JobCheated"}
        ),
    }

    await message_manager.start()
    await asyncio.sleep(0.1)

    assert gauge_value(VALIDATOR_FC_COMPONENT_STATE, {"component": "MessageManager"}) == 1

    # Verify messages from transport are forwarded to local channels
    layer = get_channel_layer()
    forwarded_job = await asyncio.wait_for(layer.receive(JOB_REQUEST_CHANNEL), timeout=2)
    assert forwarded_job["message_type"] == "V2JobRequest"

    forwarded_cheated = await asyncio.wait_for(layer.receive(CHEATED_JOB_REPORT_CHANNEL), timeout=2)
    assert forwarded_cheated["message_type"] == "V0JobCheated"

    # Metrics for received messages incremented
    assert (
        counter_value(VALIDATOR_FC_MESSAGES_RECEIVED, {"message_type": "Response"})
        == received_before["Response"] + 1
    )
    assert (
        counter_value(VALIDATOR_FC_MESSAGES_RECEIVED, {"message_type": "V2JobRequest"})
        == received_before["V2JobRequest"] + 1
    )
    assert (
        counter_value(VALIDATOR_FC_MESSAGES_RECEIVED, {"message_type": "V0JobCheated"})
        == received_before["V0JobCheated"] + 1
    )

    await message_manager.stop()
    # Component state set to 0 on stop
    assert gauge_value(VALIDATOR_FC_COMPONENT_STATE, {"component": "MessageManager"}) == 0


@pytest.mark.asyncio
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
async def test_message_manager_local_channels_enqueue_and_send():
    stub = StubTransport("stub", messages=[])

    message_manager = MessageManager(connection_manager=MockConnectionManager(transport_layer=stub))

    sent_before_heartbeat = counter_value(
        VALIDATOR_FC_MESSAGES_SENT, {"message_type": "V0Heartbeat", "retries": "0"}
    )
    sent_before_status = counter_value(
        VALIDATOR_FC_MESSAGES_SENT, {"message_type": "JobStatusUpdate", "retries": "0"}
    )

    await message_manager.start()
    # Messages apparently get los if they're sent before the message manager spins up completely
    await asyncio.sleep(0.1)

    # Send local messages on both channels manager listens to
    layer = get_channel_layer()
    to_send = [
        (HEARTBEAT_CHANNEL, V0Heartbeat().model_dump(mode="json")),
        (
            JOB_STATUS_UPDATE_CHANNEL,
            JobStatusUpdate(uuid="job-2", status="accepted").model_dump(mode="json"),
        ),
        (
            JOB_STATUS_UPDATE_CHANNEL,
            JobStatusUpdate(uuid="job-2", status="executor_ready").model_dump(mode="json"),
        ),
        (HEARTBEAT_CHANNEL, V0Heartbeat().model_dump(mode="json")),
        (
            JOB_STATUS_UPDATE_CHANNEL,
            JobStatusUpdate(uuid="job-2", status="completed").model_dump(mode="json"),
        ),
    ]
    for _ch, _msg in to_send:
        await layer.send(_ch, _msg)
        # The waiting is an awkward hack to make sure that the message manager receives messages
        # in this precise order as it reads from both channels simultaneously by design
        await asyncio.sleep(0.1)

    await wait_until(lambda: len(stub.sent_messages) >= len(to_send))

    assert len(stub.sent_messages) == len(to_send)

    # Make sure the sent messages are what was received from the local channels and in the order they were received
    for _got, (_ch, _expected) in zip(stub.sent_messages, to_send):
        assert json.loads(_got) == _expected

    # Metrics updated: messages sent and queue drained
    assert (
        counter_value(VALIDATOR_FC_MESSAGES_SENT, {"message_type": "V0Heartbeat", "retries": "0"})
        == sent_before_heartbeat + 2
    )
    assert (
        counter_value(
            VALIDATOR_FC_MESSAGES_SENT, {"message_type": "JobStatusUpdate", "retries": "0"}
        )
        == sent_before_status + 3
    )

    # Queue length should be 0 after processing
    assert gauge_value(VALIDATOR_FC_MESSAGE_QUEUE_LENGTH) == 0

    await message_manager.stop()


class FlakySendStubTransport(StubTransport):
    """
    Transport that fails to send a configurable number of times before succeeding.
    """

    def __init__(self, name: str, messages: list[str], fail_times: int = 1):
        super().__init__(name, messages)
        self._remaining_failures = fail_times

    async def send(self, message):
        if self._remaining_failures > 0:
            self._remaining_failures -= 1
            raise RuntimeError("simulated send failure")
        await super().send(message)


@pytest.mark.asyncio
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
async def test_message_manager_retries_on_send_failure():
    flaky = FlakySendStubTransport("flaky", messages=[], fail_times=1)
    message_manager = MessageManager(
        connection_manager=MockConnectionManager(transport_layer=flaky)
    )
    message_manager.MSG_RETRY_DELAY = 0.01

    sent_before_retry = counter_value(
        VALIDATOR_FC_MESSAGES_SENT, {"message_type": "V0Heartbeat", "retries": "1"}
    )

    await message_manager._enqueue_message(V0Heartbeat())
    await message_manager.start()

    # First attempt fails, second should succeed
    await wait_until(lambda: len(flaky.sent_messages) >= 1)
    assert json.loads(flaky.sent_messages[0])["message_type"] == "V0Heartbeat"

    # Metrics reflect one retry
    assert (
        counter_value(VALIDATOR_FC_MESSAGES_SENT, {"message_type": "V0Heartbeat", "retries": "1"})
        == sent_before_retry + 1
    )

    # Queue should be empty
    assert gauge_value(VALIDATOR_FC_MESSAGE_QUEUE_LENGTH) == 0

    await message_manager.stop()


@pytest.mark.asyncio
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
async def test_message_manager_too_many_retries_on_send_failure(settings):
    flaky = FlakySendStubTransport("flaky", messages=[], fail_times=3)
    message_manager = MessageManager(
        connection_manager=MockConnectionManager(transport_layer=flaky)
    )
    message_manager.MSG_RETRY_DELAY = 0.01
    message_manager.MAX_MESSAGE_SEND_RETRIES = 2

    failed_before = counter_value(
        VALIDATOR_FC_MESSAGE_SEND_FAILURES, {"message_type": "V0Heartbeat"}
    )

    await message_manager._enqueue_message(V0Heartbeat())
    await message_manager.start()

    async def _acount() -> int:
        return (
            await SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS)
            .filter(
                type=SystemEvent.EventType.FACILITATOR_CLIENT_ERROR,
                subtype=SystemEvent.EventSubType.MESSAGE_SEND_ERROR,
            )
            .acount()
        ) == 1

    await wait_until_async(_acount)

    assert (
        counter_value(VALIDATOR_FC_MESSAGE_SEND_FAILURES, {"message_type": "V0Heartbeat"})
        == failed_before + 1
    )
    assert await _acount()

    await message_manager.stop()


@pytest.mark.asyncio
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
async def test_message_manager_unknown_local_message_type(settings):
    stub = StubTransport("stub", messages=[])

    message_manager = MessageManager(connection_manager=MockConnectionManager(transport_layer=stub))
    message_manager.WAIT_ON_ERROR_INTERVAL = 0.01

    class BogusMessage(BaseModel):
        prop: str = "test"

    await message_manager.start()
    # Messages apparently get los if they're sent before the message manager spins up completely
    await asyncio.sleep(1)

    # Send local messages on both channels manager listens to
    layer = get_channel_layer()
    await layer.send(JOB_STATUS_UPDATE_CHANNEL, BogusMessage().model_dump(mode="json"))
    await asyncio.sleep(0.1)
    await layer.send(JOB_STATUS_UPDATE_CHANNEL, V0Heartbeat().model_dump(mode="json"))
    await asyncio.sleep(0.1)

    async def _acount() -> int:
        return (
            await SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS)
            .filter(
                type=SystemEvent.EventType.FACILITATOR_CLIENT_ERROR,
                subtype=SystemEvent.EventSubType.UNEXPECTED_MESSAGE,
            )
            .acount()
        ) == 2

    await wait_until_async(_acount)
    assert await _acount()

    await message_manager.stop()


@pytest.mark.asyncio
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
async def test_message_manager_unknown_transport_layer_message_type(settings):
    # Prepare incoming messages over transport layer
    class BogusMessage(BaseModel):
        prop: str = "test"

    stub = StubTransport("stub", messages=[BogusMessage().model_dump_json()])

    message_manager = MessageManager(connection_manager=MockConnectionManager(transport_layer=stub))

    await message_manager.start()
    await asyncio.sleep(1)

    async def _acount() -> int:
        return (
            await SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS)
            .filter(
                type=SystemEvent.EventType.FACILITATOR_CLIENT_ERROR,
                subtype=SystemEvent.EventSubType.UNEXPECTED_MESSAGE,
            )
            .acount()
        ) == 1

    await wait_until_async(_acount)
    assert await _acount()

    await message_manager.stop()
