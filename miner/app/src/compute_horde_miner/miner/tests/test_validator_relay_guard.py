import logging
import uuid
from unittest.mock import AsyncMock, MagicMock, call

import pytest
from compute_horde.protocol_consts import HordeFailureReason, JobParticipantType
from compute_horde.protocol_messages import (
    V0ExecutionDoneRequest,
    V0ExecutorReadyRequest,
    V0HordeFailedRequest,
    V0JobFailedRequest,
    V0JobFinishedRequest,
    V0MachineSpecsRequest,
    V0StreamingJobReadyRequest,
    V0VolumesReadyRequest,
)
from compute_horde.utils import MachineSpecs

from compute_horde_miner.miner.miner_consumer.validator_interface import MinerValidatorConsumer

JOB_UUID = str(uuid.uuid4())

RELAYED_EVENTS = [
    (
        "executor_ready",
        "_executor_ready",
        V0ExecutorReadyRequest(job_uuid=JOB_UUID, executor_token="token"),
    ),
    (
        "streaming_job_ready",
        "_streaming_job_ready",
        V0StreamingJobReadyRequest(
            job_uuid=JOB_UUID, executor_token="token", public_key="key", ip="10.0.0.1", port=1234
        ),
    ),
    (
        "executor_finished",
        "_executor_finished",
        V0JobFinishedRequest(
            job_uuid=JOB_UUID, docker_process_stdout="out", docker_process_stderr="err"
        ),
    ),
    (
        "executor_specs",
        "_executor_specs",
        V0MachineSpecsRequest(job_uuid=JOB_UUID, specs=MachineSpecs(specs={"gpu": "fake"})),
    ),
    ("job_failed", "_job_failed", V0JobFailedRequest(job_uuid=JOB_UUID)),
    (
        "horde_failed",
        "_horde_failed",
        V0HordeFailedRequest(
            job_uuid=JOB_UUID,
            reported_by=JobParticipantType.EXECUTOR,
            message="boom",
            reason=HordeFailureReason.GENERIC_ERROR,
        ),
    ),
    ("volumes_ready", "_volumes_ready", V0VolumesReadyRequest(job_uuid=JOB_UUID)),
    ("execution_done", "_execution_done", V0ExecutionDoneRequest(job_uuid=JOB_UUID)),
]


@pytest.fixture
def consumer() -> MinerValidatorConsumer:
    consumer = MinerValidatorConsumer(MagicMock())
    consumer.send = AsyncMock()
    return consumer


@pytest.fixture(params=RELAYED_EVENTS, ids=[event_type for event_type, _, _ in RELAYED_EVENTS])
def relayed_event(request, consumer: MinerValidatorConsumer):
    event_type, handler_name, message = request.param
    handler = AsyncMock()
    setattr(consumer, handler_name, handler)
    event = {"type": event_type.replace("_", "."), **message.model_dump()}
    return event_type, handler, message, event


@pytest.mark.asyncio
async def test_executor_event_dropped_before_authentication(
    consumer: MinerValidatorConsumer, relayed_event, caplog: pytest.LogCaptureFixture
):
    event_type, handler, _, event = relayed_event
    caplog.set_level(logging.WARNING)

    await getattr(consumer, event_type)(event)

    assert handler.await_args_list == []
    assert consumer.send.await_args_list == []
    assert caplog.record_tuples == [
        (
            "compute_horde_miner.miner.miner_consumer.layer_utils",
            logging.WARNING,
            f"Dropping {event_type} message for job {JOB_UUID}: "
            f"validator socket is not authenticated",
        )
    ]


@pytest.mark.asyncio
async def test_executor_event_relayed_after_authentication(
    consumer: MinerValidatorConsumer, relayed_event
):
    event_type, handler, message, event = relayed_event
    consumer.validator_authenticated = True

    await getattr(consumer, event_type)(event)

    assert handler.await_args_list == [call(message)]
