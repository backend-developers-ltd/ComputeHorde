import uuid
from unittest.mock import MagicMock

import pytest
from compute_horde.protocol_messages import (
    V0ExecutorFailedRequest,
    V0ExecutorReadyRequest,
    V0StreamingJobNotReadyRequest,
    V0StreamingJobReadyRequest,
)

from compute_horde_miner.miner.miner_consumer.validator_interface import MinerValidatorConsumer


@pytest.mark.django_db
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "message_class,handler_method,extra_params",
    [
        (V0ExecutorReadyRequest, "_executor_ready", {}),
        (
            V0StreamingJobReadyRequest,
            "_streaming_job_ready",
            {"public_key": "test-key", "port": 8080},
        ),
        (V0StreamingJobNotReadyRequest, "_streaming_job_failed_to_prepare", {}),
        (V0ExecutorFailedRequest, "_executor_failed_to_prepare", {}),
    ],
)
async def test_executor_token_setting(message_class, handler_method, extra_params):
    """Test that methods properly check for missing executor_token."""
    # Create a message with missing executor_token
    message = message_class(job_uuid=str(uuid.uuid4()), executor_token=None, **extra_params)

    # Expect error to be raised
    with pytest.raises(MinerValidatorConsumer.MissingExecutorToken):
        consumer = MinerValidatorConsumer(MagicMock())
        await getattr(consumer, handler_method)(message)
