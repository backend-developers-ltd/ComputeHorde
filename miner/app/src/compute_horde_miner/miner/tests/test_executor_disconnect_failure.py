from unittest.mock import AsyncMock

import pytest
from compute_horde.protocol_messages import V0JobFailedRequest

from compute_horde_miner.miner.miner_consumer.executor_interface import MinerExecutorConsumer
from compute_horde_miner.miner.models import AcceptedJob


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status",
    [
        AcceptedJob.Status.WAITING_FOR_PAYLOAD,
        AcceptedJob.Status.RUNNING,
        AcceptedJob.Status.WAITING_FOR_EXECUTOR,
    ],
)
async def test_disconnect_sends_failure_notification_for_active_statuses(status):
    """Test that disconnect sends immediate failure notification for all active job statuses."""
    mock_job = AsyncMock(spec=AcceptedJob)
    mock_job.job_uuid = "test-job-uuid"
    mock_job.status = status
    mock_job.error_type = None
    mock_job.error_detail = None

    consumer = MinerExecutorConsumer()
    consumer._maybe_job = mock_job
    consumer.executor_token = "test-executor-token"
    consumer.send_executor_failed = AsyncMock()

    close_code = 1006
    await consumer.disconnect(close_code)

    error_detail = f"Executor disconnected while job was running (code: {close_code})"
    assert mock_job.status == AcceptedJob.Status.FAILED
    assert mock_job.error_type == "EXECUTOR_DISCONNECTED"
    assert mock_job.error_detail == error_detail
    mock_job.asave.assert_called_once()

    consumer.send_executor_failed.assert_called_once()
    call_args = consumer.send_executor_failed.call_args
    assert call_args[0][0] == "test-executor-token"
    failure_msg = call_args[0][1]
    assert isinstance(failure_msg, V0JobFailedRequest)
    assert failure_msg.job_uuid == "test-job-uuid"
    assert failure_msg.error_type == V0JobFailedRequest.ErrorType.TIMEOUT
    assert failure_msg.error_detail == error_detail
    assert failure_msg.docker_process_stdout == ""
    assert failure_msg.docker_process_stderr == ""


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status",
    [
        AcceptedJob.Status.FINISHED,
        AcceptedJob.Status.FAILED,
        AcceptedJob.Status.REJECTED,
    ],
)
async def test_disconnect_ignores_finished_jobs(status):
    """Test that disconnect doesn't send notifications for already finished/failed jobs."""

    mock_job = AsyncMock(spec=AcceptedJob)
    mock_job.job_uuid = "test-job-uuid"
    mock_job.status = status

    consumer = MinerExecutorConsumer()
    consumer._maybe_job = mock_job
    consumer.executor_token = "test-executor-token"

    consumer.send_executor_failed = AsyncMock()

    await consumer.disconnect(1006)

    consumer.send_executor_failed.assert_not_called()

    assert mock_job.status == status
    mock_job.asave.assert_not_called()
