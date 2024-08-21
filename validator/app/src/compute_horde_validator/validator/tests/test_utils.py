import logging
import uuid

import pytest
from compute_horde.miner_client.base import BaseRequest
from compute_horde.mv_protocol.miner_requests import (
    V0ExecutorFailedRequest,
    V0ExecutorReadyRequest,
    V0JobFailedRequest,
    V0JobFinishedRequest,
)

from compute_horde_validator.validator.organic_jobs.miner_client import MinerClient

from .helpers import get_miner_client

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
@pytest.mark.parametrize(
    "msg",
    [
        V0ExecutorReadyRequest(job_uuid=str(uuid.uuid4())),
        V0ExecutorFailedRequest(job_uuid=str(uuid.uuid4())),
        V0ExecutorReadyRequest(job_uuid=str(uuid.uuid4())),
    ],
)
async def test_miner_client__handle_message__set_ready_or_declining_future(msg: BaseRequest):
    miner_client = get_miner_client(MinerClient, msg.job_uuid)
    assert not miner_client.miner_ready_or_declining_future.done()
    await miner_client.handle_message(msg)
    assert await miner_client.miner_ready_or_declining_future == msg


@pytest.mark.asyncio
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
@pytest.mark.parametrize(
    "msg",
    [
        V0JobFailedRequest(
            job_uuid=str(uuid.uuid4()),
            docker_process_exit_status=1,
            docker_process_stdout="stdout",
            docker_process_stderr="stderr",
        ),
        V0JobFinishedRequest(
            job_uuid=str(uuid.uuid4()),
            docker_process_exit_status=1,
            docker_process_stdout="stdout",
            docker_process_stderr="stderr",
        ),
    ],
)
async def test_miner_client__handle_message__set_other_msg(msg: BaseRequest):
    miner_client = get_miner_client(MinerClient, msg.job_uuid)
    assert not miner_client.miner_finished_or_failed_future.done()
    await miner_client.handle_message(msg)
    assert await miner_client.miner_finished_or_failed_future == msg
