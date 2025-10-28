import asyncio
from unittest.mock import patch

import pytest
from asgiref.sync import sync_to_async
from channels.layers import get_channel_layer
from compute_horde_core.executor_class import ExecutorClass

from compute_horde_validator.validator.models import Miner, MinerBlacklist, OrganicJob, SystemEvent
from compute_horde_validator.validator.organic_jobs.facilitator_client.constants import (
    CHEATED_JOB_REPORT_CHANNEL,
    JOB_REQUEST_CHANNEL,
)
from compute_horde_validator.validator.organic_jobs.facilitator_client.job_request_manager import (
    JobRequestManager,
)


async def wait_until_async(predicate, timeout: float = 10.0, interval: float = 0.01) -> None:
    end_time = asyncio.get_event_loop().time() + timeout
    while True:
        if await predicate():
            return
        if asyncio.get_event_loop().time() > end_time:
            raise TimeoutError("wait_until timed out")
        await asyncio.sleep(interval)


@pytest.mark.asyncio
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
@patch(
    "compute_horde_validator.validator.organic_jobs.facilitator_client.job_request_manager.job_request_task.delay"
)
async def test_job_request_manager_submits_job_request(mock_delay, job_request):
    job_request_manager = JobRequestManager()
    await job_request_manager.start()
    await asyncio.sleep(0.1)

    # Send a job request
    layer = get_channel_layer()
    await layer.send(JOB_REQUEST_CHANNEL, job_request.model_dump(mode="json"))
    await asyncio.sleep(0.1)

    # Assert that job_request_task.delay was called with the correct argument
    mock_delay.assert_called_once()
    call_args = mock_delay.call_args[0]
    assert len(call_args) == 1
    assert call_args[0] == job_request.model_dump_json()

    await job_request_manager.stop()


@pytest.mark.asyncio
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
@patch(
    "compute_horde_validator.validator.organic_jobs.facilitator_client.jobs_task.verify_request_or_fail"
)
@patch(
    "compute_horde_validator.validator.organic_jobs.facilitator_client.jobs_task.slash_collateral_task.delay"
)
async def test_job_request_manager_processes_cheated_job_report(
    mock_verify, mock_slash_collateral_task, settings, cheated_job
):
    # Prepare databasee entries
    miner = await Miner.objects.using(settings.DEFAULT_DB_ALIAS).acreate(
        hotkey="miner_client",
        collateral_wei=1,
    )
    await OrganicJob.objects.using(settings.DEFAULT_DB_ALIAS).acreate(
        job_uuid=cheated_job.job_uuid,
        status=OrganicJob.Status.COMPLETED,
        cheated=False,
        miner=miner,
        miner_address_ip_version=4,
        miner_address="127.0.0.1",
        miner_port=8080,
        executor_class=ExecutorClass.always_on__gpu_24gb,
        job_description="test job",
        block=1,
        on_trusted_miner=False,
        streaming_details=None,
        allowance_reservation_id=None,
        allowance_blocks=None,
        allowance_job_value=0,
    )

    job_request_manager = JobRequestManager()
    await job_request_manager.start()
    await asyncio.sleep(0.1)

    # Send a job request
    layer = get_channel_layer()
    await layer.send(CHEATED_JOB_REPORT_CHANNEL, cheated_job.model_dump(mode="json"))

    # Check that a JOB_CHEATED system event was created
    async def _acount_job_cheated() -> int:
        return (
            await SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS)
            .filter(
                type=SystemEvent.EventType.MINER_ORGANIC_JOB_FAILURE,
                subtype=SystemEvent.EventSubType.JOB_CHEATED,
            )
            .acount()
        ) == 1

    await wait_until_async(_acount_job_cheated)
    assert await _acount_job_cheated()

    # Check that the miner was blacklisted
    async def _acount_miner_blacklisted() -> int:
        return (
            await SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS)
            .filter(
                type=SystemEvent.EventType.MINER_ORGANIC_JOB_INFO,
                subtype=SystemEvent.EventSubType.MINER_BLACKLISTED,
            )
            .acount()
        ) == 1

    await wait_until_async(_acount_miner_blacklisted)
    assert await _acount_miner_blacklisted()

    blacklist_entry = await sync_to_async(list)(
        MinerBlacklist.objects.using(settings.DEFAULT_DB_ALIAS).all()
    )
    assert len(blacklist_entry) == 1

    await job_request_manager.stop()
