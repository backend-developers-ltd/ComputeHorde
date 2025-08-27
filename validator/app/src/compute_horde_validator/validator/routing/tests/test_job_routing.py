import uuid

import pytest
import pytest_asyncio
from asgiref.sync import sync_to_async
from compute_horde.executor_class import DEFAULT_EXECUTOR_CLASS
from compute_horde.fv_protocol.facilitator_requests import V2JobRequest

from compute_horde_validator.validator.allowance.default import allowance
from compute_horde_validator.validator.allowance.tests.mockchain import set_block_number
from compute_horde_validator.validator.allowance.types import NotEnoughAllowanceException
from compute_horde_validator.validator.allowance.utils import blocks, manifests
from compute_horde_validator.validator.allowance.utils.supertensor import supertensor
from compute_horde_validator.validator.routing.default import routing
from compute_horde_validator.validator.utils import TRUSTED_MINER_FAKE_KEY

JOB_REQUEST = V2JobRequest(
    uuid=str(uuid.uuid4()),
    executor_class=DEFAULT_EXECUTOR_CLASS,
    docker_image="doesntmatter",
    args=[],
    env={},
    use_gpu=False,
    download_time_limit=1,
    execution_time_limit=1,
    streaming_start_time_limit=1,
    upload_time_limit=1,
)


@pytest_asyncio.fixture(autouse=True)
async def mock_block_number():
    with await sync_to_async(set_block_number)(1005):
        yield


@pytest_asyncio.fixture()
async def add_allowance():
    with await sync_to_async(set_block_number)(1000):
        await sync_to_async(manifests.sync_manifests)()
    for block_number in range(1001, 1004):
        with await sync_to_async(set_block_number)(block_number):
            await sync_to_async(blocks.process_block_allowance_with_reporting)(
                block_number, supertensor_=supertensor()
            )


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_pick_miner_for_job__picks_a_miner_and_spend_allowance(add_allowance):
    job_route = await routing().pick_miner_for_job_request(JOB_REQUEST)
    assert job_route.allowance_blocks == [1001]
    assert job_route.allowance_reservation_id is not None
    await sync_to_async(allowance().spend_allowance)(job_route.allowance_reservation_id)


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_pick_miner_for_job__picks_a_miner_and_undo_allowance(add_allowance):
    job_route = await routing().pick_miner_for_job_request(JOB_REQUEST)
    assert job_route.allowance_blocks == [1001]
    assert job_route.allowance_reservation_id is not None
    await sync_to_async(allowance().undo_allowance_reservation)(job_route.allowance_reservation_id)


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_pick_miner_for_job__trusted_miner(add_allowance):
    job_request = JOB_REQUEST.__replace__(on_trusted_miner=True)
    job_route = await routing().pick_miner_for_job_request(job_request)
    assert job_route.miner.hotkey_ss58 == TRUSTED_MINER_FAKE_KEY
    assert job_route.allowance_blocks is None
    assert job_route.allowance_reservation_id is None


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_pick_miner_for_job__no_matching_executor_class(add_allowance):
    with pytest.raises(NotEnoughAllowanceException):
        await routing().pick_miner_for_job_request(
            JOB_REQUEST.__replace__(executor_class="non.existent.class")
        )


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_pick_miner_for_job__no_allowance():
    with pytest.raises(NotEnoughAllowanceException):
        await routing().pick_miner_for_job_request(JOB_REQUEST)


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_pick_miner_for_job__no_miner_with_enough_allowance(add_allowance):
    with pytest.raises(NotEnoughAllowanceException):
        job_request = JOB_REQUEST.__replace__(execution_time_limit=101)
        await routing().pick_miner_for_job_request(job_request)


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_pick_miner_for_two_jobs(add_allowance):
    job_route1 = await routing().pick_miner_for_job_request(JOB_REQUEST)
    assert job_route1.allowance_blocks == [1001]
    assert job_route1.allowance_reservation_id is not None

    job_route2 = await routing().pick_miner_for_job_request(JOB_REQUEST)
    assert job_route2.allowance_blocks == [1001]
    assert job_route2.allowance_reservation_id is not None
    assert job_route2.allowance_reservation_id != job_route1.allowance_reservation_id


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_pick_miner_for_two_long_jobs(add_allowance):
    job_request1 = JOB_REQUEST.__replace__(uuid=str(uuid.uuid4()), execution_time_limit=19)
    job_route1 = await routing().pick_miner_for_job_request(job_request1)
    assert job_route1.allowance_blocks == [1001, 1002]
    assert job_route1.allowance_reservation_id is not None
    await sync_to_async(allowance().spend_allowance)(job_route1.allowance_reservation_id)

    job_request2 = JOB_REQUEST.__replace__(uuid=str(uuid.uuid4()), execution_time_limit=19)
    job_route2 = await routing().pick_miner_for_job_request(job_request2)
    assert job_route2.allowance_blocks == [1001, 1002]
    assert job_route2.allowance_reservation_id is not None
    assert job_route2.allowance_reservation_id != job_route1.allowance_reservation_id
    assert job_route2.miner.hotkey_ss58 != job_route1.miner.hotkey_ss58
    await sync_to_async(allowance().spend_allowance)(job_route2.allowance_reservation_id)
