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
from compute_horde_validator.validator.receipts import receipts
from compute_horde_validator.validator.routing.default import routing
from compute_horde_validator.validator.routing.types import AllMinersBusy
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


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_pick_miner_for_job__skips_busy_miner_based_on_receipts(add_allowance, monkeypatch):
    # Pick the first suitable miner and mark it as busy with as many ongoing jobs
    # as its executor_count for the requested class so it gets skipped.
    executor_seconds = (
        JOB_REQUEST.download_time_limit
        + JOB_REQUEST.execution_time_limit
        + JOB_REQUEST.upload_time_limit
    )
    current_block = await sync_to_async(allowance().get_current_block)()

    suitable_miners = await sync_to_async(allowance().find_miners_with_allowance)(
        allowance_seconds=executor_seconds,
        executor_class=DEFAULT_EXECUTOR_CLASS,
        job_start_block=current_block,
    )
    assert suitable_miners, "Expected at least one suitable miner"

    manifests_map = await sync_to_async(allowance().get_manifests)()

    # Choose a miner that has at least 1 executor for this class
    busy_miner_hotkey = None
    busy_executor_count = 0
    for miner_hotkey, _ in suitable_miners:
        busy_executor_count = manifests_map.get(miner_hotkey, {}).get(DEFAULT_EXECUTOR_CLASS, 0)
        if busy_executor_count > 0:
            busy_miner_hotkey = miner_hotkey
            break

    assert busy_miner_hotkey is not None, "No miner with executors found for the requested class"

    # Simulate ongoing jobs saturating the busy miner via receipts.get_busy_executor_count
    async def _fake_get_busy_executor_count(executor_class, at_time):
        return {busy_miner_hotkey: busy_executor_count}

    monkeypatch.setattr(receipts(), "get_busy_executor_count", _fake_get_busy_executor_count)

    job_route = await routing().pick_miner_for_job_request(JOB_REQUEST)

    # Assert selected miner is not the busy one and reservation was created
    assert job_route.miner.hotkey_ss58 != busy_miner_hotkey
    assert job_route.allowance_blocks is not None
    assert job_route.allowance_reservation_id is not None


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_pick_miner_for_job__miner_becomes_eligible_after_one_finished_receipt(
    add_allowance, monkeypatch
):
    # Pick the first suitable miner and create as many started receipts as its executor
    # count for the requested class, but also create one matching finished receipt so it's eligible again.
    executor_seconds = (
        JOB_REQUEST.download_time_limit
        + JOB_REQUEST.execution_time_limit
        + JOB_REQUEST.upload_time_limit
    )
    current_block = await sync_to_async(allowance().get_current_block)()

    suitable_miners = await sync_to_async(allowance().find_miners_with_allowance)(
        allowance_seconds=executor_seconds,
        executor_class=DEFAULT_EXECUTOR_CLASS,
        job_start_block=current_block,
    )
    assert suitable_miners, "Expected at least one suitable miner"

    manifests_map = await sync_to_async(allowance().get_manifests)()

    target_miner_hotkey = None
    executor_count = 0
    for miner_hotkey, _ in suitable_miners:
        executor_count = manifests_map.get(miner_hotkey, {}).get(DEFAULT_EXECUTOR_CLASS, 0)
        if executor_count > 0:
            target_miner_hotkey = miner_hotkey
            break

    assert target_miner_hotkey is not None, "No miner with executors found for the requested class"

    # Simulate that one slot is free (executor_count - 1 ongoing jobs)
    async def _fake_get_busy_executor_count(executor_class, at_time):
        return {target_miner_hotkey: max(0, executor_count - 1)}

    monkeypatch.setattr(receipts(), "get_busy_executor_count", _fake_get_busy_executor_count)

    job_route = await routing().pick_miner_for_job_request(JOB_REQUEST)

    # Assert selected miner can be the previously targeted one because it has a free executor
    assert job_route.miner.hotkey_ss58 == target_miner_hotkey
    assert job_route.allowance_blocks is not None
    assert job_route.allowance_reservation_id is not None


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_pick_miner_for_job__miner_fully_free_picked(add_allowance, monkeypatch):
    # Pick a suitable miner and mock busy executors to 0 to simulate that
    # expired started receipts do not count towards ongoing jobs.
    executor_seconds = (
        JOB_REQUEST.download_time_limit
        + JOB_REQUEST.execution_time_limit
        + JOB_REQUEST.upload_time_limit
    )
    current_block = await sync_to_async(allowance().get_current_block)()

    suitable_miners = await sync_to_async(allowance().find_miners_with_allowance)(
        allowance_seconds=executor_seconds,
        executor_class=DEFAULT_EXECUTOR_CLASS,
        job_start_block=current_block,
    )
    assert suitable_miners, "Expected at least one suitable miner"

    manifests_map = await sync_to_async(allowance().get_manifests)()

    target_miner_hotkey = None
    for miner_hotkey, _ in suitable_miners:
        executor_count = manifests_map.get(miner_hotkey, {}).get(DEFAULT_EXECUTOR_CLASS, 0)
        if executor_count > 0:
            target_miner_hotkey = miner_hotkey
            break

    assert target_miner_hotkey is not None, "No miner with executors found for the requested class"

    # Mock receipts.get_busy_executor_count to return no ongoing jobs for any miner.
    async def _fake_get_busy_executor_count(executor_class, at_time):
        return {}

    monkeypatch.setattr(receipts(), "get_busy_executor_count", _fake_get_busy_executor_count)

    job_route = await routing().pick_miner_for_job_request(JOB_REQUEST)

    # Assert the miner is eligible because there are no ongoing jobs (expired receipts ignored)
    assert job_route.miner.hotkey_ss58 == target_miner_hotkey
    assert job_route.allowance_blocks is not None
    assert job_route.allowance_reservation_id is not None


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_pick_miner_for_job__miner_fully_busy_not_picked(add_allowance, monkeypatch):
    # Saturate a miner with started receipts and add finished receipts under another miner key.
    executor_seconds = (
        JOB_REQUEST.download_time_limit
        + JOB_REQUEST.execution_time_limit
        + JOB_REQUEST.upload_time_limit
    )
    current_block = await sync_to_async(allowance().get_current_block)()

    suitable_miners = await sync_to_async(allowance().find_miners_with_allowance)(
        allowance_seconds=executor_seconds,
        executor_class=DEFAULT_EXECUTOR_CLASS,
        job_start_block=current_block,
    )
    assert len(suitable_miners) >= 2, "Need at least two suitable miners for this test"

    manifests_map = await sync_to_async(allowance().get_manifests)()

    # Pick two distinct miners with capacity
    miners_with_capacity = []
    for miner_hotkey, _ in suitable_miners:
        count = manifests_map.get(miner_hotkey, {}).get(DEFAULT_EXECUTOR_CLASS, 0)
        if count > 0:
            miners_with_capacity.append((miner_hotkey, count))
        if len(miners_with_capacity) == 2:
            break

    assert len(miners_with_capacity) == 2, "Could not find two miners with executors"
    target_miner_hotkey, executor_count = miners_with_capacity[0]

    # Simulate the target miner is saturated; finishes on other miner must not free it
    async def _fake_get_busy_executor_count(executor_class, at_time):
        return {target_miner_hotkey: executor_count}

    monkeypatch.setattr(receipts(), "get_busy_executor_count", _fake_get_busy_executor_count)

    job_route = await routing().pick_miner_for_job_request(JOB_REQUEST)

    # Assert target miner is still considered busy and skipped
    assert job_route.miner.hotkey_ss58 != target_miner_hotkey
    assert job_route.allowance_blocks is not None
    assert job_route.allowance_reservation_id is not None


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_pick_miner_for_job__all_miners_fully_busy_raises(add_allowance, monkeypatch):
    # Limit suitable miners to two with capacity, then saturate both
    executor_seconds = (
        JOB_REQUEST.download_time_limit
        + JOB_REQUEST.execution_time_limit
        + JOB_REQUEST.upload_time_limit
    )
    current_block = await sync_to_async(allowance().get_current_block)()

    initial_suitable = await sync_to_async(allowance().find_miners_with_allowance)(
        allowance_seconds=executor_seconds,
        executor_class=DEFAULT_EXECUTOR_CLASS,
        job_start_block=current_block,
    )
    manifests_map = await sync_to_async(allowance().get_manifests)()

    miners_with_capacity = []
    for miner_hotkey, _ in initial_suitable:
        count = manifests_map.get(miner_hotkey, {}).get(DEFAULT_EXECUTOR_CLASS, 0)
        if count > 0:
            miners_with_capacity.append((miner_hotkey, count))
        if len(miners_with_capacity) == 2:
            break

    assert len(miners_with_capacity) == 2, "Could not find two miners with executors"

    limited_miners = miners_with_capacity.copy()

    # Patch finder to return only our limited set
    def _limited_find_miners_with_allowance(*, allowance_seconds, executor_class, job_start_block):
        return [(hk, allowance_seconds) for hk, _ in limited_miners]

    monkeypatch.setattr(
        allowance(), "find_miners_with_allowance", _limited_find_miners_with_allowance
    )

    # Simulate both miners are saturated via receipts.get_busy_executor_count
    busy_map = {hk: cnt for hk, cnt in limited_miners}

    async def _fake_get_busy_executor_count(executor_class, at_time):
        return busy_map

    monkeypatch.setattr(receipts(), "get_busy_executor_count", _fake_get_busy_executor_count)

    with pytest.raises(AllMinersBusy):
        await routing().pick_miner_for_job_request(JOB_REQUEST)
