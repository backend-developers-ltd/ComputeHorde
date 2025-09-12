import uuid

import pytest
import pytest_asyncio
from asgiref.sync import sync_to_async
from compute_horde.executor_class import DEFAULT_EXECUTOR_CLASS
from compute_horde.fv_protocol.facilitator_requests import V2JobRequest

from compute_horde_validator.validator.allowance.default import allowance
from compute_horde_validator.validator.allowance.tests.mockchain import set_block_number
from compute_horde_validator.validator.allowance.types import Miner as AllowanceMiner
from compute_horde_validator.validator.allowance.types import NotEnoughAllowanceException
from compute_horde_validator.validator.allowance.utils import blocks, manifests
from compute_horde_validator.validator.allowance.utils.supertensor import supertensor
from compute_horde_validator.validator.collateral.default import collateral
from compute_horde_validator.validator.receipts import receipts
from compute_horde_validator.validator.routing.default import routing
from compute_horde_validator.validator.routing.types import (
    AllMinersBusy,
    NotEnoughCollateralException,
)
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


# Ensure collateral threshold defaults to 0 for these tests unless explicitly overridden
pytestmark = pytest.mark.override_config(DYNAMIC_MINIMUM_COLLATERAL_AMOUNT_WEI=0)


def clone_job(**updates) -> V2JobRequest:
    """Helper to create modified copies of JOB_REQUEST (pydantic v2)."""
    return JOB_REQUEST.model_copy(update=updates)


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
    job_request = clone_job(on_trusted_miner=True)
    job_route = await routing().pick_miner_for_job_request(job_request)
    assert job_route.miner.hotkey_ss58 == TRUSTED_MINER_FAKE_KEY
    assert job_route.allowance_blocks is None
    assert job_route.allowance_reservation_id is None


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_pick_miner_for_job__no_matching_executor_class(add_allowance):
    with pytest.raises(NotEnoughAllowanceException):
        await routing().pick_miner_for_job_request(clone_job(executor_class="non.existent.class"))


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_pick_miner_for_job__no_allowance():
    with pytest.raises(NotEnoughAllowanceException):
        await routing().pick_miner_for_job_request(JOB_REQUEST)


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_pick_miner_for_job__no_miner_with_enough_allowance(add_allowance):
    with pytest.raises(NotEnoughAllowanceException):
        job_request = clone_job(execution_time_limit=101)
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
    job_request1 = clone_job(uuid=str(uuid.uuid4()), execution_time_limit=19)
    job_route1 = await routing().pick_miner_for_job_request(job_request1)
    assert job_route1.allowance_blocks == [1001, 1002]
    assert job_route1.allowance_reservation_id is not None
    await sync_to_async(allowance().spend_allowance)(job_route1.allowance_reservation_id)

    job_request2 = clone_job(uuid=str(uuid.uuid4()), execution_time_limit=19)
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
    async def _fake_get_busy_executor_count(_executor_class, _at_time):
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
    async def _fake_get_busy_executor_count(_executor_class, _at_time):
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
    async def _fake_get_busy_executor_count(_executor_class, _at_time):
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
    async def _fake_get_busy_executor_count(_executor_class, _at_time):
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

    # Simulate each suitable miner (not just the first two) fully saturated (busy executors == executor_count)
    # so that routing cannot find any free executors and raises AllMinersBusy.
    busy_map = {}
    for miner_hotkey, _allowance_available in initial_suitable:
        count = manifests_map.get(miner_hotkey, {}).get(DEFAULT_EXECUTOR_CLASS, 0)
        if count > 0:
            busy_map[miner_hotkey] = count

    async def _fake_get_busy_executor_count(_executor_class, _at_time):
        return busy_map

    monkeypatch.setattr(receipts(), "get_busy_executor_count", _fake_get_busy_executor_count)

    with pytest.raises(AllMinersBusy):
        await routing().pick_miner_for_job_request(JOB_REQUEST)


@pytest.mark.override_config(DYNAMIC_MINIMUM_COLLATERAL_AMOUNT_WEI=1000)
@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_pick_miner_for_job__collateral_filter_excludes_all(monkeypatch):
    # Fake allowance returning 2 miners
    miners_list = [("miner_low_a", 50.0), ("miner_low_b", 60.0)]

    def fake_find_miners_with_allowance(*, allowance_seconds, executor_class, job_start_block):
        return miners_list

    def fake_miners():
        return [
            AllowanceMiner(address="127.0.0.1", port=8001, ip_version=4, hotkey_ss58="miner_low_a"),
            AllowanceMiner(address="127.0.0.1", port=8002, ip_version=4, hotkey_ss58="miner_low_b"),
        ]

    def fake_get_manifests():
        return {
            "miner_low_a": {JOB_REQUEST.executor_class: 1},
            "miner_low_b": {JOB_REQUEST.executor_class: 1},
        }

    def fake_get_current_block():
        return 12345

    # Collateral module returns empty (none meet threshold)
    def fake_list_miners_with_sufficient_collateral(_min_amount_wei: int):
        return []

    monkeypatch.setattr(allowance(), "find_miners_with_allowance", fake_find_miners_with_allowance)
    monkeypatch.setattr(allowance(), "miners", fake_miners)
    monkeypatch.setattr(allowance(), "get_manifests", fake_get_manifests)
    monkeypatch.setattr(allowance(), "get_current_block", fake_get_current_block)
    monkeypatch.setattr(
        collateral(),
        "list_miners_with_sufficient_collateral",
        fake_list_miners_with_sufficient_collateral,
    )

    with pytest.raises(NotEnoughCollateralException):
        await routing().pick_miner_for_job_request(JOB_REQUEST)


@pytest.mark.override_config(DYNAMIC_MINIMUM_COLLATERAL_AMOUNT_WEI=10)
@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_pick_miner_for_job__collateral_filter_keeps_subset(monkeypatch):
    # Fake allowance returning 2 miners
    miners_list = [("miner_col_ok", 50.0), ("miner_col_low", 60.0)]

    def fake_find_miners_with_allowance(*, allowance_seconds, executor_class, job_start_block):
        return miners_list

    def fake_miners():
        return [
            AllowanceMiner(
                address="127.0.0.1", port=8011, ip_version=4, hotkey_ss58="miner_col_ok"
            ),
            AllowanceMiner(
                address="127.0.0.1", port=8012, ip_version=4, hotkey_ss58="miner_col_low"
            ),
        ]

    def fake_get_manifests():
        return {
            "miner_col_ok": {JOB_REQUEST.executor_class: 1},
            "miner_col_low": {JOB_REQUEST.executor_class: 1},
        }

    def fake_get_current_block():
        return 22222

    def fake_reserve_allowance(*, miner, executor_class, allowance_seconds, job_start_block):
        return 1, [22221]

    class FakeMC:
        def __init__(self, hotkey):
            self.hotkey = hotkey

    # Only miner_col_ok meets collateral threshold
    def fake_list_miners_with_sufficient_collateral(_min_amount_wei: int):
        return [FakeMC("miner_col_ok")]

    async def fake_get_busy_executor_count(_executor_class, _at_time):
        return {}

    monkeypatch.setattr(allowance(), "find_miners_with_allowance", fake_find_miners_with_allowance)
    monkeypatch.setattr(allowance(), "miners", fake_miners)
    monkeypatch.setattr(allowance(), "get_manifests", fake_get_manifests)
    monkeypatch.setattr(allowance(), "get_current_block", fake_get_current_block)
    monkeypatch.setattr(allowance(), "reserve_allowance", fake_reserve_allowance)
    monkeypatch.setattr(
        collateral(),
        "list_miners_with_sufficient_collateral",
        fake_list_miners_with_sufficient_collateral,
    )
    monkeypatch.setattr(receipts(), "get_busy_executor_count", fake_get_busy_executor_count)

    job_route = await routing().pick_miner_for_job_request(JOB_REQUEST)
    assert job_route.miner.hotkey_ss58 == "miner_col_ok"
    assert job_route.allowance_blocks == [22221]
    assert job_route.allowance_reservation_id == 1
