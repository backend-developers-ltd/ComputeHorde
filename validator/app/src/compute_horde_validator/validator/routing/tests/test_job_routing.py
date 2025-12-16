import re
import uuid
from datetime import timedelta
from decimal import Decimal

import pytest
from compute_horde.executor_class import DEFAULT_EXECUTOR_CLASS
from compute_horde.fv_protocol.facilitator_requests import V2JobRequest
from compute_horde.receipts.models import JobFinishedReceipt, JobStartedReceipt
from django.utils import timezone

from compute_horde_validator.validator.allowance.default import allowance
from compute_horde_validator.validator.allowance.tests.mockchain import set_block_number
from compute_horde_validator.validator.allowance.types import NotEnoughAllowanceException
from compute_horde_validator.validator.allowance.utils import blocks, manifests
from compute_horde_validator.validator.allowance.utils.supertensor import supertensor
from compute_horde_validator.validator.models import Miner as MinerModel
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
    download_time_limit=1,
    execution_time_limit=1,
    streaming_start_time_limit=1,
    upload_time_limit=1,
)

# Ensure collateral threshold defaults to 0 for these tests unless explicitly overridden
pytestmark = [
    pytest.mark.override_config(DYNAMIC_MINIMUM_COLLATERAL_AMOUNT_WEI=0),
    pytest.mark.usefixtures("disable_miner_shuffling"),
]


def clone_job(**updates) -> V2JobRequest:
    """Helper to create modified copies of JOB_REQUEST (pydantic v2)."""
    return JOB_REQUEST.model_copy(update=updates)


@pytest.fixture(autouse=True)
def mock_block_number():
    with set_block_number(1005):
        yield


@pytest.fixture()
def add_allowance():
    with set_block_number(1000):
        manifests.sync_manifests()
    for block_number in range(1001, 1004):
        with set_block_number(block_number):
            blocks.process_block_allowance_with_reporting(block_number, supertensor_=supertensor())


@pytest.mark.django_db(transaction=True)
def test_pick_miner_for_job__picks_a_miner_and_spend_allowance(add_allowance):
    job_route = routing().pick_miner_for_job_request(JOB_REQUEST)
    assert job_route.allowance_blocks == [1001]
    assert job_route.allowance_reservation_id is not None
    allowance().spend_allowance(job_route.allowance_reservation_id)


@pytest.mark.django_db(transaction=True)
def test_pick_miner_for_job__picks_a_miner_and_undo_allowance(add_allowance):
    job_route = routing().pick_miner_for_job_request(JOB_REQUEST)
    assert job_route.allowance_blocks == [1001]
    assert job_route.allowance_reservation_id is not None
    allowance().undo_allowance_reservation(job_route.allowance_reservation_id)


@pytest.mark.django_db(transaction=True)
def test_pick_miner_for_job__trusted_miner(add_allowance):
    job_request = clone_job(on_trusted_miner=True)
    job_route = routing().pick_miner_for_job_request(job_request)
    assert job_route.miner.hotkey_ss58 == TRUSTED_MINER_FAKE_KEY
    assert job_route.allowance_blocks is None
    assert job_route.allowance_reservation_id is None


@pytest.mark.django_db(transaction=True)
def test_pick_miner_for_job__no_matching_executor_class(add_allowance):
    with pytest.raises(NotEnoughAllowanceException):
        routing().pick_miner_for_job_request(clone_job(executor_class="non.existent.class"))


@pytest.mark.django_db(transaction=True)
def test_pick_miner_for_job__no_allowance():
    with pytest.raises(NotEnoughAllowanceException):
        routing().pick_miner_for_job_request(JOB_REQUEST)


@pytest.mark.django_db(transaction=True)
def test_pick_miner_for_job__no_miner_with_enough_allowance(add_allowance):
    with pytest.raises(NotEnoughAllowanceException):
        job_request = clone_job(execution_time_limit=101)
        routing().pick_miner_for_job_request(job_request)


@pytest.mark.django_db(transaction=True)
def test_pick_miner_for_two_jobs(add_allowance):
    job_route1 = routing().pick_miner_for_job_request(JOB_REQUEST)
    assert job_route1.allowance_blocks == [1001]
    assert job_route1.allowance_reservation_id is not None

    job_route2 = routing().pick_miner_for_job_request(JOB_REQUEST)
    assert job_route2.allowance_blocks == [1001]
    assert job_route2.allowance_reservation_id is not None
    assert job_route2.allowance_reservation_id != job_route1.allowance_reservation_id


@pytest.mark.django_db(transaction=True)
def test_pick_miner_for_two_long_jobs(add_allowance):
    job_request1 = clone_job(uuid=str(uuid.uuid4()), execution_time_limit=19)
    job_route1 = routing().pick_miner_for_job_request(job_request1)
    assert job_route1.allowance_blocks == [1001, 1002]
    assert job_route1.allowance_reservation_id is not None
    allowance().spend_allowance(job_route1.allowance_reservation_id)

    job_request2 = clone_job(uuid=str(uuid.uuid4()), execution_time_limit=19)
    job_route2 = routing().pick_miner_for_job_request(job_request2)
    assert job_route2.allowance_blocks == [1001, 1002]
    assert job_route2.allowance_reservation_id is not None
    assert job_route2.allowance_reservation_id != job_route1.allowance_reservation_id
    assert job_route2.miner.hotkey_ss58 != job_route1.miner.hotkey_ss58
    allowance().spend_allowance(job_route2.allowance_reservation_id)


@pytest.mark.django_db(transaction=True)
def test_pick_miner_for_job__skips_busy_miner_based_on_receipts(add_allowance):
    """If a miner is saturated by started receipts, routing skips it."""

    executor_seconds = (
        JOB_REQUEST.download_time_limit
        + JOB_REQUEST.execution_time_limit
        + JOB_REQUEST.upload_time_limit
    )
    current_block = allowance().get_current_block()
    suitable_miners = allowance().find_miners_with_allowance(
        allowance_seconds=executor_seconds,
        executor_class=DEFAULT_EXECUTOR_CLASS,
        job_start_block=current_block,
    )
    assert suitable_miners, "Expected at least one suitable miner"
    manifests_map = allowance().get_manifests()

    busy_miner_hotkey = None
    busy_executor_count = 0
    for miner_hotkey, _ in suitable_miners:
        busy_executor_count = manifests_map.get(miner_hotkey, {}).get(DEFAULT_EXECUTOR_CLASS, 0)
        if busy_executor_count > 0:
            busy_miner_hotkey = miner_hotkey
            break
    assert busy_miner_hotkey is not None, "No miner with executors found for the requested class"

    now = timezone.now()
    validator_hotkey = allowance().my_ss58_address

    # Saturate miner with started receipts (no finishes)
    for _ in range(busy_executor_count):
        JobStartedReceipt.objects.create(
            job_uuid=uuid.uuid4(),
            miner_hotkey=busy_miner_hotkey,
            validator_hotkey=validator_hotkey,
            validator_signature="sig",
            timestamp=now - timedelta(seconds=1),
            executor_class=str(DEFAULT_EXECUTOR_CLASS),
            is_organic=False,
            ttl=60,
        )

    job_route = routing().pick_miner_for_job_request(JOB_REQUEST)
    assert job_route.miner.hotkey_ss58 != busy_miner_hotkey
    assert job_route.allowance_blocks is not None
    assert job_route.allowance_reservation_id is not None


@pytest.mark.django_db(transaction=True)
def test_pick_miner_for_job__miner_becomes_eligible_after_one_finished_receipt(
    add_allowance,
):
    """Miner with one finished job should have a free slot and be eligible."""

    executor_seconds = (
        JOB_REQUEST.download_time_limit
        + JOB_REQUEST.execution_time_limit
        + JOB_REQUEST.upload_time_limit
    )
    current_block = allowance().get_current_block()
    suitable_miners = allowance().find_miners_with_allowance(
        allowance_seconds=executor_seconds,
        executor_class=DEFAULT_EXECUTOR_CLASS,
        job_start_block=current_block,
    )
    manifests_map = allowance().get_manifests()
    target_miner_hotkey = None
    executor_count = 0
    for miner_hotkey, _ in suitable_miners:
        executor_count = manifests_map.get(miner_hotkey, {}).get(DEFAULT_EXECUTOR_CLASS, 0)
        if executor_count > 0:
            target_miner_hotkey = miner_hotkey
            break
    assert target_miner_hotkey is not None

    now = timezone.now()
    validator_hotkey = allowance().my_ss58_address

    started = []
    for _ in range(executor_count):
        started.append(
            JobStartedReceipt.objects.create(
                job_uuid=uuid.uuid4(),
                miner_hotkey=target_miner_hotkey,
                validator_hotkey=validator_hotkey,
                validator_signature="sig",
                timestamp=now - timedelta(seconds=2),
                executor_class=str(DEFAULT_EXECUTOR_CLASS),
                is_organic=False,
                ttl=60,
            )
        )
    finished_job = started[0]
    JobFinishedReceipt.objects.create(
        job_uuid=finished_job.job_uuid,
        miner_hotkey=finished_job.miner_hotkey,
        validator_hotkey=finished_job.validator_hotkey,
        validator_signature="fsig",
        timestamp=now - timedelta(seconds=1),
        time_started=finished_job.timestamp,
        time_took_us=1000,
        score_str="1",
        block_numbers=[],
    )

    job_route = routing().pick_miner_for_job_request(JOB_REQUEST)
    assert job_route.miner.hotkey_ss58 == target_miner_hotkey
    assert job_route.allowance_blocks is not None
    assert job_route.allowance_reservation_id is not None


@pytest.mark.django_db(transaction=True)
def test_pick_miner_for_job__miner_fully_free_picked(add_allowance):
    """No busy receipts -> miner with executors selected."""

    executor_seconds = (
        JOB_REQUEST.download_time_limit
        + JOB_REQUEST.execution_time_limit
        + JOB_REQUEST.upload_time_limit
    )
    current_block = allowance().get_current_block()
    suitable_miners = allowance().find_miners_with_allowance(
        allowance_seconds=executor_seconds,
        executor_class=DEFAULT_EXECUTOR_CLASS,
        job_start_block=current_block,
    )
    manifests_map = allowance().get_manifests()
    target_miner_hotkey = None
    for miner_hotkey, _ in suitable_miners:
        if manifests_map.get(miner_hotkey, {}).get(DEFAULT_EXECUTOR_CLASS, 0) > 0:
            target_miner_hotkey = miner_hotkey
            break
    assert target_miner_hotkey is not None

    job_route = routing().pick_miner_for_job_request(JOB_REQUEST)
    assert job_route.miner.hotkey_ss58 == target_miner_hotkey
    assert job_route.allowance_blocks is not None
    assert job_route.allowance_reservation_id is not None


@pytest.mark.django_db(transaction=True)
def test_pick_miner_for_job__miner_fully_busy_not_picked(add_allowance):
    """Saturate one miner -> another miner selected."""

    executor_seconds = (
        JOB_REQUEST.download_time_limit
        + JOB_REQUEST.execution_time_limit
        + JOB_REQUEST.upload_time_limit
    )
    current_block = allowance().get_current_block()
    suitable_miners = allowance().find_miners_with_allowance(
        allowance_seconds=executor_seconds,
        executor_class=DEFAULT_EXECUTOR_CLASS,
        job_start_block=current_block,
    )
    manifests_map = allowance().get_manifests()
    miners_with_capacity = []
    for miner_hotkey, _ in suitable_miners:
        count = manifests_map.get(miner_hotkey, {}).get(DEFAULT_EXECUTOR_CLASS, 0)
        if count > 0:
            miners_with_capacity.append((miner_hotkey, count))
        if len(miners_with_capacity) == 2:
            break
    assert len(miners_with_capacity) == 2
    target_miner_hotkey, executor_count = miners_with_capacity[0]

    now = timezone.now()
    validator_hotkey = allowance().my_ss58_address

    for _ in range(executor_count):
        JobStartedReceipt.objects.create(
            job_uuid=uuid.uuid4(),
            miner_hotkey=target_miner_hotkey,
            validator_hotkey=validator_hotkey,
            validator_signature="sig",
            timestamp=now - timedelta(seconds=1),
            executor_class=str(DEFAULT_EXECUTOR_CLASS),
            is_organic=False,
            ttl=60,
        )

    job_route = routing().pick_miner_for_job_request(JOB_REQUEST)
    assert job_route.miner.hotkey_ss58 != target_miner_hotkey
    assert job_route.allowance_blocks is not None
    assert job_route.allowance_reservation_id is not None


@pytest.mark.django_db(transaction=True)
def test_pick_miner_for_job__all_miners_fully_busy_raises(add_allowance):
    """Limit suitable miners to two with capacity, then saturate both"""

    executor_seconds = (
        JOB_REQUEST.download_time_limit
        + JOB_REQUEST.execution_time_limit
        + JOB_REQUEST.upload_time_limit
    )
    current_block = allowance().get_current_block()
    initial_suitable = allowance().find_miners_with_allowance(
        allowance_seconds=executor_seconds,
        executor_class=DEFAULT_EXECUTOR_CLASS,
        job_start_block=current_block,
    )
    manifests_map = allowance().get_manifests()
    now = timezone.now()
    validator_hotkey = allowance().my_ss58_address

    for miner_hotkey, _ in initial_suitable:
        count = manifests_map.get(miner_hotkey, {}).get(DEFAULT_EXECUTOR_CLASS, 0)
        if count > 0:
            for _ in range(count):
                JobStartedReceipt.objects.create(
                    job_uuid=uuid.uuid4(),
                    miner_hotkey=miner_hotkey,
                    validator_hotkey=validator_hotkey,
                    validator_signature="sig",
                    timestamp=now - timedelta(seconds=1),
                    executor_class=str(DEFAULT_EXECUTOR_CLASS),
                    is_organic=False,
                    ttl=60,
                )

    with pytest.raises(AllMinersBusy):
        routing().pick_miner_for_job_request(JOB_REQUEST)


@pytest.mark.override_config(DYNAMIC_MINIMUM_COLLATERAL_AMOUNT_WEI=1000)
@pytest.mark.django_db(transaction=True)
def test_pick_miner_for_job__collateral_filter_excludes_all(add_allowance):
    """With a positive collateral threshold and no miners having collateral, all are filtered out.

    We rely on real allowance + manifest state produced by add_allowance. No Miner objects have
    collateral_wei >= threshold (they either don't exist yet or default to 0) so the collateral
    filter yields an empty set and the routing layer raises NotEnoughCollateralException.
    """
    with pytest.raises(NotEnoughCollateralException):
        routing().pick_miner_for_job_request(JOB_REQUEST)


@pytest.mark.override_config(DYNAMIC_MINIMUM_COLLATERAL_AMOUNT_WEI=10)
@pytest.mark.django_db(transaction=True)
def test_pick_miner_for_job__collateral_filter_keeps_subset(add_allowance):
    """Only miners whose recorded collateral meets the threshold should remain.

    We pick the first suitable miner (real allowance) and give it collateral >= threshold.
    Other miners either don't have Miner records yet or keep default collateral (0) so they are
    filtered out. The routed miner must be the one we funded.
    """

    executor_seconds = (
        JOB_REQUEST.download_time_limit
        + JOB_REQUEST.execution_time_limit
        + JOB_REQUEST.upload_time_limit
    )
    current_block = allowance().get_current_block()
    suitable_miners = allowance().find_miners_with_allowance(
        allowance_seconds=executor_seconds,
        executor_class=DEFAULT_EXECUTOR_CLASS,
        job_start_block=current_block,
    )
    assert suitable_miners
    # Use a different hotkey (second suitable miner) to avoid conflicting with any
    # implicit assumptions the routing layer might have had about the first one
    funded_index = 1 if len(suitable_miners) > 1 else 0
    funded_hotkey = suitable_miners[funded_index][0]

    # Ensure Miner exists and has sufficient collateral.
    # Use get_or_create which is atomic and resilient to concurrent creation by routing/other setup.
    # Derive expected address/port from naming convention so that routing's
    # get_or_create(address=..., ip_version=..., port=..., hotkey=...) matches
    # our pre-created record exactly, preventing a duplicate hotkey IntegrityError.
    m = re.search(r"_(\d+)$", funded_hotkey)
    addr_port_num = int(m.group(1)) if m else 0
    expected_address = f"192.168.1.{addr_port_num}" if m else "127.0.0.1"
    expected_port = 8000 + addr_port_num if m else 9000

    miner_obj, created = MinerModel.objects.get_or_create(
        hotkey=funded_hotkey,
        address=expected_address,
        ip_version=4,
        port=expected_port,
        defaults={
            "collateral_wei": Decimal(100),  # >= threshold
        },
    )
    if not created and miner_obj.collateral_wei < 100:  # ensure it meets threshold
        miner_obj.collateral_wei = Decimal(100)
        miner_obj.save()

    job_route = routing().pick_miner_for_job_request(JOB_REQUEST)
    assert job_route.miner.hotkey_ss58 == funded_hotkey
    # Real reservation should have non-empty blocks and reservation id
    assert job_route.allowance_blocks
    assert job_route.allowance_reservation_id is not None
