import datetime
import uuid

import bittensor_wallet
import pytest
from aiohttp import web
from asgiref.sync import sync_to_async
from compute_horde.receipts.models import JobAcceptedReceipt, JobFinishedReceipt, JobStartedReceipt
from compute_horde.receipts.schemas import (
    JobAcceptedReceiptPayload,
    JobFinishedReceiptPayload,
    JobStartedReceiptPayload,
    Receipt,
)
from compute_horde.utils import sign_blob
from django.utils import timezone

from compute_horde_validator.validator.models import Miner
from compute_horde_validator.validator.models.allowance.internal import Block
from compute_horde_validator.validator.receipts import Receipts



@pytest.mark.django_db
def test_create_job_started_receipt_returns_payload_and_signature(settings):
    receipts = Receipts()

    job_uuid = str(uuid.uuid4())
    miner_hotkey = "miner_hotkey_1"
    validator_hotkey = settings.BITTENSOR_WALLET().get_hotkey().ss58_address
    executor_class = "always_on.gpu-24gb"
    is_organic = True
    ttl = 300

    payload, signature = receipts.create_job_started_receipt(
        job_uuid=job_uuid,
        miner_hotkey=miner_hotkey,
        validator_hotkey=validator_hotkey,
        executor_class=executor_class,
        is_organic=is_organic,
        ttl=ttl,
    )

    assert isinstance(signature, str) and len(signature) > 0
    assert payload.job_uuid == job_uuid
    assert payload.miner_hotkey == miner_hotkey
    assert payload.validator_hotkey == validator_hotkey
    assert payload.executor_class == executor_class
    assert payload.is_organic is is_organic
    assert payload.ttl == ttl
    assert payload.timestamp.tzinfo is datetime.UTC


@pytest.mark.django_db
def test_create_job_finished_receipt_returns_expected_values(settings):
    receipts = Receipts()

    job_uuid = str(uuid.uuid4())
    miner_hotkey = "miner_hotkey_2"
    validator_hotkey = settings.BITTENSOR_WALLET().get_hotkey().ss58_address
    time_started = datetime.datetime.now(datetime.UTC) - datetime.timedelta(seconds=5)
    time_took_us = 1_234_567
    score_str = "0.987"

    finished = receipts.create_job_finished_receipt(
        job_uuid=job_uuid,
        miner_hotkey=miner_hotkey,
        validator_hotkey=validator_hotkey,
        time_started=time_started,
        time_took_us=time_took_us,
        score_str=score_str,
    )

    assert finished.job_uuid == job_uuid
    assert finished.miner_hotkey == miner_hotkey
    assert finished.validator_hotkey == validator_hotkey
    assert finished.time_started == time_started
    assert finished.time_took_us == time_took_us
    assert finished.score_str == score_str
    assert isinstance(finished.validator_signature, str) and len(finished.validator_signature) > 0
    assert (
        isinstance(finished.timestamp, datetime.datetime)
        and finished.timestamp.tzinfo is datetime.UTC
    )


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_get_valid_job_started_receipts_for_miner_filters_correctly(settings):
    miner_hotkey = "miner_hotkey_valid"
    other_miner = "miner_hotkey_other"
    validator_hotkey = settings.BITTENSOR_WALLET().get_hotkey().ss58_address

    base_ts = datetime.datetime.now(datetime.UTC)

    await sync_to_async(JobStartedReceipt.objects.create, thread_sensitive=True)(
        job_uuid=str(uuid.uuid4()),
        miner_hotkey=miner_hotkey,
        validator_hotkey=validator_hotkey,
        validator_signature="sig",
        timestamp=base_ts - datetime.timedelta(seconds=10),
        executor_class="always_on.gpu-24gb",
        is_organic=True,
        ttl=60,
    )

    await sync_to_async(JobStartedReceipt.objects.create, thread_sensitive=True)(
        job_uuid=str(uuid.uuid4()),
        miner_hotkey=miner_hotkey,
        validator_hotkey=validator_hotkey,
        validator_signature="sig",
        timestamp=base_ts - datetime.timedelta(minutes=10),
        executor_class="always_on.gpu-24gb",
        is_organic=False,
        ttl=30,
    )

    await sync_to_async(JobStartedReceipt.objects.create, thread_sensitive=True)(
        job_uuid=str(uuid.uuid4()),
        miner_hotkey=other_miner,
        validator_hotkey=validator_hotkey,
        validator_signature="sig",
        timestamp=base_ts - datetime.timedelta(seconds=5),
        executor_class="always_on.gpu-24gb",
        is_organic=True,
        ttl=60,
    )

    receipts = Receipts()
    results = await receipts.get_valid_job_started_receipts_for_miner(
        miner_hotkey=miner_hotkey, at_time=base_ts
    )

    assert len(results) == 1
    r = results[0]
    assert r.miner_hotkey == miner_hotkey
    assert r.validator_hotkey == validator_hotkey
    assert r.executor_class == "always_on.gpu-24gb"
    assert r.is_organic is True
    assert r.ttl == 60


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_get_job_finished_receipts_for_miner_filters_by_uuid(settings):
    miner_hotkey = "miner_hotkey_finished"
    validator_hotkey = settings.BITTENSOR_WALLET().get_hotkey().ss58_address
    common_ts = timezone.now()

    wanted_uuid = str(uuid.uuid4())
    other_uuid = str(uuid.uuid4())

    await sync_to_async(JobFinishedReceipt.objects.create, thread_sensitive=True)(
        job_uuid=wanted_uuid,
        miner_hotkey=miner_hotkey,
        validator_hotkey=validator_hotkey,
        validator_signature="sig",
        timestamp=common_ts,
        time_started=common_ts - datetime.timedelta(seconds=2),
        time_took_us=42,
        score_str="0.5",
    )

    await sync_to_async(JobFinishedReceipt.objects.create, thread_sensitive=True)(
        job_uuid=other_uuid,
        miner_hotkey=miner_hotkey,
        validator_hotkey=validator_hotkey,
        validator_signature="sig",
        timestamp=common_ts,
        time_started=common_ts - datetime.timedelta(seconds=3),
        time_took_us=43,
        score_str="0.6",
    )

    receipts = Receipts()
    results = await receipts.get_job_finished_receipts_for_miner(miner_hotkey, [wanted_uuid])

    assert len(results) == 1
    r = results[0]
    assert str(r.job_uuid) == wanted_uuid
    assert r.miner_hotkey == miner_hotkey
    assert r.validator_hotkey == validator_hotkey
    assert r.time_took_us == 42
    assert r.score_str == "0.5"


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_get_job_started_receipt_by_uuid_returns_instance_or_none(settings):
    receipts = Receipts()
    job_uuid_present = str(uuid.uuid4())
    job_uuid_missing = str(uuid.uuid4())

    await sync_to_async(JobStartedReceipt.objects.create, thread_sensitive=True)(
        job_uuid=job_uuid_present,
        miner_hotkey="miner_xyz",
        validator_hotkey=settings.BITTENSOR_WALLET().get_hotkey().ss58_address,
        validator_signature="sig",
        timestamp=timezone.now(),
        executor_class="always_on.gpu-24gb",
        is_organic=True,
        ttl=60,
    )

    found = await receipts.get_job_started_receipt_by_uuid(job_uuid_present)
    missing = await receipts.get_job_started_receipt_by_uuid(job_uuid_missing)

    assert found is not None
    assert str(found.job_uuid) == job_uuid_present
    assert missing is None


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_get_completed_job_receipts_for_block_range_returns_only_in_range(settings):
    receipts = Receipts()

    # Setup block timestamps
    start_block = 100
    end_block = 105
    start_ts = datetime.datetime.now(datetime.UTC)
    end_ts = start_ts + datetime.timedelta(minutes=10)

    await sync_to_async(Block.objects.create, thread_sensitive=True)(
        block_number=start_block, creation_timestamp=start_ts
    )
    await sync_to_async(Block.objects.create, thread_sensitive=True)(
        block_number=end_block, creation_timestamp=end_ts
    )

    miner_hotkey = "miner_hotkey_blockrange"
    validator_hotkey = settings.BITTENSOR_WALLET().get_hotkey().ss58_address

    in_uuid = str(uuid.uuid4())
    await sync_to_async(JobFinishedReceipt.objects.create, thread_sensitive=True)(
        job_uuid=in_uuid,
        miner_hotkey=miner_hotkey,
        validator_hotkey=validator_hotkey,
        validator_signature="v_sig",
        miner_signature="m_sig",
        timestamp=start_ts + datetime.timedelta(minutes=5),
        time_started=start_ts + datetime.timedelta(minutes=4),
        time_took_us=1,
        score_str="1.0",
    )

    await sync_to_async(JobFinishedReceipt.objects.create, thread_sensitive=True)(
        job_uuid=str(uuid.uuid4()),
        miner_hotkey=miner_hotkey,
        validator_hotkey=validator_hotkey,
        validator_signature="v_sig",
        miner_signature="m_sig",
        timestamp=start_ts - datetime.timedelta(seconds=1),
        time_started=start_ts - datetime.timedelta(seconds=2),
        time_took_us=2,
        score_str="0.1",
    )

    await sync_to_async(JobFinishedReceipt.objects.create, thread_sensitive=True)(
        job_uuid=str(uuid.uuid4()),
        miner_hotkey=miner_hotkey,
        validator_hotkey=validator_hotkey,
        validator_signature="v_sig",
        miner_signature="m_sig",
        timestamp=end_ts,
        time_started=end_ts - datetime.timedelta(seconds=2),
        time_took_us=3,
        score_str="0.2",
    )

    receipts_list = await receipts.get_completed_job_receipts_for_block_range(
        start_block, end_block
    )

    assert len(receipts_list) == 1
    converted = receipts_list[0]
    assert converted.payload.job_uuid == in_uuid
    assert converted.payload.miner_hotkey == miner_hotkey
    assert converted.payload.validator_hotkey == validator_hotkey
    assert converted.payload.timestamp == start_ts + datetime.timedelta(minutes=5)
