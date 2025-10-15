import datetime
import uuid

import bittensor_wallet
import pytest
from aiohttp import web
from compute_horde.receipts.models import JobAcceptedReceipt, JobFinishedReceipt, JobStartedReceipt
from compute_horde.receipts.schemas import (
    JobAcceptedReceiptPayload,
    JobFinishedReceiptPayload,
    JobStartedReceiptPayload,
    Receipt,
)
from compute_horde.utils import sign_blob
from compute_horde_core.executor_class import ExecutorClass
from django.utils import timezone

from compute_horde_validator.validator.models import Miner
from compute_horde_validator.validator.models.allowance.internal import Block
from compute_horde_validator.validator.receipts import receipts
from compute_horde_validator.validator.tests.helpers import patch_constance


@pytest.mark.asyncio
@pytest.mark.django_db(transaction=True)
@patch_constance({"DYNAMIC_RECEIPT_TRANSFER_ENABLED": True})
async def test_transfer_receipts_from_miners_happy_path(settings):
    settings.RECEIPT_TRANSFER_CHECKPOINT_CACHE = "default"

    miner_kp = bittensor_wallet.Keypair.create_from_mnemonic(
        "almost fatigue race slim picnic mass better clog deal solve already champion"
    )
    miner_hotkey = miner_kp.ss58_address
    validator_kp = settings.BITTENSOR_WALLET().get_hotkey()

    started_payload = JobStartedReceiptPayload(
        job_uuid=str(uuid.uuid4()),
        miner_hotkey=miner_hotkey,
        validator_hotkey=validator_kp.ss58_address,
        timestamp=datetime.datetime.now(datetime.UTC),
        executor_class="always_on.gpu-24gb",
        is_organic=True,
        ttl=300,
    )
    started_blob = started_payload.blob_for_signing()
    started_receipt = Receipt(
        payload=started_payload,
        validator_signature=sign_blob(validator_kp, started_blob),
        miner_signature=sign_blob(miner_kp, started_blob),
    )

    accepted_payload = JobAcceptedReceiptPayload(
        job_uuid=str(uuid.uuid4()),
        miner_hotkey=miner_hotkey,
        validator_hotkey=validator_kp.ss58_address,
        timestamp=datetime.datetime.now(datetime.UTC),
        time_accepted=datetime.datetime.now(datetime.UTC),
        ttl=123,
    )
    accepted_blob = accepted_payload.blob_for_signing()
    accepted_receipt = Receipt(
        payload=accepted_payload,
        validator_signature=sign_blob(validator_kp, accepted_blob),
        miner_signature=sign_blob(miner_kp, accepted_blob),
    )

    finished_payload = JobFinishedReceiptPayload(
        job_uuid=str(uuid.uuid4()),
        miner_hotkey=miner_hotkey,
        validator_hotkey=validator_kp.ss58_address,
        timestamp=datetime.datetime.now(datetime.UTC),
        time_started=datetime.datetime.now(datetime.UTC) - datetime.timedelta(seconds=5),
        time_took_us=42,
        score_str="0.5",
    )
    finished_blob = finished_payload.blob_for_signing()
    finished_receipt = Receipt(
        payload=finished_payload,
        validator_signature=sign_blob(validator_kp, finished_blob),
        miner_signature=sign_blob(miner_kp, finished_blob),
    )

    jsonl_body = (
        started_receipt.model_dump_json()
        + "\n"
        + accepted_receipt.model_dump_json()
        + "\n"
        + finished_receipt.model_dump_json()
        + "\n"
    )

    app = web.Application()
    state = {"body": jsonl_body.encode("utf-8")}

    async def handler(request: web.Request):
        rng = request.headers.get("Range")
        if rng:
            return web.Response(status=416)
        return web.Response(status=200, body=state["body"], content_type="application/jsonl")

    app.router.add_get("/receipts/{page}.jsonl", handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    server = getattr(site, "_server", None)
    assert server is not None and server.sockets, "Server failed to start"
    port = server.sockets[0].getsockname()[1]

    try:
        await Miner.objects.acreate(hotkey=miner_hotkey, address="127.0.0.1", port=port)

        await receipts().run_receipts_transfer(
            daemon=False,
            debug_miner_hotkey=miner_hotkey,
            debug_miner_ip="127.0.0.1",
            debug_miner_port=port,
        )

        stored_started = await JobStartedReceipt.objects.aget(job_uuid=started_payload.job_uuid)
        assert str(stored_started.job_uuid) == started_payload.job_uuid
        assert stored_started.miner_hotkey == started_payload.miner_hotkey
        assert stored_started.executor_class == "always_on.gpu-24gb"
        assert stored_started.is_organic is True
        assert stored_started.ttl == 300

        stored_accepted = await JobAcceptedReceipt.objects.aget(job_uuid=accepted_payload.job_uuid)
        assert str(stored_accepted.job_uuid) == accepted_payload.job_uuid
        assert stored_accepted.miner_hotkey == accepted_payload.miner_hotkey
        assert stored_accepted.ttl == 123

        stored_finished = await JobFinishedReceipt.objects.aget(job_uuid=finished_payload.job_uuid)
        assert str(stored_finished.job_uuid) == finished_payload.job_uuid
        assert stored_finished.miner_hotkey == finished_payload.miner_hotkey
        assert stored_finished.time_took_us == 42
        assert stored_finished.score_str == "0.5"

    finally:
        await runner.cleanup()


@pytest.mark.django_db(transaction=True)
def test_create_job_started_receipt_returns_payload_and_signature(settings):
    job_uuid = str(uuid.uuid4())
    miner_hotkey = "miner_hotkey_1"
    validator_hotkey = settings.BITTENSOR_WALLET().get_hotkey().ss58_address
    executor_class = ExecutorClass.always_on__gpu_24gb
    is_organic = True
    ttl = 300

    payload, signature = receipts().create_job_started_receipt(
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
    assert payload.executor_class == str(executor_class)
    assert payload.is_organic is is_organic
    assert payload.ttl == ttl
    assert payload.timestamp.tzinfo is datetime.UTC


@pytest.mark.django_db(transaction=True)
def test_create_job_finished_receipt_returns_expected_values(settings):
    job_uuid = str(uuid.uuid4())
    miner_hotkey = "miner_hotkey_2"
    validator_hotkey = settings.BITTENSOR_WALLET().get_hotkey().ss58_address
    time_started = datetime.datetime.now(datetime.UTC) - datetime.timedelta(seconds=5)
    time_took_us = 1_234_567
    score_str = "0.987"
    block_numbers = [100, 101, 102]

    finished = receipts().create_job_finished_receipt(
        job_uuid=job_uuid,
        miner_hotkey=miner_hotkey,
        validator_hotkey=validator_hotkey,
        time_started=time_started,
        time_took_us=time_took_us,
        score_str=score_str,
        block_numbers=block_numbers,
    )

    assert finished.job_uuid == job_uuid
    assert finished.miner_hotkey == miner_hotkey
    assert finished.validator_hotkey == validator_hotkey
    assert finished.time_started == time_started
    assert finished.time_took_us == time_took_us
    assert finished.score_str == score_str
    assert finished.block_numbers == block_numbers


@pytest.mark.asyncio
@pytest.mark.django_db(transaction=True)
async def test_get_job_started_receipt_by_uuid(settings):
    job_uuid_present = str(uuid.uuid4())
    job_uuid_missing = str(uuid.uuid4())

    await JobStartedReceipt.objects.acreate(
        job_uuid=job_uuid_present,
        miner_hotkey="miner_xyz",
        validator_hotkey=settings.BITTENSOR_WALLET().get_hotkey().ss58_address,
        validator_signature="sig",
        timestamp=timezone.now(),
        executor_class="always_on.gpu-24gb",
        is_organic=True,
        ttl=60,
    )

    found = await receipts().get_job_started_receipt_by_uuid(job_uuid_present)

    assert found is not None
    assert str(found.job_uuid) == job_uuid_present

    with pytest.raises(JobStartedReceipt.DoesNotExist):
        await receipts().get_job_started_receipt_by_uuid(job_uuid_missing)


@pytest.mark.django_db(transaction=True)
def test_get_finished_jobs_for_block_range_returns_only_in_range(settings):
    # Setup block timestamps
    start_block = 100
    end_block = 105
    start_ts = datetime.datetime.now(datetime.UTC)
    end_ts = start_ts + datetime.timedelta(minutes=10)

    Block.objects.create(block_number=start_block, creation_timestamp=start_ts)
    Block.objects.create(block_number=end_block, creation_timestamp=end_ts)

    miner_hotkey = "miner_hotkey_blockrange"
    validator_hotkey = settings.BITTENSOR_WALLET().get_hotkey().ss58_address

    in_uuid = str(uuid.uuid4())

    # Should be included - job ends within range
    in_range_start_ts = start_ts - datetime.timedelta(seconds=30)
    JobStartedReceipt.objects.create(
        job_uuid=in_uuid,
        miner_hotkey=miner_hotkey,
        validator_hotkey=validator_hotkey,
        validator_signature="sig",
        timestamp=in_range_start_ts,
        executor_class="always_on.gpu-24gb",
        is_organic=True,
        ttl=60,
    )
    in_block_numbers = [start_block, start_block + 1]
    JobFinishedReceipt.objects.create(
        job_uuid=in_uuid,
        miner_hotkey=miner_hotkey,
        validator_hotkey=validator_hotkey,
        validator_signature="v_sig",
        miner_signature="m_sig",
        timestamp=start_ts + datetime.timedelta(minutes=5),
        time_started=start_ts + datetime.timedelta(minutes=4),
        time_took_us=1,
        score_str="1.0",
        block_numbers=in_block_numbers,
    )

    # Should not be included - job ends before the range
    uuid_out_of_range = str(uuid.uuid4())
    JobStartedReceipt.objects.create(
        job_uuid=uuid_out_of_range,
        miner_hotkey=miner_hotkey,
        validator_hotkey=validator_hotkey,
        validator_signature="sig",
        timestamp=start_ts - datetime.timedelta(minutes=2),
        executor_class="always_on.gpu-24gb",
        is_organic=True,
        ttl=60,
    )
    JobFinishedReceipt.objects.create(
        job_uuid=uuid_out_of_range,
        miner_hotkey=miner_hotkey,
        validator_hotkey=validator_hotkey,
        validator_signature="v_sig",
        miner_signature="m_sig",
        timestamp=start_ts - datetime.timedelta(seconds=1),
        time_started=start_ts - datetime.timedelta(seconds=2),
        time_took_us=2,
        score_str="0.1",
    )

    # Should be excluded - job ends exactly at the upper bound
    uuid_end_exclusive = str(uuid.uuid4())
    JobStartedReceipt.objects.create(
        job_uuid=uuid_end_exclusive,
        miner_hotkey=miner_hotkey,
        validator_hotkey=validator_hotkey,
        validator_signature="sig",
        timestamp=end_ts - datetime.timedelta(minutes=1),
        executor_class="always_on.gpu-24gb",
        is_organic=True,
        ttl=60,
    )
    JobFinishedReceipt.objects.create(
        job_uuid=uuid_end_exclusive,
        miner_hotkey=miner_hotkey,
        validator_hotkey=validator_hotkey,
        validator_signature="v_sig",
        miner_signature="m_sig",
        timestamp=end_ts,
        time_started=end_ts - datetime.timedelta(seconds=2),
        time_took_us=3,
        score_str="0.2",
    )

    rows = receipts().get_finished_jobs_for_block_range(
        start_block, end_block, executor_class=ExecutorClass.always_on__gpu_24gb
    )

    assert len(rows) == 1
    item = rows[0]
    assert item.miner_hotkey == miner_hotkey
    assert item.validator_hotkey == validator_hotkey
    assert item.executor_seconds_cost == 1
    assert item.started_at == in_range_start_ts
    assert item.paid_with_blocks == in_block_numbers


@pytest.mark.asyncio
@pytest.mark.django_db(transaction=True)
async def test_get_busy_executor_count_includes_job_accepted_receipts(settings):
    now = timezone.now()
    executor_class = ExecutorClass.always_on__gpu_24gb
    miner_hotkey = "miner_hotkey_busy_tally"
    validator_hotkey = settings.BITTENSOR_WALLET().get_hotkey().ss58_address

    # Job with started receipt still valid.
    await JobStartedReceipt.objects.acreate(
        job_uuid=str(uuid.uuid4()),
        miner_hotkey=miner_hotkey,
        validator_hotkey=validator_hotkey,
        validator_signature="sig",
        timestamp=now - datetime.timedelta(seconds=30),
        executor_class=str(executor_class),
        is_organic=True,
        ttl=120,
    )

    # Job where the started receipt expired but the accepted receipt is still valid.
    accepted_only_uuid = str(uuid.uuid4())
    await JobStartedReceipt.objects.acreate(
        job_uuid=accepted_only_uuid,
        miner_hotkey=miner_hotkey,
        validator_hotkey=validator_hotkey,
        validator_signature="sig",
        timestamp=now - datetime.timedelta(minutes=5),
        executor_class=str(executor_class),
        is_organic=True,
        ttl=60,
    )
    await JobAcceptedReceipt.objects.acreate(
        job_uuid=accepted_only_uuid,
        miner_hotkey=miner_hotkey,
        validator_hotkey=validator_hotkey,
        validator_signature="sig",
        timestamp=now - datetime.timedelta(seconds=20),
        time_accepted=now - datetime.timedelta(seconds=25),
        ttl=180,
    )

    # Job that has both receipts valid should only be counted once.
    both_valid_uuid = str(uuid.uuid4())
    await JobStartedReceipt.objects.acreate(
        job_uuid=both_valid_uuid,
        miner_hotkey=miner_hotkey,
        validator_hotkey=validator_hotkey,
        validator_signature="sig",
        timestamp=now - datetime.timedelta(seconds=40),
        executor_class=str(executor_class),
        is_organic=True,
        ttl=180,
    )
    await JobAcceptedReceipt.objects.acreate(
        job_uuid=both_valid_uuid,
        miner_hotkey=miner_hotkey,
        validator_hotkey=validator_hotkey,
        validator_signature="sig",
        timestamp=now - datetime.timedelta(seconds=10),
        time_accepted=now - datetime.timedelta(seconds=15),
        ttl=180,
    )

    counts = await receipts().get_busy_executor_count(executor_class, now)

    assert counts == {miner_hotkey: 3}


@pytest.mark.django_db(transaction=True)
def test_get_completed_job_receipts_for_block_range_filters_by_executor_class(settings):
    start_block = 300
    end_block = 305
    start_ts = datetime.datetime.now(datetime.UTC)
    end_ts = start_ts + datetime.timedelta(minutes=10)

    Block.objects.create(block_number=start_block, creation_timestamp=start_ts)
    Block.objects.create(block_number=end_block, creation_timestamp=end_ts)

    miner_hotkey = "miner_hotkey_exec_filter"
    validator_hotkey = settings.BITTENSOR_WALLET().get_hotkey().ss58_address

    exec_gpu = ExecutorClass.always_on__gpu_24gb
    exec_llm = ExecutorClass.always_on__llm__a6000

    gpu_uuid = str(uuid.uuid4())
    gpu_start_ts = start_ts - datetime.timedelta(seconds=5)
    JobStartedReceipt.objects.create(
        job_uuid=gpu_uuid,
        miner_hotkey=miner_hotkey,
        validator_hotkey=validator_hotkey,
        validator_signature="sig",
        timestamp=gpu_start_ts,
        executor_class=str(exec_gpu),
        is_organic=True,
        ttl=60,
    )
    gpu_block_numbers = [start_block, start_block + 2]
    JobFinishedReceipt.objects.create(
        job_uuid=gpu_uuid,
        miner_hotkey=miner_hotkey,
        validator_hotkey=validator_hotkey,
        validator_signature="sig",
        timestamp=start_ts + datetime.timedelta(minutes=2),
        time_started=start_ts + datetime.timedelta(minutes=1, seconds=30),
        time_took_us=90_000_000,
        score_str="1337.000",
        block_numbers=gpu_block_numbers,
    )

    llm_uuid = str(uuid.uuid4())
    JobStartedReceipt.objects.create(
        job_uuid=llm_uuid,
        miner_hotkey=miner_hotkey,
        validator_hotkey=validator_hotkey,
        validator_signature="sig",
        timestamp=start_ts - datetime.timedelta(seconds=4),
        executor_class=str(exec_llm),
        is_organic=False,
        ttl=60,
    )
    JobFinishedReceipt.objects.create(
        job_uuid=llm_uuid,
        miner_hotkey=miner_hotkey,
        validator_hotkey=validator_hotkey,
        validator_signature="sig",
        timestamp=start_ts + datetime.timedelta(minutes=3),
        time_started=start_ts + datetime.timedelta(minutes=2, seconds=30),
        time_took_us=120_000_000,
        score_str="0.8",
    )

    no_job_started_uuid = str(uuid.uuid4())
    JobFinishedReceipt.objects.create(
        job_uuid=no_job_started_uuid,
        miner_hotkey=miner_hotkey,
        validator_hotkey=validator_hotkey,
        validator_signature="sig",
        timestamp=start_ts + datetime.timedelta(minutes=4),
        time_started=start_ts + datetime.timedelta(minutes=3, seconds=30),
        time_took_us=30_000_000,
        score_str="0.7",
    )

    rows = receipts().get_finished_jobs_for_block_range(
        start_block, end_block, executor_class=exec_gpu
    )

    assert len(rows) == 1
    item = rows[0]
    assert item.miner_hotkey == miner_hotkey
    assert item.validator_hotkey == validator_hotkey
    assert item.executor_seconds_cost == 1337
    assert item.started_at == gpu_start_ts
    assert item.paid_with_blocks == gpu_block_numbers


@pytest.mark.asyncio
@pytest.mark.django_db(transaction=True)
async def test_get_busy_executor_count_counts_only_valid_and_unfinished(settings):
    start_ts = datetime.datetime.now(datetime.UTC)
    miner_a = "miner_A"
    miner_b = "miner_B"
    validator_hotkey = settings.BITTENSOR_WALLET().get_hotkey().ss58_address
    executor = ExecutorClass.always_on__gpu_24gb

    job_a = str(uuid.uuid4())
    await JobStartedReceipt.objects.acreate(
        job_uuid=job_a,
        miner_hotkey=miner_a,
        validator_hotkey=validator_hotkey,
        validator_signature="sig",
        timestamp=start_ts - datetime.timedelta(seconds=10),
        executor_class=str(executor),
        is_organic=True,
        ttl=60,
    )

    job_a_finished = str(uuid.uuid4())
    await JobStartedReceipt.objects.acreate(
        job_uuid=job_a_finished,
        miner_hotkey=miner_a,
        validator_hotkey=validator_hotkey,
        validator_signature="sig",
        timestamp=start_ts - datetime.timedelta(seconds=20),
        executor_class=str(executor),
        is_organic=True,
        ttl=60,
    )
    await JobFinishedReceipt.objects.acreate(
        job_uuid=job_a_finished,
        miner_hotkey=miner_a,
        validator_hotkey=validator_hotkey,
        validator_signature="sig",
        timestamp=start_ts - datetime.timedelta(seconds=1),
        time_started=start_ts - datetime.timedelta(seconds=30),
        time_took_us=1,
        score_str="0",
    )

    job_b_other_exec = str(uuid.uuid4())
    await JobStartedReceipt.objects.acreate(
        job_uuid=job_b_other_exec,
        miner_hotkey=miner_b,
        validator_hotkey=validator_hotkey,
        validator_signature="sig",
        timestamp=start_ts - datetime.timedelta(seconds=5),
        executor_class="always_on.llm.a6000",
        is_organic=True,
        ttl=60,
    )

    job_b_expired = str(uuid.uuid4())
    await JobStartedReceipt.objects.acreate(
        job_uuid=job_b_expired,
        miner_hotkey=miner_b,
        validator_hotkey=validator_hotkey,
        validator_signature="sig",
        timestamp=start_ts - datetime.timedelta(minutes=2),
        executor_class=executor,
        is_organic=True,
        ttl=30,
    )

    counts = await receipts().get_busy_executor_count(executor_class=executor, at_time=start_ts)
    assert counts == {miner_a: 1}


@pytest.mark.asyncio
@pytest.mark.django_db(transaction=True)
async def test_get_busy_executor_count__busy_miner_based_on_started_receipts():
    executor = ExecutorClass.always_on__gpu_24gb
    miner = "busy_miner"
    validator_hotkey = "validator_hotkey"
    now = datetime.datetime.now(datetime.UTC)

    # Create multiple started receipts for the same miner and executor class
    n_started = 3
    for _ in range(n_started):
        await JobStartedReceipt.objects.acreate(
            job_uuid=str(uuid.uuid4()),
            miner_hotkey=miner,
            validator_hotkey=validator_hotkey,
            validator_signature="sig",
            timestamp=now,
            executor_class=str(executor),
            is_organic=True,
            ttl=3600,
        )

    busy_map = await receipts().get_busy_executor_count(executor_class=executor, at_time=now)
    assert busy_map.get(miner) == n_started


@pytest.mark.asyncio
@pytest.mark.django_db(transaction=True)
async def test_get_busy_executor_count__miner_becomes_less_busy_after_finished():
    executor = ExecutorClass.always_on__gpu_24gb
    miner = "target_miner"
    validator_hotkey = "validator_hotkey"
    now = datetime.datetime.now(datetime.UTC)

    # Create started receipts and keep their job_uuids
    job_uuids: list[str] = []
    for _ in range(4):
        job_uuid = str(uuid.uuid4())
        job_uuids.append(job_uuid)
        await JobStartedReceipt.objects.acreate(
            job_uuid=job_uuid,
            miner_hotkey=miner,
            validator_hotkey=validator_hotkey,
            validator_signature="sig",
            timestamp=now,
            executor_class=str(executor),
            is_organic=True,
            ttl=3600,
        )

    # Finish one of them (matching miner)
    finished_uuid = job_uuids[0]
    await JobFinishedReceipt.objects.acreate(
        job_uuid=finished_uuid,
        miner_hotkey=miner,
        validator_hotkey=validator_hotkey,
        validator_signature="sig",
        timestamp=now,
        time_started=now,
        time_took_us=1000,
        score_str="1.0",
    )

    busy_map = await receipts().get_busy_executor_count(executor_class=executor, at_time=now)
    assert busy_map.get(miner) == len(job_uuids) - 1


@pytest.mark.asyncio
@pytest.mark.django_db(transaction=True)
async def test_get_busy_executor_count__expired_started_receipts_do_not_count():
    executor = ExecutorClass.always_on__gpu_24gb
    miner = "miner_with_expired"
    validator_hotkey = "validator_hotkey"
    now = datetime.datetime.now(datetime.UTC)

    # Insert expired started receipts: timestamp far in the past, small ttl so not valid_at(now)
    past_time = now - datetime.timedelta(seconds=3600)
    for _ in range(3):
        await JobStartedReceipt.objects.acreate(
            job_uuid=str(uuid.uuid4()),
            miner_hotkey=miner,
            validator_hotkey=validator_hotkey,
            validator_signature="sig",
            timestamp=past_time,
            executor_class=str(executor),
            is_organic=True,
            ttl=1,  # definitely expired
        )

    busy_map = await receipts().get_busy_executor_count(executor_class=executor, at_time=now)
    assert miner not in busy_map


@pytest.mark.asyncio
@pytest.mark.django_db(transaction=True)
async def test_get_busy_executor_count__two_miners_saturated():
    executor = ExecutorClass.always_on__gpu_24gb
    validator_hotkey = "validator_hotkey"
    now = datetime.datetime.now(datetime.UTC)

    miners_counts = {"miner_one": 2, "miner_two": 3}

    for miner, count in miners_counts.items():
        for _ in range(count):
            await JobStartedReceipt.objects.acreate(
                job_uuid=str(uuid.uuid4()),
                miner_hotkey=miner,
                validator_hotkey=validator_hotkey,
                validator_signature="sig",
                timestamp=now,
                executor_class=str(executor),
                is_organic=True,
                ttl=3600,
            )

    busy_map = await receipts().get_busy_executor_count(executor_class=executor, at_time=now)
    for miner, count in miners_counts.items():
        assert busy_map.get(miner) == count
