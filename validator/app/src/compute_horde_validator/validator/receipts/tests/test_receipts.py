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
async def test_get_valid_job_started_receipts_for_miner_filters_correctly(settings):
    miner_hotkey = "miner_hotkey_valid"
    other_miner = "miner_hotkey_other"
    validator_hotkey = settings.BITTENSOR_WALLET().get_hotkey().ss58_address

    base_ts = datetime.datetime.now(datetime.UTC)

    await JobStartedReceipt.objects.acreate(
        job_uuid=str(uuid.uuid4()),
        miner_hotkey=miner_hotkey,
        validator_hotkey=validator_hotkey,
        validator_signature="sig",
        timestamp=base_ts - datetime.timedelta(seconds=10),
        executor_class="always_on.gpu-24gb",
        is_organic=True,
        ttl=60,
    )

    await JobStartedReceipt.objects.acreate(
        job_uuid=str(uuid.uuid4()),
        miner_hotkey=miner_hotkey,
        validator_hotkey=validator_hotkey,
        validator_signature="sig",
        timestamp=base_ts - datetime.timedelta(minutes=10),
        executor_class="always_on.gpu-24gb",
        is_organic=False,
        ttl=30,
    )

    await JobStartedReceipt.objects.acreate(
        job_uuid=str(uuid.uuid4()),
        miner_hotkey=other_miner,
        validator_hotkey=validator_hotkey,
        validator_signature="sig",
        timestamp=base_ts - datetime.timedelta(seconds=5),
        executor_class="always_on.gpu-24gb",
        is_organic=True,
        ttl=60,
    )

    results = await receipts().get_valid_job_started_receipts_for_miner(
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
@pytest.mark.django_db(transaction=True)
async def test_get_job_finished_receipts_for_miner_filters_by_uuid(settings):
    miner_hotkey = "miner_hotkey_finished"
    validator_hotkey = settings.BITTENSOR_WALLET().get_hotkey().ss58_address
    common_ts = timezone.now()

    wanted_uuid = str(uuid.uuid4())
    other_uuid = str(uuid.uuid4())

    await JobFinishedReceipt.objects.acreate(
        job_uuid=wanted_uuid,
        miner_hotkey=miner_hotkey,
        validator_hotkey=validator_hotkey,
        validator_signature="sig",
        timestamp=common_ts,
        time_started=common_ts - datetime.timedelta(seconds=2),
        time_took_us=42,
        score_str="0.5",
    )

    await JobFinishedReceipt.objects.acreate(
        job_uuid=other_uuid,
        miner_hotkey=miner_hotkey,
        validator_hotkey=validator_hotkey,
        validator_signature="sig",
        timestamp=common_ts,
        time_started=common_ts - datetime.timedelta(seconds=3),
        time_took_us=43,
        score_str="0.6",
    )

    results = await receipts().get_job_finished_receipts_for_miner(miner_hotkey, [wanted_uuid])

    assert len(results) == 1
    r = results[0]
    assert str(r.job_uuid) == wanted_uuid
    assert r.miner_hotkey == miner_hotkey
    assert r.validator_hotkey == validator_hotkey
    assert r.time_took_us == 42
    assert r.score_str == "0.5"


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


@pytest.mark.asyncio
@pytest.mark.django_db(transaction=True)
async def test_get_finished_jobs_for_block_range_returns_only_in_range(settings):
    # Setup block timestamps
    start_block = 100
    end_block = 105
    start_ts = datetime.datetime.now(datetime.UTC)
    end_ts = start_ts + datetime.timedelta(minutes=10)

    await Block.objects.acreate(block_number=start_block, creation_timestamp=start_ts)
    await Block.objects.acreate(block_number=end_block, creation_timestamp=end_ts)

    miner_hotkey = "miner_hotkey_blockrange"
    validator_hotkey = settings.BITTENSOR_WALLET().get_hotkey().ss58_address

    in_uuid = str(uuid.uuid4())

    await JobStartedReceipt.objects.acreate(
        job_uuid=in_uuid,
        miner_hotkey=miner_hotkey,
        validator_hotkey=validator_hotkey,
        validator_signature="sig",
        timestamp=start_ts - datetime.timedelta(seconds=30),
        executor_class="always_on.gpu-24gb",
        is_organic=True,
        ttl=60,
    )
    in_block_numbers = [start_block, start_block + 1]
    await JobFinishedReceipt.objects.acreate(
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

    uuid_out_of_range = str(uuid.uuid4())
    await JobStartedReceipt.objects.acreate(
        job_uuid=uuid_out_of_range,
        miner_hotkey=miner_hotkey,
        validator_hotkey=validator_hotkey,
        validator_signature="sig",
        timestamp=start_ts - datetime.timedelta(minutes=2),
        executor_class="always_on.gpu-24gb",
        is_organic=True,
        ttl=60,
    )
    await JobFinishedReceipt.objects.acreate(
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

    uuid_end_exclusive = str(uuid.uuid4())
    await JobStartedReceipt.objects.acreate(
        job_uuid=uuid_end_exclusive,
        miner_hotkey=miner_hotkey,
        validator_hotkey=validator_hotkey,
        validator_signature="sig",
        timestamp=end_ts - datetime.timedelta(minutes=1),
        executor_class="always_on.gpu-24gb",
        is_organic=True,
        ttl=60,
    )
    await JobFinishedReceipt.objects.acreate(
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

    rows = await receipts().get_finished_jobs_for_block_range(
        start_block, end_block, executor_class=ExecutorClass.always_on__gpu_24gb
    )

    assert len(rows) == 1
    item = rows[0]
    assert item.miner_hotkey == miner_hotkey
    assert item.validator_hotkey == validator_hotkey
    assert item.job_run_time_us == 1
    assert item.block_start_time == start_ts
    assert item.block_ids == in_block_numbers


@pytest.mark.asyncio
@pytest.mark.django_db(transaction=True)
async def test_get_completed_job_receipts_for_block_range_filters_by_executor_class(settings):
    start_block = 300
    end_block = 305
    start_ts = datetime.datetime.now(datetime.UTC)
    end_ts = start_ts + datetime.timedelta(minutes=10)

    await Block.objects.acreate(block_number=start_block, creation_timestamp=start_ts)
    await Block.objects.acreate(block_number=end_block, creation_timestamp=end_ts)

    miner_hotkey = "miner_hotkey_exec_filter"
    validator_hotkey = settings.BITTENSOR_WALLET().get_hotkey().ss58_address

    exec_gpu = ExecutorClass.always_on__gpu_24gb
    exec_llm = ExecutorClass.always_on__llm__a6000

    gpu_uuid = str(uuid.uuid4())
    await JobStartedReceipt.objects.acreate(
        job_uuid=gpu_uuid,
        miner_hotkey=miner_hotkey,
        validator_hotkey=validator_hotkey,
        validator_signature="sig",
        timestamp=start_ts - datetime.timedelta(seconds=5),
        executor_class=str(exec_gpu),
        is_organic=True,
        ttl=60,
    )
    gpu_block_numbers = [start_block, start_block + 2]
    await JobFinishedReceipt.objects.acreate(
        job_uuid=gpu_uuid,
        miner_hotkey=miner_hotkey,
        validator_hotkey=validator_hotkey,
        validator_signature="sig",
        timestamp=start_ts + datetime.timedelta(minutes=2),
        time_started=start_ts + datetime.timedelta(minutes=1, seconds=30),
        time_took_us=90_000_000,
        score_str="0.9",
        block_numbers=gpu_block_numbers,
    )

    llm_uuid = str(uuid.uuid4())
    await JobStartedReceipt.objects.acreate(
        job_uuid=llm_uuid,
        miner_hotkey=miner_hotkey,
        validator_hotkey=validator_hotkey,
        validator_signature="sig",
        timestamp=start_ts - datetime.timedelta(seconds=4),
        executor_class=str(exec_llm),
        is_organic=False,
        ttl=60,
    )
    await JobFinishedReceipt.objects.acreate(
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
    await JobFinishedReceipt.objects.acreate(
        job_uuid=no_job_started_uuid,
        miner_hotkey=miner_hotkey,
        validator_hotkey=validator_hotkey,
        validator_signature="sig",
        timestamp=start_ts + datetime.timedelta(minutes=4),
        time_started=start_ts + datetime.timedelta(minutes=3, seconds=30),
        time_took_us=30_000_000,
        score_str="0.7",
    )

    rows = await receipts().get_finished_jobs_for_block_range(
        start_block, end_block, executor_class=exec_gpu
    )

    assert len(rows) == 1
    item = rows[0]
    assert item.miner_hotkey == miner_hotkey
    assert item.validator_hotkey == validator_hotkey
    assert item.job_run_time_us == 90_000_000
    assert item.block_start_time == start_ts
    assert item.block_ids == gpu_block_numbers


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
