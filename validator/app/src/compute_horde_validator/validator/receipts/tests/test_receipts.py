import datetime as dt
import uuid
from unittest.mock import AsyncMock, Mock, patch

import bittensor_wallet
import pytest
from aiohttp import web
from asgiref.sync import sync_to_async
from compute_horde.receipts import Receipt
from compute_horde.receipts.models import JobFinishedReceipt, JobStartedReceipt
from compute_horde.receipts.schemas import JobFinishedReceiptPayload
from django.utils.timezone import make_aware

from compute_horde_validator.validator.models import Miner
from compute_horde_validator.validator.models.allowance.internal import Block
from compute_horde_validator.validator.receipts import Receipts


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_scrape_receipts_from_miners_integration():
    with patch("compute_horde.receipts.transfer.checkpoint_backend") as mock_checkpoint:
        mock_backend = Mock()
        mock_backend.get = AsyncMock(return_value=0)
        mock_backend.set = AsyncMock()
        mock_checkpoint.return_value = mock_backend

        miner_keypair1 = bittensor_wallet.Keypair.create_from_mnemonic(
            "almost fatigue race slim picnic mass better clog deal solve already champion"
        )
        miner_keypair2 = bittensor_wallet.Keypair.create_from_mnemonic(
            "edit evoke caught tunnel harsh plug august group enact cable govern immense"
        )
        validator_keypair = bittensor_wallet.Keypair.create_from_mnemonic(
            "slot excuse valid grief praise rifle spoil auction weasel glove pen share"
        )

        await sync_to_async(Miner.objects.create)(
            hotkey=miner_keypair1.ss58_address, address="127.0.0.1", port=7001
        )
        await sync_to_async(Miner.objects.create)(
            hotkey=miner_keypair2.ss58_address, address="127.0.0.1", port=7002
        )

        # Use timestamps that will result in page numbers
        # The page calculation is: int(timestamp // (60 * 5)) where 60*5 = 300 seconds = 5 minutes
        # So we'll use timestamps that result in page numbers like 1, 2, 3
        t0 = make_aware(dt.datetime(2025, 1, 1, 0, 0, 0))  # timestamp 1735689600, page 5785632
        t1 = make_aware(dt.datetime(2025, 1, 1, 0, 5, 0))  # timestamp 1735689900, page 5785633

        # Let's use much smaller timestamps to get reasonable page numbers
        # Use a base timestamp that gives us small page numbers
        base_timestamp = 1000  # This will give us page 3
        t0 = make_aware(dt.datetime.fromtimestamp(base_timestamp))
        t1 = make_aware(dt.datetime.fromtimestamp(base_timestamp + 300))  # +5 minutes, page 4

        await sync_to_async(Block.objects.create)(block_number=1000, creation_timestamp=t0)
        await sync_to_async(Block.objects.create)(block_number=2000, creation_timestamp=t1)

        test_receipts = [
            JobFinishedReceiptPayload(
                job_uuid="00000000-0000-0000-0000-000000000001",
                miner_hotkey=miner_keypair1.ss58_address,
                validator_hotkey=validator_keypair.ss58_address,
                timestamp=t0 + dt.timedelta(minutes=10),
                time_started=t0 + dt.timedelta(minutes=5),
                time_took_us=1_000_000,
                score_str="0.5",
            ),
            JobFinishedReceiptPayload(
                job_uuid="00000000-0000-0000-0000-000000000002",
                miner_hotkey=miner_keypair2.ss58_address,
                validator_hotkey=validator_keypair.ss58_address,
                timestamp=t0 + dt.timedelta(minutes=15),
                time_started=t0 + dt.timedelta(minutes=10),
                time_took_us=2_000_000,
                score_str="0.8",
            ),
        ]

        async def mock_receipts_handler(request):
            # Extract page number from URL like /receipts/3.jsonl
            path = request.path
            if not path.startswith("/receipts/") or not path.endswith(".jsonl"):
                return web.Response(status=404, text="Endpoint not found")

            try:
                page = int(path[10:-6])
            except ValueError:
                return web.Response(status=400, text="Invalid page number")

            if page not in [3, 4]:
                return web.Response(status=404, text="Page not found")

            receipt_lines = []
            for receipt in test_receipts:
                blob = receipt.blob_for_signing()
                if receipt.miner_hotkey == miner_keypair1.ss58_address:
                    miner_signature = f"0x{miner_keypair1.sign(blob).hex()}"
                else:
                    miner_signature = f"0x{miner_keypair2.sign(blob).hex()}"
                validator_signature = f"0x{validator_keypair.sign(blob).hex()}"

                mock_receipt = Receipt(
                    payload=receipt,
                    validator_signature=validator_signature,
                    miner_signature=miner_signature,
                )
                receipt_lines.append(mock_receipt.model_dump_json())

            response_text = "\n".join(receipt_lines)
            return web.Response(text=response_text, content_type="application/json")

        app = web.Application()
        app.router.add_get("/receipts/{page}.jsonl", mock_receipts_handler)

        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 7001)
        await site.start()

        try:
            await Receipts().scrape_receipts_from_miners(
                miner_hotkeys=[miner_keypair1.ss58_address, miner_keypair2.ss58_address],
                start_block=1000,
                end_block=2000,
            )
            stored_receipts_qs = await sync_to_async(JobFinishedReceipt.objects.filter)(
                miner_hotkey__in=[miner_keypair1.ss58_address, miner_keypair2.ss58_address]
            )

            def convert_to_list(qs):
                return list(qs)

            stored_receipts: list[JobFinishedReceipt] = await sync_to_async(convert_to_list)(
                stored_receipts_qs
            )

            assert len(stored_receipts) == 2
            assert str(stored_receipts[0].job_uuid) == "00000000-0000-0000-0000-000000000001"
            assert str(stored_receipts[1].job_uuid) == "00000000-0000-0000-0000-000000000002"

        finally:
            await runner.cleanup()


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_scrape_receipts_network_failure_handling():
    await sync_to_async(Miner.objects.create)(hotkey="hk1", address="127.0.0.1", port=7004)
    t0 = make_aware(dt.datetime(2025, 1, 1, 0, 0, 0))
    await sync_to_async(Block.objects.create)(block_number=1000, creation_timestamp=t0)

    async def mock_failing_handler(request):
        """Mock handler that always raises an exception."""
        raise web.HTTPInternalServerError(text="Server error")

    app = web.Application()
    app.router.add_get("/receipts", mock_failing_handler)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 7004)
    await site.start()

    try:
        result = await Receipts().scrape_receipts_from_miners(
            miner_hotkeys=["hk1"], start_block=1000, end_block=2000
        )

        assert result == []

    finally:
        await runner.cleanup()


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_scrape_receipts_invalid_block_range():
    result = await Receipts().scrape_receipts_from_miners(
        miner_hotkeys=["hk1"], start_block=1000, end_block=1000
    )
    assert result == []

    result = await Receipts().scrape_receipts_from_miners(
        miner_hotkeys=["hk1"], start_block=2000, end_block=1000
    )
    assert result == []


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_scrape_receipts_no_miners():
    result = await Receipts().scrape_receipts_from_miners(
        miner_hotkeys=[], start_block=1000, end_block=2000
    )
    assert result == []


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_scrape_receipts_invalid_miner_endpoints():
    await sync_to_async(Miner.objects.create)(hotkey="hk1", address="127.0.0.1", port=7005)
    await sync_to_async(Miner.objects.create)(hotkey="hk2", address="127.0.0.1", port=7006)
    await sync_to_async(Miner.objects.create)(hotkey="hk3", address="127.0.0.1", port=7007)

    t0 = make_aware(dt.datetime(2025, 1, 1, 0, 0, 0))
    t1 = make_aware(dt.datetime(2025, 1, 1, 1, 0, 0))
    await sync_to_async(Block.objects.create)(block_number=1000, creation_timestamp=t0)
    await sync_to_async(Block.objects.create)(block_number=2000, creation_timestamp=t1)

    result = await Receipts().scrape_receipts_from_miners(
        miner_hotkeys=["hk1", "hk2", "hk3"],
        start_block=1000,
        end_block=2000,
    )

    assert result == []


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_get_valid_job_started_receipts_for_miner():
    miner_hotkey = "test_miner_hotkey"
    validator_hotkey = "test_validator_hotkey"

    valid_receipt = await sync_to_async(JobStartedReceipt.objects.create)(
        job_uuid=str(uuid.uuid4()),
        miner_hotkey=miner_hotkey,
        validator_hotkey=validator_hotkey,
        validator_signature="0xv",
        miner_signature="0xm",
        timestamp=make_aware(dt.datetime.now()),
        executor_class="spin_up-4min.gpu-24gb",
        is_organic=False,
        ttl=300,
    )

    await sync_to_async(JobStartedReceipt.objects.create)(
        job_uuid=str(uuid.uuid4()),
        miner_hotkey=miner_hotkey,
        validator_hotkey=validator_hotkey,
        validator_signature="0xv",
        miner_signature="0xm",
        timestamp=make_aware(dt.datetime.now() - dt.timedelta(minutes=10)),
        executor_class="spin_up-4min.gpu-24gb",
        is_organic=False,
        ttl=300,
    )

    await sync_to_async(JobStartedReceipt.objects.create)(
        job_uuid=str(uuid.uuid4()),
        miner_hotkey="other_miner",
        validator_hotkey=validator_hotkey,
        validator_signature="0xv",
        miner_signature="0xm",
        timestamp=make_aware(dt.datetime.now()),
        executor_class="spin_up-4min.gpu-24gb",
        is_organic=False,
        ttl=300,
    )

    result = await Receipts().get_valid_job_started_receipts_for_miner(
        miner_hotkey, make_aware(dt.datetime.now())
    )

    assert len(result) == 1
    assert result[0].miner_hotkey == miner_hotkey
    assert str(result[0].job_uuid) == str(valid_receipt.job_uuid)


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_get_job_finished_receipts_for_miner():
    job_uuid1 = str(uuid.uuid4())
    job_uuid2 = str(uuid.uuid4())
    job_uuid3 = str(uuid.uuid4())
    miner_hotkey = "test_miner_hotkey"
    validator_hotkey = "test_validator_hotkey"

    await sync_to_async(JobFinishedReceipt.objects.create)(
        job_uuid=job_uuid1,
        miner_hotkey=miner_hotkey,
        validator_hotkey=validator_hotkey,
        validator_signature="0xv",
        miner_signature="0xm",
        timestamp=make_aware(dt.datetime.now()),
        time_started=make_aware(dt.datetime.now() - dt.timedelta(minutes=5)),
        time_took_us=5_000_000,
        score_str="0.8",
    )

    await sync_to_async(JobFinishedReceipt.objects.create)(
        job_uuid=job_uuid2,
        miner_hotkey=miner_hotkey,
        validator_hotkey=validator_hotkey,
        validator_signature="0xv",
        miner_signature="0xm",
        timestamp=make_aware(dt.datetime.now()),
        time_started=make_aware(dt.datetime.now() - dt.timedelta(minutes=3)),
        time_took_us=3_000_000,
        score_str="0.9",
    )

    await sync_to_async(JobFinishedReceipt.objects.create)(
        job_uuid=job_uuid3,
        miner_hotkey=miner_hotkey,
        validator_hotkey=validator_hotkey,
        validator_signature="0xv",
        miner_signature="0xm",
        timestamp=make_aware(dt.datetime.now()),
        time_started=make_aware(dt.datetime.now() - dt.timedelta(minutes=2)),
        time_took_us=2_000_000,
        score_str="0.7",
    )

    requested_jobs = [job_uuid1, job_uuid2]
    result = await Receipts().get_job_finished_receipts_for_miner(miner_hotkey, requested_jobs)

    assert len(result) == 2
    job_uuids = {str(r.job_uuid) for r in result}
    assert job_uuids == {job_uuid1, job_uuid2}


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_get_job_started_receipt_by_uuid():
    job_uuid = str(uuid.uuid4())
    miner_hotkey = "test_miner_hotkey"
    validator_hotkey = "test_validator_hotkey"

    await sync_to_async(JobStartedReceipt.objects.create)(
        job_uuid=job_uuid,
        miner_hotkey=miner_hotkey,
        validator_hotkey=validator_hotkey,
        validator_signature="0xv",
        miner_signature="0xm",  # Add miner signature
        timestamp=make_aware(dt.datetime.now()),
        executor_class="spin_up-4min.gpu-24gb",
        is_organic=False,
        ttl=300,
    )

    result = await Receipts().get_job_started_receipt_by_uuid(job_uuid)

    assert result is not None
    assert str(result.job_uuid) == job_uuid
    assert result.miner_hotkey == miner_hotkey
    assert result.validator_hotkey == validator_hotkey

    non_existent_uuid = str(uuid.uuid4())
    result = await Receipts().get_job_started_receipt_by_uuid(non_existent_uuid)
    assert result is None


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_get_completed_job_receipts_for_block_range():
    t0 = make_aware(dt.datetime(2025, 1, 1, 0, 0, 0))
    t1 = make_aware(dt.datetime(2025, 1, 1, 1, 0, 0))
    t2 = make_aware(dt.datetime(2025, 1, 1, 2, 0, 0))
    t3 = make_aware(dt.datetime(2025, 1, 1, 3, 0, 0))

    await sync_to_async(Block.objects.create)(block_number=1000, creation_timestamp=t0)
    await sync_to_async(Block.objects.create)(block_number=1500, creation_timestamp=t1)
    await sync_to_async(Block.objects.create)(block_number=2000, creation_timestamp=t2)
    await sync_to_async(Block.objects.create)(block_number=3000, creation_timestamp=t3)

    receipt1 = await sync_to_async(JobFinishedReceipt.objects.create)(
        job_uuid=str(uuid.uuid4()),
        miner_hotkey="miner1",
        validator_hotkey="validator1",
        validator_signature="0xv1",
        miner_signature="0xm1",
        timestamp=t0 + dt.timedelta(minutes=30),
        time_started=t0 + dt.timedelta(minutes=25),
        time_took_us=5_000_000,
        score_str="0.8",
    )

    receipt2 = await sync_to_async(JobFinishedReceipt.objects.create)(
        job_uuid=str(uuid.uuid4()),
        miner_hotkey="miner2",
        validator_hotkey="validator2",
        validator_signature="0xv2",
        miner_signature="0xm2",
        timestamp=t2 + dt.timedelta(minutes=30),
        time_started=t2 + dt.timedelta(minutes=25),
        time_took_us=3_000_000,
        score_str="0.9",
    )

    await sync_to_async(JobFinishedReceipt.objects.create)(
        job_uuid=str(uuid.uuid4()),
        miner_hotkey="miner3",
        validator_hotkey="validator3",
        validator_signature="0xv3",
        miner_signature="0xm3",
        timestamp=t0 - dt.timedelta(minutes=1),
        time_started=t0 - dt.timedelta(minutes=2),
        time_took_us=2_000_000,
        score_str="0.7",
    )

    result = await Receipts().get_completed_job_receipts_for_block_range(1000, 1500)

    assert len(result) == 1
    assert str(result[0].payload.job_uuid) == str(receipt1.job_uuid)

    result = await Receipts().get_completed_job_receipts_for_block_range(2000, 3000)

    assert len(result) == 1
    assert str(result[0].payload.job_uuid) == str(receipt2.job_uuid)


@pytest.mark.django_db(transaction=True)
def test_create_job_finished_receipt_success():
    job_uuid = str(uuid.uuid4())
    miner_hotkey = "test_miner_hotkey"
    validator_hotkey = "test_validator_hotkey"
    time_started = dt.datetime.now(dt.UTC)
    time_took_us = 5000000
    score_str = "0.85"

    receipt = Receipts().create_job_finished_receipt(
        job_uuid=job_uuid,
        miner_hotkey=miner_hotkey,
        validator_hotkey=validator_hotkey,
        time_started=time_started,
        time_took_us=time_took_us,
        score_str=score_str,
    )

    assert receipt is not None
    assert isinstance(receipt, JobFinishedReceipt)

    assert receipt.job_uuid == job_uuid
    assert receipt.miner_hotkey == miner_hotkey
    assert receipt.validator_hotkey == validator_hotkey
    assert receipt.time_started == time_started
    assert receipt.time_took_us == time_took_us
    assert receipt.score_str == score_str
    assert receipt.validator_signature is not None
    assert len(receipt.validator_signature) > 0

    assert receipt.time_took() == dt.timedelta(microseconds=time_took_us)
    assert receipt.score() == float(score_str)
