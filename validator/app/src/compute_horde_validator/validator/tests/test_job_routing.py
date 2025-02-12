import uuid
from datetime import timedelta

import pytest
from compute_horde.executor_class import DEFAULT_EXECUTOR_CLASS, ExecutorClass
from compute_horde.fv_protocol.facilitator_requests import V2JobRequest
from compute_horde.receipts import Receipt
from compute_horde.receipts.models import JobStartedReceipt
from compute_horde.receipts.schemas import JobStartedReceiptPayload
from compute_horde.utils import sign_blob
from django.utils import timezone

from compute_horde_validator.validator.models import (
    Cycle,
    Miner,
    MinerBlacklist,
    MinerManifest,
    SyntheticJobBatch,
)
from compute_horde_validator.validator.organic_jobs import routing

JOB_REQUEST = V2JobRequest(
    uuid=str(uuid.uuid4()),
    executor_class=DEFAULT_EXECUTOR_CLASS,
    docker_image="doesntmatter",
    raw_script="doesntmatter",
    args=[],
    env={},
    use_gpu=False,
)


@pytest.fixture(autouse=True)
def setup_db():
    now = timezone.now()
    cycle = Cycle.objects.create(start=1, stop=2)
    batch = SyntheticJobBatch.objects.create(block=1, created_at=now, cycle=cycle)
    miners = [Miner.objects.create(hotkey=f"miner_{i}") for i in range(5)]
    for i, miner in enumerate(miners):
        MinerManifest.objects.create(
            miner=miner,
            batch=batch,
            created_at=now - timedelta(minutes=i * 2),
            executor_class=DEFAULT_EXECUTOR_CLASS,
            executor_count=5,
            online_executor_count=5,
        )


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_pick_miner_for_job__picks_a_miner():
    assert await routing.pick_miner_for_job_request(JOB_REQUEST) is not None


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_pick_miner_for_job__no_matching_executor_class():
    with pytest.raises(routing.NoMinerForExecutorType):
        await routing.pick_miner_for_job_request(
            JOB_REQUEST.__replace__(
                executor_class=next(c for c in ExecutorClass if c != DEFAULT_EXECUTOR_CLASS)
            )
        )


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_pick_miner_for_job__no_online_executors():
    await MinerManifest.objects.all().aupdate(online_executor_count=0)
    with pytest.raises(routing.NoMinerForExecutorType):
        await routing.pick_miner_for_job_request(JOB_REQUEST)


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_pick_miner_for_job__miner_banned():
    async for miner in Miner.objects.all():
        await MinerBlacklist.objects.acreate(
            miner=miner,
            reason=MinerBlacklist.BlacklistReason.JOB_FAILED,
            expires_at=timezone.now() + timedelta(minutes=5),
        )

    with pytest.raises(routing.NoMinerForExecutorType):
        await routing.pick_miner_for_job_request(JOB_REQUEST)


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_pick_miner_for_job__miner_blacklist_expires():
    async for miner in Miner.objects.all():
        await MinerBlacklist.objects.acreate(
            miner=miner,
            reason=MinerBlacklist.BlacklistReason.JOB_FAILED,
            expires_at=timezone.now() - timedelta(minutes=15),
        )

    await routing.pick_miner_for_job_request(JOB_REQUEST)


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_pick_miner_for_job__all_executors_busy(validator_keypair, miner_keypair):
    async for miner in Miner.objects.all():
        for _ in range(5):
            payload = JobStartedReceiptPayload(
                job_uuid=str(uuid.uuid4()),
                miner_hotkey=miner.hotkey,
                validator_hotkey=validator_keypair.ss58_address,
                timestamp=timezone.now(),
                executor_class=DEFAULT_EXECUTOR_CLASS,
                max_timeout=60,
                is_organic=True,
                ttl=60,
            )
            blob = payload.blob_for_signing()
            receipt = Receipt(
                payload=payload,
                validator_signature=sign_blob(validator_keypair, blob),
                miner_signature=sign_blob(miner_keypair, blob),
            )
            await JobStartedReceipt.from_receipt(receipt).asave()

    with pytest.raises(routing.AllMinersBusy):
        await routing.pick_miner_for_job_request(JOB_REQUEST)
