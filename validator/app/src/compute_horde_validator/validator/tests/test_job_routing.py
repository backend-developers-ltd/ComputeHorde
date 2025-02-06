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

from compute_horde_validator.validator.models import Miner, MinerManifest, SyntheticJobBatch
from compute_horde_validator.validator.organic_jobs import routing


async def setup_db(n: int = 1):
    now = timezone.now()
    batch = await SyntheticJobBatch.objects.acreate(block=1, created_at=now)
    miners = [await Miner.objects.acreate(hotkey=f"miner_{i}") for i in range(0, n)]
    for i, miner in enumerate(miners):
        await MinerManifest.objects.acreate(
            miner=miner,
            batch=batch,
            created_at=now - timedelta(minutes=i * 2),
            executor_class=DEFAULT_EXECUTOR_CLASS,
            online_executor_count=5,
        )


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_pick_miner_for_job__no_matching_executor_class():
    await setup_db()
    with pytest.raises(routing.NoMinerForExecutorType):
        await routing.pick_miner_for_job(
            V2JobRequest(
                uuid=str(uuid.uuid4()),
                executor_class=next(c for c in ExecutorClass if c != DEFAULT_EXECUTOR_CLASS),
                docker_image="doesntmatter",
                raw_script="doesntmatter",
                args=[],
                env={},
                use_gpu=False,
            )
        )


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_pick_miner_for_job__no_online_executors():
    await setup_db()
    await MinerManifest.objects.all().aupdate(online_executor_count=0)
    with pytest.raises(routing.NoMinerForExecutorType):
        await routing.pick_miner_for_job(
            V2JobRequest(
                uuid=str(uuid.uuid4()),
                executor_class=DEFAULT_EXECUTOR_CLASS,
                docker_image="doesntmatter",
                raw_script="doesntmatter",
                args=[],
                env={},
                use_gpu=False,
            )
        )


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_pick_miner_for_job__all_executors_busy(validator_keypair, miner_keypair):
    await setup_db()
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
            await routing.pick_miner_for_job(
                V2JobRequest(
                    uuid=str(uuid.uuid4()),
                    executor_class=DEFAULT_EXECUTOR_CLASS,
                    docker_image="doesntmatter",
                    raw_script="doesntmatter",
                    args=[],
                    env={},
                    use_gpu=False,
                )
            )
