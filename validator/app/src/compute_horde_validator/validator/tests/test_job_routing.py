import uuid
from datetime import timedelta
from decimal import Decimal
from unittest.mock import patch

import pytest
from compute_horde.executor_class import DEFAULT_EXECUTOR_CLASS
from compute_horde.fv_protocol.facilitator_requests import V2JobRequest
from compute_horde.receipts import Receipt
from compute_horde.receipts.models import JobFinishedReceipt, JobStartedReceipt
from compute_horde.receipts.schemas import JobStartedReceiptPayload
from compute_horde.utils import sign_blob
from compute_horde_core.executor_class import ExecutorClass
from django.utils import timezone
from freezegun import freeze_time

from compute_horde_validator.validator.dynamic_config import aget_config
from compute_horde_validator.validator.models import (
    ComputeTimeAllowance,
    Cycle,
    MetagraphSnapshot,
    Miner,
    MinerBlacklist,
    MinerManifest,
    SyntheticJobBatch,
)
from compute_horde_validator.validator.organic_jobs import routing
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

pytestmark = [
    pytest.mark.override_config(DYNAMIC_MINIMUM_COLLATERAL_AMOUNT_WEI=10000000000000000),
]


@pytest.fixture(autouse=True)
def miners():
    return [
        Miner.objects.create(hotkey=f"miner_{i}", collateral_wei=Decimal(10**18)) for i in range(5)
    ]


@pytest.fixture(autouse=True)
def validator(settings):
    return Miner.objects.create(hotkey=settings.BITTENSOR_WALLET().hotkey.ss58_address)


@pytest.fixture(autouse=True)
def setup_db(miners, validator):
    now = timezone.now()
    cycle = Cycle.objects.create(start=1, stop=2)
    batch = SyntheticJobBatch.objects.create(block=1, created_at=now, cycle=cycle)
    for i, miner in enumerate(miners):
        MinerManifest.objects.create(
            miner=miner,
            batch=batch,
            created_at=now - timedelta(minutes=i * 2),
            executor_class=DEFAULT_EXECUTOR_CLASS,
            executor_count=5,
            online_executor_count=5,
        )
    MetagraphSnapshot.objects.create(
        id=MetagraphSnapshot.SnapshotType.LATEST,
        block=1,
        alpha_stake=[2000] + [0] * len(miners),
        tao_stake=[2000] + [0] * len(miners),
        stake=[2000] + [0] * len(miners),
        uids=list(range(len(miners) + 1)),
        hotkeys=[validator.hotkey] + [m.hotkey for m in miners],
        serving_hotkeys=["miner_0", "miner_1"],
    )
    for miner in miners:
        ComputeTimeAllowance.objects.create(
            cycle=cycle,
            miner=miner,
            validator=validator,
            initial_allowance=100,
            remaining_allowance=100,
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


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_pick_miner_for_job__trusted_miner():
    job_request = JOB_REQUEST.__replace__(on_trusted_miner=True)
    miner = await routing.pick_miner_for_job_request(job_request)
    assert miner.hotkey == TRUSTED_MINER_FAKE_KEY


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_preliminary_reservation__prevents_double_select():
    await MinerManifest.objects.aupdate(executor_count=1, online_executor_count=1)

    picked_miners: set[str] = set()

    # We have 5 miners
    for _ in range(5):
        job_request = JOB_REQUEST.__replace__(uuid=str(uuid.uuid4()))
        miner = await routing.pick_miner_for_job_request(job_request)
        picked_miners.add(miner.hotkey)

    # No miner is double-selected
    assert len(picked_miners) == 5

    # Last request has nothing to choose from
    with pytest.raises(routing.AllMinersBusy):
        await routing.pick_miner_for_job_request(JOB_REQUEST.__replace__(uuid=str(uuid.uuid4())))


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_preliminary_reservation__minimum_collateral():
    await MinerManifest.objects.aupdate(executor_count=1, online_executor_count=1)
    miner_ne = await Miner.objects.afirst()
    miner_ne.collateral_wei = Decimal(int(0.009 * 10**18))
    await miner_ne.asave()

    picked_miners: set[str] = set()

    # We have 4 miners who have enough collateral
    for _ in range(4):
        job_request = JOB_REQUEST.__replace__(uuid=str(uuid.uuid4()))
        miner = await routing.pick_miner_for_job_request(job_request)
        picked_miners.add(miner.hotkey)

    # No miner is double-selected
    assert len(picked_miners) == 4

    # Miner with not enough collateral is not picked
    assert miner_ne.hotkey not in picked_miners

    # Last request has nothing to choose from
    with pytest.raises(routing.AllMinersBusy):
        await routing.pick_miner_for_job_request(JOB_REQUEST.__replace__(uuid=str(uuid.uuid4())))


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_preliminary_reservation__lifted_by_receipt(miners, validator):
    miner = miners[0]
    await Miner.objects.exclude(id__in=[miner.id, validator.id]).adelete()
    await MinerManifest.objects.aupdate(executor_count=1, online_executor_count=1)
    job_request_1 = JOB_REQUEST.__replace__(uuid=str(uuid.uuid4()))
    job_request_2 = JOB_REQUEST.__replace__(uuid=str(uuid.uuid4()))

    # Pick miner for job
    picked_miner = await routing.pick_miner_for_job_request(job_request_1)
    assert picked_miner == miner

    # Create receipt noting that the job was finished
    await JobFinishedReceipt.objects.acreate(
        job_uuid=job_request_1.uuid,
        miner_hotkey=miner.hotkey,
        validator_hotkey="doesntmatter",
        validator_signature="doesntmatter",
        miner_signature="doesntmatter",
        timestamp=timezone.now(),
        time_started=timezone.now(),
        time_took_us=123,
        score_str="1",
    )

    # The same miner should be immediately pickable
    picked_miner = await routing.pick_miner_for_job_request(job_request_2)
    assert picked_miner == miner


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_preliminary_reservation__lifted_after_timeout(miners, validator):
    miner = miners[0]
    await Miner.objects.exclude(id__in=[miner.id, validator.id]).adelete()
    await MinerManifest.objects.aupdate(executor_count=1, online_executor_count=1)
    job_request_1 = JOB_REQUEST.__replace__(uuid=str(uuid.uuid4()))
    job_request_2 = JOB_REQUEST.__replace__(uuid=str(uuid.uuid4()))

    with freeze_time() as now:
        # Pick miner for job
        picked_miner = await routing.pick_miner_for_job_request(job_request_1)
        assert picked_miner == miner

        # Wait for timeout
        now.tick(
            delta=await aget_config("DYNAMIC_ROUTING_PRELIMINARY_RESERVATION_TIME_SECONDS") + 1
        )

        # The same miner should be immediately pickable
        picked_miner = await routing.pick_miner_for_job_request(job_request_2)
        assert picked_miner == miner


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_pick_miner_for_job__no_allowance_in_db():
    await ComputeTimeAllowance.objects.all().adelete()
    with pytest.raises(routing.NoMinerWithEnoughAllowance):
        await routing.pick_miner_for_job_request(JOB_REQUEST)


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_pick_miner_for_job__no_miner_with_enough_allowance():
    with pytest.raises(routing.NoMinerWithEnoughAllowance):
        job_request = JOB_REQUEST.__replace__(execution_time_limit=101)
        await routing.pick_miner_for_job_request(job_request)


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
@pytest.mark.override_config(DYNAMIC_CHECK_ALLOWANCE_WHILE_ROUTING=True)
@pytest.mark.xfail(raises=routing.NoMinerWithEnoughAllowance)
async def test_pick_miner_for_job__respects_allowance_check_feature_flag__enabled():
    await ComputeTimeAllowance.objects.all().aupdate(remaining_allowance=0)
    job_request = JOB_REQUEST.__replace__(execution_time_limit=101)
    miner = await routing.pick_miner_for_job_request(job_request)
    assert miner, "No miner was picked"


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
@pytest.mark.override_config(DYNAMIC_CHECK_ALLOWANCE_WHILE_ROUTING=False)
async def test_pick_miner_for_job__respects_allowance_check_feature_flag__disabled():
    await ComputeTimeAllowance.objects.all().aupdate(remaining_allowance=0)
    job_request = JOB_REQUEST.__replace__(execution_time_limit=101)
    miner = await routing.pick_miner_for_job_request(job_request)
    assert miner, "No miner was picked"


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_pick_miner_for_job__miner_with_more_allowance_percentage_is_chosen(miners):
    await ComputeTimeAllowance.objects.all().aupdate(remaining_allowance=0)

    await ComputeTimeAllowance.objects.filter(miner=miners[0]).aupdate(remaining_allowance=75)
    await ComputeTimeAllowance.objects.filter(miner=miners[1]).aupdate(remaining_allowance=80)

    chosen_miner = await routing.pick_miner_for_job_request(JOB_REQUEST)
    assert chosen_miner == miners[1]


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_pick_miner_for_job__collateral_tiebreak_on_equal_allowance_percentage(miners):
    await ComputeTimeAllowance.objects.all().aupdate(remaining_allowance=0)

    miner1 = miners[0]
    miner2 = miners[1]
    await ComputeTimeAllowance.objects.filter(miner=miner1).aupdate(remaining_allowance=30)
    await ComputeTimeAllowance.objects.filter(miner=miner2).aupdate(remaining_allowance=30)
    miner1.collateral_wei = Decimal(int(0.1 * 10**18))
    miner2.collateral_wei = Decimal(int(0.2 * 10**18))
    await miner1.asave()
    await miner2.asave()

    chosen_miner = await routing.pick_miner_for_job_request(JOB_REQUEST)
    assert chosen_miner == miner2


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_pick_miner_for_job__use_subtensor_for_block_on_cache_miss(miners):
    await MetagraphSnapshot.objects.all().adelete()

    with patch(
        "compute_horde_validator.validator.organic_jobs.routing.bittensor.AsyncSubtensor"
    ) as mocked_subtensor:
        mocked_subtensor.return_value.__aenter__.return_value.get_current_block.return_value = 1
        await routing.pick_miner_for_job_request(JOB_REQUEST)


@pytest.mark.django_db(transaction=True)
@pytest.mark.override_config(DYNAMIC_ALLOW_CROSS_CYCLE_ORGANIC_JOBS=True)
@patch(
    "compute_horde_validator.validator.organic_jobs.routing.get_seconds_remaining_in_current_cycle"
)
@pytest.mark.asyncio
async def test_cross_cycle_job__allowed_when_enabled(mock_get_seconds_remaining):
    mock_get_seconds_remaining.return_value = 5
    long_job = JOB_REQUEST.__replace__(execution_time_limit=90)
    miner = await routing.pick_miner_for_job_request(long_job)
    assert miner is not None


@pytest.mark.django_db(transaction=True)
@pytest.mark.override_config(DYNAMIC_ALLOW_CROSS_CYCLE_ORGANIC_JOBS=False)
@patch(
    "compute_horde_validator.validator.organic_jobs.routing.get_seconds_remaining_in_current_cycle"
)
@pytest.mark.asyncio
async def test_cross_cycle_job__disallowed_when_disabled(mock_get_seconds_remaining):
    mock_get_seconds_remaining.return_value = 5
    long_job = JOB_REQUEST.__replace__(execution_time_limit=90)
    with pytest.raises(routing.NotEnoughTimeInCycle):
        await routing.pick_miner_for_job_request(long_job)
