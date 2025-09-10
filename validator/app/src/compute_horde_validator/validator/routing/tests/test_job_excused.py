import uuid

import pytest
from compute_horde.miner_client.organic import MinerRejectedJob
from compute_horde.protocol_consts import JobRejectionReason
from compute_horde.protocol_messages import V0DeclineJobRequest
from compute_horde.receipts.models import JobStartedReceipt
from compute_horde_core.executor_class import ExecutorClass
from django.conf import settings

from compute_horde_validator.validator.models import Miner, MinerIncident, OrganicJob
from compute_horde_validator.validator.organic_jobs.miner_driver import drive_organic_job
from compute_horde_validator.validator.receipts.default import receipts


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_excused_job_no_incident(monkeypatch):
    """If a miner rejects a job as BUSY but provides a valid excuse (sufficient receipts),
    the job is marked EXCUSED and no MinerIncident is recorded."""

    # Create a miner
    miner = await Miner.objects.acreate(
        hotkey="excused_miner_hotkey",
        address="127.0.0.1",
        port=1234,
        ip_version=4,
    )

    # Prepare job UUID and create corresponding OrganicJob (pending)
    job_uuid = uuid.uuid4()
    job = await OrganicJob.objects.acreate(
        job_uuid=job_uuid,
        miner=miner,
        miner_address="127.0.0.1",
        miner_address_ip_version=4,
        miner_port=1234,
        executor_class=ExecutorClass.always_on__gpu_24gb.value,
        job_description="test job",
        block=100,
    )

    # Create a JobStartedReceipt for this job so miner_driver can fetch request time
    payload, validator_signature = receipts().create_job_started_receipt(
        job_uuid=str(job_uuid),
        miner_hotkey=miner.hotkey,
        validator_hotkey=settings.BITTENSOR_WALLET().get_hotkey().ss58_address,
        executor_class=ExecutorClass.always_on__gpu_24gb,
        is_organic=True,
        ttl=60,
    )
    await JobStartedReceipt.from_payload(payload, validator_signature=validator_signature).asave()

    # Monkeypatch excuse validation helpers to simulate a valid excuse (1 executor, 1 valid excuse)
    async def fake_filter_valid_excuse_receipts(**_kwargs):  # returns list length 1
        return [object()]

    async def fake_get_expected_miner_executor_count(**_kwargs):
        return 1

    monkeypatch.setattr(
        "compute_horde_validator.validator.organic_jobs.miner_driver.job_excuses.filter_valid_excuse_receipts",
        fake_filter_valid_excuse_receipts,
    )
    monkeypatch.setattr(
        "compute_horde_validator.validator.organic_jobs.miner_driver.job_excuses.get_expected_miner_executor_count",
        fake_get_expected_miner_executor_count,
    )

    # Patch execute_organic_job_on_miner to simulate a BUSY rejection
    async def fake_execute_organic_job_on_miner(*_args, **_kwargs):
        msg = V0DeclineJobRequest(
            job_uuid=str(job_uuid),
            reason=JobRejectionReason.BUSY,
            message="busy",
            receipts=[],
            context=None,
        )
        raise MinerRejectedJob(msg)

    monkeypatch.setattr(
        "compute_horde_validator.validator.organic_jobs.miner_driver.execute_organic_job_on_miner",
        fake_execute_organic_job_on_miner,
    )

    # Minimal AdminJobRequest-like object (drive_organic_job only inspects attributes + get_args())
    class DummyAdminRequest:
        uuid = job_uuid
        executor_class = ExecutorClass.always_on__gpu_24gb.value
        docker_image = "ubuntu:latest"
        use_gpu = False
        timeout = 30

        def get_args(self):
            return []

        volume = None
        output_upload = None

    dummy_request = DummyAdminRequest()

    # Execute driver (should mark job as EXCUSED, not record incident)
    await drive_organic_job(
        miner_client=type("MC", (), {"my_hotkey": miner.hotkey, "miner_name": miner.hotkey})(),
        job=job,
        job_request=dummy_request,
    )

    await job.arefresh_from_db()
    assert job.status == OrganicJob.Status.EXCUSED
    assert await MinerIncident.objects.acount() == 0
