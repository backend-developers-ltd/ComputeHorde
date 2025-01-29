from datetime import datetime
from unittest.mock import patch
from uuid import uuid4

import pytest
from compute_horde.base.volume import VolumeType
from compute_horde.executor_class import ExecutorClass
from compute_horde.mv_protocol.validator_requests import (
    AuthenticationPayload,
    V0AuthenticateRequest,
    V0InitialJobRequest,
)
from compute_horde.receipts.models import JobStartedReceipt
from compute_horde.receipts.schemas import JobStartedReceiptPayload
from compute_horde.utils import sign_blob
from django.utils import timezone

from compute_horde_miner.miner.executor_manager.current import executor_manager
from compute_horde_miner.miner.models import Validator
from compute_horde_miner.miner.tests.executor_manager import fake_executor
from compute_horde_miner.miner.tests.validator import fake_validator

pytestmark = [pytest.mark.asyncio, pytest.mark.django_db]


@pytest.fixture
def job_uuid():
    _job_uuid = str(uuid4())
    fake_executor.job_uuid = _job_uuid
    yield _job_uuid
    fake_executor.job_uuid = None


async def _authenticate(channel, validator, miner_hotkey):
    auth_payload = AuthenticationPayload(
        validator_hotkey=validator.hotkey.ss58_address,
        miner_hotkey=miner_hotkey,
        timestamp=int(datetime.now().timestamp()),
    )
    await channel.send_to(
        V0AuthenticateRequest(
            payload=auth_payload,
            signature=sign_blob(validator.hotkey, auth_payload.blob_for_signing()),
        ).model_dump_json()
    )


async def test_receipt_stored_before_executor_reserved(job_uuid, validator_wallet, miner_wallet):
    receipt_created_before_executor_reserved = False

    async def _on_start_new_executor(*args, **kwargs):
        nonlocal receipt_created_before_executor_reserved
        receipt_created_before_executor_reserved = await JobStartedReceipt.objects.acount() == 1

    with patch.object(executor_manager, "reserve_executor_class") as reserve_executor_class:
        reserve_executor_class.side_effect = _on_start_new_executor
        await Validator.objects.acreate(
            public_key=validator_wallet.hotkey.ss58_address, active=True
        )
        async with fake_validator(validator_wallet) as validator_channel:
            await _authenticate(
                validator_channel, validator_wallet, miner_wallet.hotkey.ss58_address
            )

            # Organic job
            job_started_receipt_payload = JobStartedReceiptPayload(
                job_uuid=job_uuid,
                is_organic=True,
                miner_hotkey=miner_wallet.hotkey.ss58_address,
                validator_hotkey=validator_wallet.hotkey.ss58_address,
                timestamp=timezone.now(),
                executor_class=ExecutorClass.spin_up_4min__gpu_24gb,
                max_timeout=60,
                ttl=60,
            )
            job_started_receipt_signature = sign_blob(
                validator_wallet.hotkey,
                job_started_receipt_payload.blob_for_signing(),
            )
            await validator_channel.send_to(
                V0InitialJobRequest(
                    job_uuid=job_uuid,
                    executor_class=ExecutorClass.spin_up_4min__gpu_24gb,
                    base_docker_image_name="it's teeeeests",
                    timeout_seconds=60,
                    volume_type=VolumeType.inline,
                    job_started_receipt_payload=job_started_receipt_payload,
                    job_started_receipt_signature=job_started_receipt_signature,
                ).model_dump_json()
            )
            # await asyncio.sleep(1)

        reserve_executor_class.assert_called()
        assert receipt_created_before_executor_reserved


async def test_reject_as_busy_when_busy(job_uuid, validator_wallet, miner_wallet):
    pass
