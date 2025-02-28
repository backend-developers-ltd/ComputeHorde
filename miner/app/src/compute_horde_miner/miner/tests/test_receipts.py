from datetime import datetime
from unittest.mock import AsyncMock
from uuid import uuid4

import bittensor
import pytest
from compute_horde.mv_protocol.validator_requests import (
    AuthenticationPayload,
    JobFinishedReceiptPayload,
    JobStartedReceiptPayload,
    V0AuthenticateRequest,
    V0InitialJobRequest,
    V0JobAcceptedReceiptRequest,
    V0JobFinishedReceiptRequest,
)
from compute_horde.receipts.models import JobAcceptedReceipt, JobFinishedReceipt, JobStartedReceipt
from compute_horde.receipts.schemas import JobAcceptedReceiptPayload
from compute_horde.utils import sign_blob
from compute_horde_core.executor_class import ExecutorClass
from compute_horde_core.volume import VolumeType
from django.utils import timezone
from pytest_mock import MockerFixture

from compute_horde_miner.miner.models import Validator
from compute_horde_miner.miner.tests.executor_manager import fake_executor
from compute_horde_miner.miner.tests.validator import fake_validator


@pytest.fixture
def job_uuid():
    _job_uuid = str(uuid4())
    fake_executor.job_uuid = _job_uuid
    yield _job_uuid
    fake_executor.job_uuid = None


@pytest.mark.parametrize(
    "organic_job",
    (True, False),
)
@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_receipt_is_saved(
    validator_wallet: bittensor.wallet,
    miner_wallet: bittensor.wallet,
    mocker: MockerFixture,
    organic_job: bool,
    job_uuid: str,
    settings,
) -> None:
    executor = mocker.patch("compute_horde_miner.miner.miner_consumer.validator_interface.current")
    receipt_store_factory = mocker.patch(
        "compute_horde_miner.miner.miner_consumer.validator_interface.current_store"
    )
    stored_receipts = []
    receipt_store_factory.return_value.store = lambda rs: stored_receipts.extend(rs)

    executor.executor_manager.get_manifest = AsyncMock(return_value={})
    executor.executor_manager.reserve_executor_class = AsyncMock()
    executor.executor_manager.get_executor_public_address = AsyncMock()
    executor.executor_manager.wait_for_executor_reservation = AsyncMock()

    settings.DEBUG_TURN_AUTHENTICATION_OFF = True
    settings.BITTENSOR_WALLET = lambda: miner_wallet

    await Validator.objects.acreate(
        public_key=validator_wallet.hotkey.ss58_address,
        active=True,
    )

    # 1. Authenticate to miner as test validator
    # 2. Send JobStarted and JobFinished receipts
    async with fake_validator(validator_wallet) as fake_validator_channel:
        # Authenticate, otherwise the consumer will refuse to talk to us
        auth_payload = AuthenticationPayload(
            validator_hotkey=validator_wallet.hotkey.ss58_address,
            miner_hotkey=miner_wallet.hotkey.ss58_address,
            timestamp=int(datetime.now().timestamp()),
        )
        await fake_validator_channel.send_to(
            V0AuthenticateRequest(
                payload=auth_payload,
                signature=sign_blob(validator_wallet.hotkey, auth_payload.blob_for_signing()),
            ).model_dump_json()
        )
        response = await fake_validator_channel.receive_json_from()
        assert response == {
            "message_type": "V0ExecutorManifestRequest",
            "manifest": {"executor_classes": []},
        }

        # Send the receipts
        job_started_receipt_payload = JobStartedReceiptPayload(
            job_uuid=job_uuid,
            is_organic=organic_job,
            miner_hotkey=miner_wallet.hotkey.ss58_address,
            validator_hotkey=validator_wallet.hotkey.ss58_address,
            timestamp=timezone.now(),
            executor_class=ExecutorClass.spin_up_4min__gpu_24gb,
            max_timeout=60,
            ttl=5,
        )
        job_started_receipt_signature = sign_blob(
            validator_wallet.hotkey, job_started_receipt_payload.blob_for_signing()
        )
        await fake_validator_channel.send_to(
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

        # skip doing the job, and only send receipts

        job_accepted_receipt_payload = JobAcceptedReceiptPayload(
            job_uuid=job_uuid,
            miner_hotkey=miner_wallet.hotkey.ss58_address,
            validator_hotkey=validator_wallet.hotkey.ss58_address,
            timestamp=timezone.now(),
            time_accepted=timezone.now(),
            ttl=5,
        )
        await fake_validator_channel.send_to(
            V0JobAcceptedReceiptRequest(
                payload=job_accepted_receipt_payload,
                signature=sign_blob(
                    validator_wallet.hotkey, job_accepted_receipt_payload.blob_for_signing()
                ),
            ).model_dump_json()
        )

        job_finished_receipt_payload = JobFinishedReceiptPayload(
            job_uuid=job_uuid,
            miner_hotkey=miner_wallet.hotkey.ss58_address,
            validator_hotkey=validator_wallet.hotkey.ss58_address,
            timestamp=timezone.now(),
            time_started=timezone.now(),
            time_took_us=123,
            score_str="123.45",
        )
        await fake_validator_channel.send_to(
            V0JobFinishedReceiptRequest(
                payload=job_finished_receipt_payload,
                signature=sign_blob(
                    validator_wallet.hotkey, job_finished_receipt_payload.blob_for_signing()
                ),
            ).model_dump_json()
        )

    assert await JobStartedReceipt.objects.filter(
        job_uuid=job_uuid, is_organic=organic_job
    ).aexists()
    assert await JobAcceptedReceipt.objects.filter(job_uuid=job_uuid).aexists()
    assert await JobFinishedReceipt.objects.filter(job_uuid=job_uuid).aexists()

    stored_payloads = [r.payload for r in stored_receipts]
    assert stored_payloads == [
        job_started_receipt_payload,
        job_accepted_receipt_payload,
        job_finished_receipt_payload,
    ]
