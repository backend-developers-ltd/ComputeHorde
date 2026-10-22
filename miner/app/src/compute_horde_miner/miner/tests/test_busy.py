from datetime import timedelta
from unittest.mock import patch
from uuid import uuid4

import pytest
from compute_horde.executor_class import DEFAULT_EXECUTOR_CLASS
from compute_horde.protocol_consts import JobRejectionReason
from compute_horde.receipts import Receipt
from compute_horde.receipts.models import JobStartedReceipt
from compute_horde.receipts.schemas import JobStartedReceiptPayload
from compute_horde.test_wallet import (
    get_test_miner_wallet,
    get_test_validator_wallet,
)
from compute_horde.utils import sign_blob
from compute_horde_core.executor_class import ExecutorClass
from django.utils import timezone

from compute_horde_miner.miner.executor_manager.current import executor_manager
from compute_horde_miner.miner.executor_manager.v0 import AllExecutorsBusy
from compute_horde_miner.miner.models import Validator
from compute_horde_miner.miner.tests.executor_manager import fake_executor
from compute_horde_miner.miner.tests.validator import (
    WEBSOCKET_TIMEOUT,
    authenticate,
    fake_validator,
    initial_job_request,
    wait_until,
)

pytestmark = [pytest.mark.asyncio, pytest.mark.django_db(transaction=True)]


@pytest.fixture
def job_uuid():
    _job_uuid = str(uuid4())
    fake_executor.job_uuid = _job_uuid
    yield _job_uuid
    fake_executor.job_uuid = None


async def _send_initial_job_request(validator_channel, job_uuid):
    await validator_channel.send_to(initial_job_request(job_uuid).model_dump_json())


async def test_receipt_stored_before_executor_reserved(job_uuid):
    validator_wallet = get_test_validator_wallet()

    receipt_created_before_executor_reserved = False

    async def _on_start_new_executor(*args, **kwargs):
        nonlocal receipt_created_before_executor_reserved
        receipt_created_before_executor_reserved = await JobStartedReceipt.objects.acount() == 1

    async def executor_reserved() -> bool:
        return reserve_executor_class.called

    with patch.object(executor_manager, "reserve_executor_class") as reserve_executor_class:
        reserve_executor_class.side_effect = _on_start_new_executor
        await Validator.objects.acreate(
            public_key=validator_wallet.hotkey.ss58_address, active=True
        )
        async with fake_validator() as validator_channel:
            await authenticate(validator_channel)
            await _send_initial_job_request(validator_channel, job_uuid)
            await wait_until(executor_reserved)

        reserve_executor_class.assert_called()
        assert receipt_created_before_executor_reserved


async def test_reject_as_busy_when_busy(job_uuid):
    validator_wallet = get_test_validator_wallet()
    miner_wallet = get_test_miner_wallet()

    await Validator.objects.acreate(public_key=validator_wallet.hotkey.ss58_address, active=True)

    # Insert some receipts
    base_good_receipt = JobStartedReceiptPayload(
        is_organic=True,
        job_uuid=str(uuid4()),
        executor_class=DEFAULT_EXECUTOR_CLASS,
        miner_hotkey=miner_wallet.hotkey.ss58_address,
        validator_hotkey=validator_wallet.hotkey.ss58_address,
        timestamp=timezone.now(),
        ttl=60,
    )
    synthetic_receipt = base_good_receipt.__replace__(is_organic=False)
    different_executor_class = base_good_receipt.__replace__(
        executor_class=next(c for c in ExecutorClass if c != DEFAULT_EXECUTOR_CLASS)
    )
    old_receipt = base_good_receipt.__replace__(timestamp=timezone.now() - timedelta(minutes=5))
    future_receipt = base_good_receipt.__replace__(timestamp=timezone.now() + timedelta(minutes=5))

    for payload in [
        base_good_receipt,
        synthetic_receipt,
        different_executor_class,
        old_receipt,
        future_receipt,
    ]:
        payload.job_uuid = str(uuid4())
        blob = payload.blob_for_signing()
        receipt = Receipt(
            payload=payload,
            validator_signature=sign_blob(validator_wallet.hotkey, blob),
            miner_signature=sign_blob(miner_wallet.hotkey, blob),
        )
        await JobStartedReceipt.from_receipt(receipt).asave()

    with (
        patch.object(executor_manager, "reserve_executor_class") as reserve_executor_class,
        patch.object(
            executor_manager, "wait_for_executor_reservation"
        ) as wait_for_executor_reservation,
    ):
        reserve_executor_class.side_effect = AllExecutorsBusy()
        wait_for_executor_reservation.side_effect = AllExecutorsBusy()

        async with fake_validator() as validator_channel:
            await authenticate(validator_channel)
            await validator_channel.receive_from(timeout=WEBSOCKET_TIMEOUT)  # the manifest

            await _send_initial_job_request(validator_channel, job_uuid)
            response = await validator_channel.receive_json_from(timeout=WEBSOCKET_TIMEOUT)

            assert response["message_type"] == "V0DeclineJobRequest"
            assert response["reason"] == JobRejectionReason.BUSY.value
            assert len(response["receipts"]) == 1
            assert response["receipts"][0]["payload"]["job_uuid"] == base_good_receipt.job_uuid
