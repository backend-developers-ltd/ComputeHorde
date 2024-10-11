import uuid

import pytest
from compute_horde.executor_class import DEFAULT_EXECUTOR_CLASS
from compute_horde.mv_protocol.validator_requests import (
    JobFinishedReceiptPayload,
    JobStartedReceiptPayload,
)
from compute_horde.receipts.models import JobFinishedReceipt, JobStartedReceipt
from compute_horde.receipts.schemas import Receipt
from django.utils.timezone import now
from pytest_mock import MockerFixture

from compute_horde_miner.miner.tasks import announce_address_and_port, get_receipts_from_old_miner


@pytest.mark.django_db
@pytest.mark.override_config(SERVING=False)
def test__migration__not_serving__should_not_announce_address(mocker: MockerFixture):
    announce_mock = mocker.patch(
        "compute_horde_miner.miner.tasks.quasi_axon.announce_address_and_port"
    )

    announce_address_and_port()

    assert announce_mock.call_count == 0
    assert len(announce_mock.method_calls) == 0


@pytest.mark.django_db
@pytest.mark.override_config(MIGRATING=False, OLD_MINER_IP="127.0.0.1")
def test__migration__not_migrating__should_not_get_receipts_from_old_miner(mocker: MockerFixture):
    get_miner_receipts_mock = mocker.patch("compute_horde_miner.miner.tasks.get_miner_receipts")

    get_receipts_from_old_miner()

    assert get_miner_receipts_mock.call_count == 0
    assert len(get_miner_receipts_mock.method_calls) == 0


@pytest.mark.django_db
@pytest.mark.override_config(SERVING=False, MIGRATING=True, OLD_MINER_IP="127.0.0.1")
def test_get_receipts_from_old_miner(mocker: MockerFixture):
    receipts = [
        Receipt(
            payload=JobStartedReceiptPayload(
                job_uuid=str(uuid.uuid4()),
                miner_hotkey="m1",
                validator_hotkey="v1",
                executor_class=DEFAULT_EXECUTOR_CLASS,
                time_accepted=now(),
                max_timeout=30,
            ),
            validator_signature="0xv1",
            miner_signature="0xm1",
        ),
        Receipt(
            payload=JobFinishedReceiptPayload(
                job_uuid=str(uuid.uuid4()),
                miner_hotkey="m1",
                validator_hotkey="v2",
                time_started=now(),
                time_took_us=35_000_000,
                score_str="103.45",
            ),
            validator_signature="0xv2",
            miner_signature="0xm2",
        ),
    ]
    mocker.patch("compute_horde_miner.miner.tasks.get_miner_receipts", return_value=receipts)
    mocker.patch("compute_horde_miner.miner.tasks.settings.BITTENSOR_WALLET")

    get_receipts_from_old_miner()

    assert JobStartedReceipt.objects.count() == 1
    assert JobFinishedReceipt.objects.count() == 1
