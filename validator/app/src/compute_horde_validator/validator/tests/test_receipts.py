import uuid
from typing import NamedTuple

import pytest
from compute_horde.executor_class import DEFAULT_EXECUTOR_CLASS
from compute_horde.mv_protocol.validator_requests import (
    JobFinishedReceiptPayload,
    JobStartedReceiptPayload,
)
from compute_horde.receipts.models import JobFinishedReceipt, JobStartedReceipt
from compute_horde.receipts.schemas import Receipt
from django.utils.timezone import now

from compute_horde_validator.validator.models import (
    SystemEvent,
)
from compute_horde_validator.validator.tasks import fetch_receipts

from .helpers import MockedAxonInfo, check_system_events, throw_error


class MockedNeuron(NamedTuple):
    hotkey: str
    axon_info: MockedAxonInfo


class MockedMetagraph:
    def __init__(self, *args, **kwargs):
        self.neurons = [
            MockedNeuron(
                hotkey="5G9qWBzLPVVu2fCPPvg3QgPPK5JaJmJKaJha95TPHH9NZWuL",
                axon_info=MockedAxonInfo(is_serving=True, ip="127.0.0.1", ip_type=4, port=8000),
            ),
            MockedNeuron(
                hotkey="5CPhGRp4cdEG4KSui7VQixHhvN5eBUSnMYeUF5thdxm4sKtz",
                axon_info=MockedAxonInfo(is_serving=True, ip="127.0.0.2", ip_type=4, port=8000),
            ),
        ]


def mocked_get_miner_receipts(hotkey: str, ip: str, port: int) -> list[Receipt]:
    if hotkey == "5G9qWBzLPVVu2fCPPvg3QgPPK5JaJmJKaJha95TPHH9NZWuL":
        return [
            Receipt(
                payload=JobStartedReceiptPayload(
                    job_uuid=str(uuid.uuid4()),
                    miner_hotkey="5G9qWBzLPVVu2fCPPvg3QgPPK5JaJmJKaJha95TPHH9NZWuL",
                    validator_hotkey="v1",
                    executor_class=DEFAULT_EXECUTOR_CLASS,
                    time_accepted=now(),
                    max_timeout=30,
                    is_organic=False,
                ),
                validator_signature="0xv1",
                miner_signature="0xm1",
            )
        ]
    elif hotkey == "5CPhGRp4cdEG4KSui7VQixHhvN5eBUSnMYeUF5thdxm4sKtz":
        return [
            Receipt(
                payload=JobFinishedReceiptPayload(
                    job_uuid=str(uuid.uuid4()),
                    miner_hotkey="5CPhGRp4cdEG4KSui7VQixHhvN5eBUSnMYeUF5thdxm4sKtz",
                    validator_hotkey="v1",
                    time_started=now(),
                    time_took_us=30_000_000,
                    score_str="123.45",
                ),
                validator_signature="0xv1",
                miner_signature="0xm1",
            )
        ]
    else:
        return []


@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
def test_fetch_receipts__success(monkeypatch):
    monkeypatch.setattr("bittensor.metagraph", MockedMetagraph)
    monkeypatch.setattr(
        "compute_horde_validator.validator.tasks.get_miner_receipts", mocked_get_miner_receipts
    )
    fetch_receipts()
    assert JobStartedReceipt.objects.count() == 1
    assert JobFinishedReceipt.objects.count() == 1


@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
def test_fetch_receipts__fail(monkeypatch):
    monkeypatch.setattr("bittensor.metagraph", MockedMetagraph)
    monkeypatch.setattr("compute_horde_validator.validator.tasks.get_miner_receipts", throw_error)
    fetch_receipts()
    assert JobStartedReceipt.objects.count() == 0
    assert JobFinishedReceipt.objects.count() == 0
    check_system_events(
        SystemEvent.EventType.RECEIPT_FAILURE, SystemEvent.EventSubType.RECEIPT_FETCH_ERROR, 2
    )
