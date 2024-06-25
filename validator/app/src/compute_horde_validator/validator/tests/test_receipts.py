import uuid
from typing import NamedTuple

import pytest
from compute_horde.mv_protocol.validator_requests import ReceiptPayload
from compute_horde.receipts import Receipt
from django.utils.timezone import now

from compute_horde_validator.validator.models import JobReceipt
from compute_horde_validator.validator.tasks import fetch_receipts

from .helpers import MockedAxonInfo


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
                payload=ReceiptPayload(
                    job_uuid=str(uuid.uuid4()),
                    miner_hotkey="5G9qWBzLPVVu2fCPPvg3QgPPK5JaJmJKaJha95TPHH9NZWuL",
                    validator_hotkey="v1",
                    time_started=now(),
                    time_took_us=30_000_000,
                    score_str="123.45",
                ),
                validator_signature="0xv1",
                miner_signature="0xm1",
            )
        ]
    elif hotkey == "5CPhGRp4cdEG4KSui7VQixHhvN5eBUSnMYeUF5thdxm4sKtz":
        return [
            Receipt(
                payload=ReceiptPayload(
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


@pytest.mark.django_db
def test_fetch_receipts(monkeypatch):
    monkeypatch.setattr("bittensor.metagraph", MockedMetagraph)
    monkeypatch.setattr(
        "compute_horde_validator.validator.tasks.get_miner_receipts", mocked_get_miner_receipts
    )
    fetch_receipts()
    assert JobReceipt.objects.count() == 2
