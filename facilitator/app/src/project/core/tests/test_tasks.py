from typing import NamedTuple
from unittest import mock

import pytest
from asgiref.sync import sync_to_async
from compute_horde import protocol_consts

from ..models import Channel, JobStatus, Validator
from ..tasks import send_job_to_validator_task, sync_metagraph


class MockedAxonInfo(NamedTuple):
    is_serving: bool
    ip: str = ""
    port: int = 0


class MockedNeuron(NamedTuple):
    uid: int
    hotkey: str
    axon_info: MockedAxonInfo
    stake: float


class MockedMetagraph:
    def __init__(self, neurons):
        self.neurons = neurons
        self.total_stake = [n.stake for n in neurons]


class MockedSubtensor:
    def __init__(self, metagraph: MockedMetagraph):
        self._metagraph = metagraph

    def __call__(self, *args, **kwargs):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        pass

    def metagraph(self, *args, **kwargs):
        return self._metagraph


validator_params = dict(
    axon_info=MockedAxonInfo(is_serving=False),
    stake=1000.0,
)

miner_params = dict(
    axon_info=MockedAxonInfo(is_serving=True),
    stake=0.0,
)


@pytest.mark.django_db(transaction=True)
def test__sync_metagraph__activation(monkeypatch):
    import bittensor

    validators = Validator.objects.bulk_create(
        [
            Validator(ss58_address="remains_active", is_active=True),
            Validator(ss58_address="is_deactivated", is_active=True),
            Validator(ss58_address="remains_inactive", is_active=False),
            Validator(ss58_address="is_activated", is_active=False),
        ]
    )

    metagraph = MockedMetagraph(
        neurons=[
            MockedNeuron(uid=0, hotkey="remains_active", **validator_params),
            MockedNeuron(uid=1, hotkey="is_deactivated", **miner_params),
            MockedNeuron(uid=2, hotkey="remains_inactive", **miner_params),
            MockedNeuron(uid=3, hotkey="is_activated", **validator_params),
            MockedNeuron(uid=4, hotkey="new_validator", **validator_params),
            MockedNeuron(uid=5, hotkey="new_miner", **miner_params),
        ]
    )
    subtensor = MockedSubtensor(metagraph=metagraph)

    with monkeypatch.context() as mp:
        mp.setattr(bittensor, "subtensor", subtensor)
        sync_metagraph()

    validators = Validator.objects.order_by("id").values_list("ss58_address", "is_active")
    assert list(validators) == [
        tuple(d.values())
        for d in [
            dict(ss58_address="remains_active", is_active=True),
            dict(ss58_address="is_deactivated", is_active=False),
            dict(ss58_address="remains_inactive", is_active=False),
            dict(ss58_address="is_activated", is_active=True),
            dict(ss58_address="new_validator", is_active=True),
        ]
    ]


@pytest.mark.asyncio
@pytest.mark.django_db(transaction=True)
async def test__websocket__disconnect_validator_if_become_inactive(
    monkeypatch,
    communicator,
    authenticated,
    validator,
    job,
    dummy_job_params,
):
    """Check that validator is disconnected if it becomes inactive"""
    import bittensor

    await communicator.receive_json_from()
    assert await Channel.objects.filter(validator=validator).aexists()

    metagraph = MockedMetagraph(neurons=[MockedNeuron(uid=0, hotkey=validator.ss58_address, **miner_params)])
    subtensor = MockedSubtensor(metagraph=metagraph)

    with monkeypatch.context() as mp:
        mp.setattr(bittensor, "subtensor", subtensor)
        await sync_to_async(sync_metagraph)()

    assert (await communicator.receive_output())["type"] == "websocket.close"
    assert not await Channel.objects.filter(validator=validator).aexists()


@pytest.mark.django_db
def test_send_job_to_validator_task_success(settings, job):
    settings.CELERY_TASK_ALWAYS_EAGER = True

    JobStatus.objects.create(job=job, status=protocol_consts.JobStatus.PENDING.value)

    with mock.patch("project.core.models.Job.send_to_validator") as mock_send:
        send_job_to_validator_task.apply_async(args=[job.uuid])

    mock_send.assert_called_once()

    assert JobStatus.objects.filter(job=job).count() == 2
    assert job.status.status == protocol_consts.JobStatus.SENT.value


@pytest.mark.django_db
@pytest.mark.parametrize(
    "status", [status.value for status in protocol_consts.JobStatus if status != protocol_consts.JobStatus.PENDING]
)
def test_send_job_to_validator_task_skips_if_not_pending(status, job, settings):
    settings.CELERY_TASK_ALWAYS_EAGER = True

    JobStatus.objects.create(job=job, status=status)

    with mock.patch("project.core.models.Job.send_to_validator") as mock_send:
        send_job_to_validator_task.apply_async(args=[job.uuid])

    mock_send.assert_not_called()

    assert JobStatus.objects.filter(job=job).count() == 1
    assert job.status.status == status


@pytest.mark.django_db
def test_send_job_to_validator_task_retry(settings, job):
    settings.CELERY_TASK_ALWAYS_EAGER = True

    JobStatus.objects.create(job=job, status=protocol_consts.JobStatus.PENDING.value)

    with mock.patch(
        "project.core.models.Job.send_to_validator", side_effect=[Exception("Network Error"), None]
    ) as mock_send:
        send_job_to_validator_task.apply_async(args=[job.uuid])

    assert mock_send.call_count == 2

    assert JobStatus.objects.filter(job=job).count() == 2
    assert job.status.status == protocol_consts.JobStatus.SENT.value


@pytest.mark.django_db
def test_send_job_to_validator_task_max_retries_exceeded(settings, job):
    settings.CELERY_TASK_ALWAYS_EAGER = True

    JobStatus.objects.create(job=job, status=protocol_consts.JobStatus.PENDING.value)

    with mock.patch(
        "project.core.models.Job.send_to_validator", side_effect=Exception("Persistent Error")
    ) as mock_send:
        send_job_to_validator_task.apply_async(args=[job.uuid])

    assert mock_send.call_count == 6

    assert JobStatus.objects.filter(job=job).count() == 2
    assert job.status.status == protocol_consts.JobStatus.HORDE_FAILED.value


@pytest.mark.django_db
def test_send_job_to_validator_task_job_does_not_exist(settings):
    settings.CELERY_TASK_ALWAYS_EAGER = True

    non_existent_uuid = "00000000-0000-0000-0000-000000000000"

    with mock.patch("project.core.models.Job.send_to_validator") as mock_send:
        send_job_to_validator_task.apply_async(args=[non_existent_uuid])

    mock_send.assert_not_called()
