import uuid
from unittest.mock import patch

import numpy as np
import pytest
from django.conf import settings
from django.utils.timezone import now

from compute_horde_validator.validator.models import (
    Miner,
    OrganicJob,
    SyntheticJob,
    SyntheticJobBatch,
    SystemEvent,
)
from compute_horde_validator.validator.tasks import set_scores

from . import MockWallet

NUM_NEURONS = 5


class MockSubtensor:
    def __init__(
        self,
        *args,
        mocked_set_weights=lambda: (True, ""),
        mocked_metagraph=lambda: MockMetagraph(),
    ):
        self.mocked_set_weights = mocked_set_weights
        self.mocked_metagraph = mocked_metagraph

    def min_allowed_weights(self, netuid):
        return 0

    def max_weight_limit(self, netuid):
        return 99999

    def get_subnet_hyperparameters(self, *args):
        return None

    def metagraph(self, netuid):
        return self.mocked_metagraph()

    def set_weights(
        self, wallet, netuid, uids, weights, version_key, wait_for_inclusion, wait_for_finalization
    ) -> tuple[bool, str]:
        return self.mocked_set_weights()


class MockNeuron:
    def __init__(self, hotkey, uid):
        self.hotkey = hotkey
        self.uid = uid


class MockMetagraph:
    def __init__(self, netuid=1, num_neurons=NUM_NEURONS):
        self.n = num_neurons
        self.netuid = netuid
        self.num_neurons = num_neurons
        self.W = np.ones((num_neurons, num_neurons))
        self.hotkeys = [f"hotkey_{i}" for i in range(num_neurons)]
        self.uids = np.array(list(range(num_neurons)))
        self.neurons = [MockNeuron(f"hotkey_{i}", i) for i in range(NUM_NEURONS)]


def setup_db():
    miners = [Miner.objects.create(hotkey=f"hotkey_{i}") for i in range(NUM_NEURONS)]

    job_batch = SyntheticJobBatch.objects.create(
        started_at=now(),
        accepting_results_until=now(),
        scored=False,
    )
    for i in range(NUM_NEURONS):
        SyntheticJob.objects.create(
            batch=job_batch,
            score=0,
            job_uuid=uuid.uuid4(),
            miner=miners[i],
            miner_address="ignore",
            miner_address_ip_version=4,
            miner_port=9999,
            status=OrganicJob.Status.COMPLETED,
        )


def throw_error(*args):
    raise Exception("Error thrown for testing")


@patch("bittensor.subtensor", lambda *args, **kwargs: MockSubtensor())
@pytest.mark.django_db(databases=["default", "default_alias"])
def test_set_scores__no_batches_found():
    set_scores()
    assert SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).count() == 0


@patch("bittensor.subtensor", lambda *args, **kwargs: MockSubtensor())
@patch("django.conf.settings.BITTENSOR_WALLET", lambda *args, **kwargs: MockWallet())
@pytest.mark.django_db(databases=["default", "default_alias"])
def test_set_scores__set_weight_success():
    setup_db()
    set_scores()
    system_events = SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS)
    assert system_events.count() == 1
    assert (
        system_events.filter(
            type=SystemEvent.EventType.WEIGHT_SETTING_SUCCESS,
            subtype=SystemEvent.EventSubType.SUCCESS,
        ).count()
        == 1
    )


@patch("compute_horde_validator.validator.tasks.WEIGHT_SETTING_ATTEMPTS", 1)
@patch("compute_horde_validator.validator.tasks.WEIGHT_SETTING_FAILURE_BACKOFF", 0)
@patch(
    "bittensor.subtensor",
    lambda *args, **kwargs: MockSubtensor(mocked_set_weights=lambda: (False, "error")),
)
@patch("django.conf.settings.BITTENSOR_WALLET", lambda *args, **kwargs: MockWallet())
@pytest.mark.django_db(databases=["default", "default_alias"])
def test_set_scores__set_weight_failure():
    setup_db()
    set_scores()
    system_events = SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS)
    assert system_events.count() == 2
    assert (
        system_events.filter(
            type=SystemEvent.EventType.WEIGHT_SETTING_FAILURE,
            subtype=SystemEvent.EventSubType.WRITING_TO_CHAIN_FAILED,
        ).count()
        == 1
    )
    # end of retries system event
    assert (
        system_events.filter(
            type=SystemEvent.EventType.WEIGHT_SETTING_FAILURE,
            subtype=SystemEvent.EventSubType.GENERIC_ERROR,
        ).count()
        == 1
    )


def set_weights_succeed_third_time():
    global weight_set_attempts
    weight_set_attempts += 1
    return (False, "error") if weight_set_attempts < 3 else (True, "")


@patch("compute_horde_validator.validator.tasks.WEIGHT_SETTING_ATTEMPTS", 3)
@patch("compute_horde_validator.validator.tasks.WEIGHT_SETTING_FAILURE_BACKOFF", 0)
@patch(
    "bittensor.subtensor",
    lambda *args, **kwargs: MockSubtensor(mocked_set_weights=set_weights_succeed_third_time),
)
@patch("django.conf.settings.BITTENSOR_WALLET", lambda *args, **kwargs: MockWallet())
@pytest.mark.django_db(databases=["default", "default_alias"])
def test_set_scores__set_weight_eventual_success():
    global weight_set_attempts
    weight_set_attempts = 0
    setup_db()
    set_scores()
    system_events = SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS)
    assert system_events.count() == 3
    assert (
        system_events.filter(
            type=SystemEvent.EventType.WEIGHT_SETTING_FAILURE,
            subtype=SystemEvent.EventSubType.WRITING_TO_CHAIN_FAILED,
        ).count()
        == 2
    )
    assert (
        system_events.filter(
            type=SystemEvent.EventType.WEIGHT_SETTING_SUCCESS,
            subtype=SystemEvent.EventSubType.SUCCESS,
        ).count()
        == 1
    )


@patch("compute_horde_validator.validator.tasks.WEIGHT_SETTING_ATTEMPTS", 1)
@patch("compute_horde_validator.validator.tasks.WEIGHT_SETTING_FAILURE_BACKOFF", 0)
@patch("bittensor.subtensor", lambda *args, **kwargs: MockSubtensor(mocked_set_weights=throw_error))
@patch("django.conf.settings.BITTENSOR_WALLET", lambda *args, **kwargs: MockWallet())
@pytest.mark.django_db(databases=["default", "default_alias"])
def test_set_scores__set_weight_exception():
    setup_db()
    set_scores()
    system_events = SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS)
    assert system_events.count() == 2
    assert (
        system_events.filter(
            type=SystemEvent.EventType.WEIGHT_SETTING_FAILURE,
            subtype=SystemEvent.EventSubType.WRITING_TO_CHAIN_GENERIC_ERROR,
        ).count()
        == 1
    )
    # end of retries system event
    assert (
        system_events.filter(
            type=SystemEvent.EventType.WEIGHT_SETTING_FAILURE,
            subtype=SystemEvent.EventSubType.GENERIC_ERROR,
        ).count()
        == 1
    )


@patch("compute_horde_validator.validator.tasks.WEIGHT_SETTING_ATTEMPTS", 1)
@patch("compute_horde_validator.validator.tasks.WEIGHT_SETTING_FAILURE_BACKOFF", 0)
@patch("compute_horde_validator.validator.tasks.WEIGHT_SETTING_HARD_TTL", 1)
@patch("compute_horde_validator.validator.tasks.WEIGHT_SETTING_TTL", 1)
@patch("bittensor.subtensor", lambda *args, **kwargs: MockSubtensor())
@patch("django.conf.settings.BITTENSOR_WALLET", lambda *args, **kwargs: MockWallet())
@pytest.mark.django_db(databases=["default", "default_alias"])
def test_set_scores__set_weight_timeout():
    settings.CELERY_TASK_ALWAYS_EAGER = False  # to make it timeout
    setup_db()
    set_scores()
    system_events = SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS)
    assert system_events.count() == 2
    assert (
        system_events.filter(
            type=SystemEvent.EventType.WEIGHT_SETTING_FAILURE,
            subtype=SystemEvent.EventSubType.WRITING_TO_CHAIN_TIMEOUT,
        ).count()
        == 1
    )
    # end of retries system event
    assert (
        system_events.filter(
            type=SystemEvent.EventType.WEIGHT_SETTING_FAILURE,
            subtype=SystemEvent.EventSubType.GENERIC_ERROR,
        ).count()
        == 1
    )


@patch("compute_horde_validator.validator.tasks.WEIGHT_SETTING_ATTEMPTS", 1)
@patch("compute_horde_validator.validator.tasks.WEIGHT_SETTING_FAILURE_BACKOFF", 0)
@patch("bittensor.subtensor", lambda *args, **kwargs: MockSubtensor(mocked_metagraph=throw_error))
@patch("django.conf.settings.BITTENSOR_WALLET", lambda *args, **kwargs: MockWallet())
@pytest.mark.django_db(databases=["default", "default_alias"])
def test_set_scores__metagraph_fetch_exception():
    setup_db()
    set_scores()
    system_events = SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS)
    assert system_events.count() == 1
    assert (
        system_events.filter(
            type=SystemEvent.EventType.WEIGHT_SETTING_FAILURE,
            subtype=SystemEvent.EventSubType.GENERIC_ERROR,
        ).count()
        == 1
    )
