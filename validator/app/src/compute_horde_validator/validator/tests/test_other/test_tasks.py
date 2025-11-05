from unittest.mock import MagicMock, patch

import pytest
from celery import exceptions as celery_exceptions
from django.conf import settings
from django.utils.timezone import now
from requests import Response
from web3.exceptions import ProviderConnectionError

from compute_horde_validator.validator.collateral.types import (
    NonceTooHighCollateralException,
    NonceTooLowCollateralException,
    ReplacementUnderpricedCollateralException,
    SlashCollateralError,
)
from compute_horde_validator.validator.models import (
    Miner,
    OrganicJob,
    SystemEvent,
)
from compute_horde_validator.validator.tasks import (
    send_events_to_facilitator,
    slash_collateral_task,
)


def add_system_events():
    events = SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS)
    events.create(
        type=SystemEvent.EventType.WEIGHT_SETTING_SUCCESS,
        subtype=SystemEvent.EventSubType.SUCCESS,
        data={},
        sent=False,
    )
    events.create(
        type=SystemEvent.EventType.WEIGHT_SETTING_FAILURE,
        subtype=SystemEvent.EventSubType.GENERIC_ERROR,
        data={},
        sent=False,
    )
    events.create(
        type=SystemEvent.EventType.WEIGHT_SETTING_FAILURE,
        subtype=SystemEvent.EventSubType.GENERIC_ERROR,
        data={},
        sent=True,
    )


def get_response(status):
    response = Response()
    response.status_code = status
    return response


@patch(
    "compute_horde_validator.validator.tasks.requests.post",
    lambda url, json, headers: get_response(201),
)
@pytest.mark.django_db(databases=["default", "default_alias"])
def test_send_events_to_facilitator__success():
    add_system_events()
    send_events_to_facilitator()
    assert SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).filter(sent=True).count() == 3


@patch(
    "compute_horde_validator.validator.tasks.requests.post",
    lambda url, json, headers: get_response(400),
)
@pytest.mark.django_db(databases=["default", "default_alias"])
def test_send_events_to_facilitator__failure():
    add_system_events()
    send_events_to_facilitator()
    assert SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).filter(sent=False).count() == 2


@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
@pytest.mark.parametrize(
    "exception",
    [
        NonceTooLowCollateralException("Nonce too low"),
        NonceTooHighCollateralException("Nonce too high"),
        ReplacementUnderpricedCollateralException("replacement transaction underpriced"),
        ProviderConnectionError("Failed to connect to RPC node"),
    ],
)
def test_slash_collateral_task_retriable_error_triggers_retry(settings, monkeypatch, exception):
    settings.CELERY_TASK_ALWAYS_EAGER = True
    settings.CELERY_TASK_EAGER_PROPAGATES = True

    miner = Miner.objects.create(hotkey="hotkey-slash-1")
    job = OrganicJob.objects.create(
        miner=miner,
        miner_address="127.0.0.1",
        miner_address_ip_version=4,
        miner_port=8000,
        cheated=True,
    )

    mock_collateral = MagicMock()
    mock_collateral.slash_collateral = MagicMock(side_effect=exception)
    monkeypatch.setattr(
        "compute_horde_validator.validator.tasks.collateral",
        lambda: mock_collateral,
    )

    with pytest.raises(celery_exceptions.Retry):
        slash_collateral_task.apply(args=(str(job.job_uuid),))

    job.refresh_from_db()
    assert job.slashed is False
    assert job.slashed_at is None


@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
def test_slash_collateral_task_non_retriable_error_no_retry(settings, monkeypatch):
    settings.CELERY_TASK_ALWAYS_EAGER = True
    settings.CELERY_TASK_EAGER_PROPAGATES = True

    miner = Miner.objects.create(hotkey="hotkey-slash-2")
    job = OrganicJob.objects.create(
        miner=miner,
        miner_address="127.0.0.1",
        miner_address_ip_version=4,
        miner_port=8001,
        cheated=True,
    )

    mock_collateral = MagicMock()
    mock_collateral.slash_collateral = MagicMock(
        side_effect=SlashCollateralError("some fatal error")
    )
    monkeypatch.setattr(
        "compute_horde_validator.validator.tasks.collateral",
        lambda: mock_collateral,
    )

    slash_collateral_task.apply(args=(str(job.job_uuid),))

    job.refresh_from_db()
    assert job.slashed is False
    assert job.slashed_at is None


@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
def test_slash_collateral_task_success_marks_slashed(settings, monkeypatch):
    settings.CELERY_TASK_ALWAYS_EAGER = True
    settings.CELERY_TASK_EAGER_PROPAGATES = True

    miner = Miner.objects.create(hotkey="hotkey-slash-3")
    job = OrganicJob.objects.create(
        miner=miner,
        miner_address="127.0.0.1",
        miner_address_ip_version=4,
        miner_port=8002,
        cheated=True,
    )

    # Mock successful slash
    mock_collateral = MagicMock()
    mock_collateral.slash_collateral = MagicMock(return_value=None)
    monkeypatch.setattr(
        "compute_horde_validator.validator.tasks.collateral",
        lambda: mock_collateral,
    )

    slash_collateral_task.apply(args=(str(job.job_uuid),))

    job.refresh_from_db()
    assert job.slashed is True
    assert job.slashed_at is not None


@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
def test_slash_collateral_task_already_slashed_returns_early(settings, monkeypatch):
    settings.CELERY_TASK_ALWAYS_EAGER = True
    settings.CELERY_TASK_EAGER_PROPAGATES = True

    miner = Miner.objects.create(hotkey="hotkey-slash-4")
    job = OrganicJob.objects.create(
        miner=miner,
        miner_address="127.0.0.1",
        miner_address_ip_version=4,
        miner_port=8003,
        cheated=True,
        slashed=True,
        slashed_at=now(),
    )

    mock_collateral = MagicMock()
    mock_collateral.slash_collateral = MagicMock(return_value=None)
    monkeypatch.setattr(
        "compute_horde_validator.validator.tasks.collateral",
        lambda: mock_collateral,
    )

    slash_collateral_task.apply(args=(str(job.job_uuid),))

    assert mock_collateral.slash_collateral.call_count == 0
