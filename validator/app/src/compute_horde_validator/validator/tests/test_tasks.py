from unittest.mock import patch

import pytest
from django.conf import settings
from requests import Response

from compute_horde_validator.validator.models import AdminJobRequest, Miner, OrganicJob, SystemEvent
from compute_horde_validator.validator.tasks import (
    send_events_to_facilitator,
    trigger_run_admin_job_request,
)

from . import mock_get_miner_axon_info, mock_keypair, throw_error
from .test_miner_driver import MockMinerClient


@patch("compute_horde_validator.validator.tasks.get_miner_axon_info", mock_get_miner_axon_info)
@patch("compute_horde_validator.validator.tasks.get_keypair", mock_keypair)
@patch("compute_horde_validator.validator.tasks.MinerClient", MockMinerClient)
@pytest.mark.django_db
def test_trigger_run_admin_job__should_trigger_job():
    miner = Miner.objects.create(hotkey="miner_client_1")
    OrganicJob.objects.all().delete()
    job_request = AdminJobRequest.objects.create(
        miner=miner,
        timeout=0,  # should timeout
        docker_image="python:3.11-slim",
        raw_script="print('hello world')",
        args="",
    )

    assert AdminJobRequest.objects.count() == 1
    trigger_run_admin_job_request.delay(job_request.pk)

    job_request.refresh_from_db()
    assert job_request.status_message == "Job successfully triggered"

    assert OrganicJob.objects.count() == 1
    job = OrganicJob.objects.filter(job_uuid=job_request.uuid).first()
    assert job.comment == "Miner timed out while preparing executor"
    assert job.status == OrganicJob.Status.FAILED


@patch("compute_horde_validator.validator.tasks.get_miner_axon_info", throw_error)
@patch("compute_horde_validator.validator.tasks.get_keypair", mock_keypair)
@patch("compute_horde_validator.validator.tasks.MinerClient", MockMinerClient)
@pytest.mark.django_db
def test_trigger_run_admin_job__should_not_trigger_job():
    miner = Miner.objects.create(hotkey="miner_client_2")
    OrganicJob.objects.all().delete()
    job_request = AdminJobRequest.objects.create(
        miner=miner,
        timeout=0,  # should timeout
        docker_image="python:3.11-slim",
        raw_script="print('hello world')",
        args="",
    )

    assert AdminJobRequest.objects.count() == 1
    trigger_run_admin_job_request.delay(job_request.pk)

    job_request.refresh_from_db()
    assert "Job failed to trigger" in job_request.status_message

    assert OrganicJob.objects.count() == 0


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


@patch("compute_horde_validator.validator.tasks.requests.post", lambda url, json: get_response(201))
@patch("compute_horde_validator.validator.tasks.get_keypair", mock_keypair)
@pytest.mark.django_db(databases=["default", "default_alias"])
def test_send_events_to_facilitator__success():
    add_system_events()
    send_events_to_facilitator()
    assert SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).filter(sent=True).count() == 3


@patch("compute_horde_validator.validator.tasks.requests.post", lambda url, json: get_response(400))
@patch("compute_horde_validator.validator.tasks.get_keypair", mock_keypair)
@pytest.mark.django_db(databases=["default", "default_alias"])
def test_send_events_to_facilitator__failure():
    add_system_events()
    send_events_to_facilitator()
    assert SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).filter(sent=False).count() == 2
