from unittest.mock import patch

import pytest

from compute_horde_validator.validator.models import AdminJobRequest, Miner, OrganicJob
from compute_horde_validator.validator.tasks import trigger_run_admin_job_request

from . import mock_get_miner_axon_info, mock_keypair
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
    assert job_request.status_message == "job successfully triggered"

    assert OrganicJob.objects.count() == 1
    job = OrganicJob.objects.filter(job_uuid=job_request.uuid).first()
    assert job.comment == "Miner timed out while preparing executor"
    assert job.status == OrganicJob.Status.FAILED


async def throw_error(*args):
    raise Exception("timeout axon fetching")


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
    assert "job failed to trigger" in job_request.status_message

    assert OrganicJob.objects.count() == 0
