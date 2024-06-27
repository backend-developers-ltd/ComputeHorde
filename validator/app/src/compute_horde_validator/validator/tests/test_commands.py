import io
import logging
from contextlib import redirect_stdout
from unittest.mock import patch

import pytest
from django.core import management

from compute_horde_validator.validator.models import AdminJobRequest, Miner, OrganicJob, SystemEvent

from .helpers import (
    MockMinerClient,
    check_system_events,
    mock_get_miner_axon_info,
    throw_error,
)
from .test_facilitator_client import MockJobStateMinerClient

logger = logging.getLogger(__name__)


@patch("compute_horde_validator.validator.tasks.get_miner_axon_info", mock_get_miner_axon_info)
@patch("compute_horde_validator.validator.tasks.MinerClient", MockJobStateMinerClient)
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
def test_debug_run_organic_job_command__job_completed():
    # random miner to be picked
    Miner.objects.create(hotkey="miner_client")

    with redirect_stdout(io.StringIO()) as buf:
        management.call_command(
            "debug_run_organic_job", docker_image="noop", timeout=4, cmd_args=""
        )

    assert AdminJobRequest.objects.count() == 1
    assert AdminJobRequest.objects.first().status_message == "Job successfully triggered"

    assert OrganicJob.objects.count() == 1
    assert OrganicJob.objects.first().status == OrganicJob.Status.COMPLETED

    output = buf.getvalue()
    assert "done processing" in output
    assert "status: completed" in output
    assert "Picked miner: hotkey: miner_client to run the job" in output

    check_system_events(
        SystemEvent.EventType.MINER_ORGANIC_JOB_SUCCESS, SystemEvent.EventSubType.SUCCESS, 1
    )


@patch("compute_horde_validator.validator.tasks.get_miner_axon_info", mock_get_miner_axon_info)
@patch("compute_horde_validator.validator.tasks.MinerClient", MockMinerClient)
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
def test_debug_run_organic_job_command__job_timeout():
    # random miner to be picked
    Miner.objects.create(hotkey="miner_client")

    with redirect_stdout(io.StringIO()) as buf:
        management.call_command(
            "debug_run_organic_job", docker_image="noop", timeout=0, cmd_args=""
        )

    assert AdminJobRequest.objects.count() == 1
    assert AdminJobRequest.objects.first().status_message == "Job successfully triggered"

    assert OrganicJob.objects.count() == 1
    assert OrganicJob.objects.first().status == OrganicJob.Status.FAILED

    output = buf.getvalue()
    assert "done processing" in output
    assert "status: failed" in output
    assert "timed out" in output
    assert "Picked miner: hotkey: miner_client to run the job" in output

    check_system_events(
        SystemEvent.EventType.MINER_ORGANIC_JOB_FAILURE,
        SystemEvent.EventSubType.JOB_NOT_STARTED,
        1,
    )


@patch("compute_horde_validator.validator.tasks.get_miner_axon_info", throw_error)
@patch("compute_horde_validator.validator.tasks.MinerClient", MockJobStateMinerClient)
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
def test_debug_run_organic_job_command__job_not_created():
    Miner.objects.create(hotkey="miner_client")

    with redirect_stdout(io.StringIO()) as buf:
        with pytest.raises(BaseException):
            management.call_command(
                "debug_run_organic_job", docker_image="noop", timeout=4, cmd_args=""
            )

    assert AdminJobRequest.objects.count() == 1
    assert "Job failed to trigger due to" in AdminJobRequest.objects.first().status_message

    assert OrganicJob.objects.count() == 0

    output = buf.getvalue()
    assert "not found" in output
    assert SystemEvent.objects.count() == 0


@patch("compute_horde_validator.validator.tasks.get_keypair", throw_error)
@patch("compute_horde_validator.validator.tasks.get_miner_axon_info", mock_get_miner_axon_info)
@patch("compute_horde_validator.validator.tasks.MinerClient", MockJobStateMinerClient)
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
def test_debug_run_organic_job_command__job_created_but_not_triggered():
    Miner.objects.create(hotkey="miner_client")

    with redirect_stdout(io.StringIO()) as buf:
        management.call_command(
            "debug_run_organic_job", docker_image="noop", timeout=4, cmd_args=""
        )

    assert AdminJobRequest.objects.count() == 1
    assert "Job failed to trigger" in AdminJobRequest.objects.first().status_message

    assert OrganicJob.objects.count() == 1
    assert OrganicJob.objects.first().status == OrganicJob.Status.PENDING

    output = buf.getvalue()
    assert "done processing" in output
    assert SystemEvent.objects.count() == 0
