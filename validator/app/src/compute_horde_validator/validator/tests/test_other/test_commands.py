import io
import logging
from contextlib import redirect_stdout
from unittest.mock import patch

import pytest
from django.core import management

from compute_horde_validator.validator.models import Miner, OrganicJob, SystemEvent

from ..helpers import (
    MockSuccessfulMinerClient,
    check_system_events,
    throw_error,
)

logger = logging.getLogger(__name__)


@pytest.fixture(autouse=True)
def patch_miner_client():
    with patch(
        "compute_horde_validator.validator.management.commands.debug_run_organic_job.MinerClient",
        MockSuccessfulMinerClient,
    ):
        yield


@pytest.fixture(autouse=True)
def patch_current_block():
    with patch(
        "compute_horde_validator.validator.allowance.utils.supertensor.get_current_block",
        return_value=1337,
    ):
        yield


@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
def test_debug_run_organic_job_command__job_completed(bittensor):
    # random miner to be picked
    Miner.objects.create(hotkey="miner_client")

    with redirect_stdout(io.StringIO()) as buf:
        management.call_command("debug_run_organic_job", docker_image="noop", cmd_args="")

    assert OrganicJob.objects.count() == 1
    assert OrganicJob.objects.first().status == OrganicJob.Status.COMPLETED

    output = buf.getvalue()
    assert "done processing" in output
    assert "status: completed" in output
    assert "Picked miner: hotkey: miner_client to run the job" in output

    check_system_events(
        SystemEvent.EventType.MINER_ORGANIC_JOB_SUCCESS, SystemEvent.EventSubType.SUCCESS, 1
    )


@patch(
    "compute_horde_validator.validator.management.commands.debug_run_organic_job.get_keypair",
    throw_error,
)
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
def test_debug_run_organic_job_command__job_created_but_not_triggered(bittensor):
    Miner.objects.create(hotkey="miner_client")

    with redirect_stdout(io.StringIO()) as buf, pytest.raises(SystemExit) as exit_info:
        management.call_command("debug_run_organic_job", docker_image="noop", cmd_args="")

    assert exit_info.value.code == 1

    assert OrganicJob.objects.count() == 1
    assert OrganicJob.objects.first().status == OrganicJob.Status.PENDING

    output = buf.getvalue()
    assert "Failed to run job" in output
    assert SystemEvent.objects.count() == 0
