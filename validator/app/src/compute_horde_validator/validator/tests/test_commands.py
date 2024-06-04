import io
import sys
from unittest.mock import patch

import pytest
from django.core import management

from compute_horde_validator.validator.models import Miner, OrganicJob

from . import mock_get_miner_axon_info
from .test_miner_driver import MockMinerClient


@patch("compute_horde_validator.validator.tasks.get_miner_axon_info", mock_get_miner_axon_info)
@patch("compute_horde_validator.validator.tasks.MinerClient", MockMinerClient)
@pytest.mark.django_db
def test_command():
    # random miner to be picked
    Miner.objects.create(hotkey="miner_client")
    buf = io.StringIO()
    sys.stdout = buf
    management.call_command("debug_run_organic_job", docker_image="fu", timeout=4, cmd_args="")
    output = buf.getvalue()
    assert OrganicJob.objects.count() == 1
    assert "done processing" in output
    assert "Picked miner: hotkey: miner_client to run the job" in output
