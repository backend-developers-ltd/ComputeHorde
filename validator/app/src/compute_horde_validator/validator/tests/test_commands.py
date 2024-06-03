import io
from unittest.mock import patch

import pytest
from django.core import management

from compute_horde_validator.validator.models import Miner, OrganicJob

from .test_miner_driver import MockMinerClient
from .test_receipts import MockedAxonInfo


async def mock_get_miner_axon_info(hotkey: str):
    return MockedAxonInfo(is_serving=True, ip_type=4, ip="0000", port=8000)


@patch("compute_horde_validator.validator.tasks.get_miner_axon_info", mock_get_miner_axon_info)
@patch("compute_horde_validator.validator.tasks.MinerClient", MockMinerClient)
@pytest.mark.django_db
def test_command():
    # random miner to be picked
    Miner.objects.create(hotkey="miner_client")
    buf = io.StringIO()
    management.call_command(
        "debug_run_organic_job", docker_image="fu", timeout=4, cmd_args="", stdout=buf
    )
    buf.seek(0)
    output = buf.read()
    assert OrganicJob.objects.count() == 1
    assert "done processing" in output
    assert "Picked miner: hotkey: miner_client to run the job" in output
    # assert False
