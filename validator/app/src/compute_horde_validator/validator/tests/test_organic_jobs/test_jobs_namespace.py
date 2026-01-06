import uuid
from unittest.mock import patch

import pytest

from compute_horde_validator.validator.allowance.types import Miner as AllowanceMiner
from compute_horde_validator.validator.models import Miner
from compute_horde_validator.validator.organic_jobs.miner_driver import execute_organic_job_request
from compute_horde_validator.validator.routing.types import JobRoute
from compute_horde_validator.validator.tests.helpers import get_dummy_job_request_v2


@pytest.mark.django_db
@pytest.mark.parametrize(
    "job_namespace,namespace_value", [("SN123.1.0", "SN123.1.0"), ("", "docker_image")]
)
def test_organic_job_namespace_priority(job_namespace, namespace_value):
    """
    Test OrganicJob uses namespace with fallback to docker_image.
    """
    miner_model = Miner.objects.create(
        hotkey=f"test-miner-{str(uuid.uuid4())[:8]}",
        address="127.0.0.1",
        port=8000,
        ip_version=4,
    )

    miner = AllowanceMiner(
        address=miner_model.address,
        port=miner_model.port,
        ip_version=miner_model.ip_version,
        hotkey_ss58=miner_model.hotkey,
    )
    job_route = JobRoute(
        miner=miner, allowance_reservation_id=None, allowance_blocks=None, allowance_job_value=None
    )

    job_request = get_dummy_job_request_v2(uuid=str(uuid.uuid4()))
    job_request.job_namespace = job_namespace

    # Namespace should fallback to docker_image
    job_request.docker_image = "docker_image"

    with patch(
        "compute_horde_validator.validator.organic_jobs.miner_driver._get_current_block"
    ) as mock_block:
        mock_block.return_value = 1000
        with patch(
            "compute_horde_validator.validator.organic_jobs.miner_driver.drive_organic_job"
        ) as mock_drive:
            mock_drive.return_value = True

            job = execute_organic_job_request(job_request, job_route)
            assert job.namespace == namespace_value
