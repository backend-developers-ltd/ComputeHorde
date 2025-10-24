from unittest.mock import patch

import pytest
from compute_horde import protocol_consts

from ..models import Job, JobStatus, Validator


@pytest.mark.django_db(transaction=True)
def test__job__selecting_validator__failure__not_exist(user, signature, keypair):
    with pytest.raises(Validator.DoesNotExist):
        Job.objects.create(
            user=user,
            validator=None,
            target_validator_hotkey=keypair.ss58_address,
            signature=signature.model_dump(),
        )


@pytest.mark.django_db(transaction=True)
def test__job__selecting_validator__failure__not_connected(user, validator, signature):
    """Check that new job is not assigned to a validator which exists but is not connected"""
    with pytest.raises(Validator.DoesNotExist):
        Job.objects.create(
            user=user,
            validator=None,
            target_validator_hotkey=validator.ss58_address,
            signature=signature.model_dump(),
        )


@pytest.mark.django_db(transaction=True)
def test__job__selecting_validator__failure__not_authorized(user, validator, signature):
    """Check that new job is not assigned to connected but not authorized validator"""
    with pytest.raises(Validator.DoesNotExist):
        Job.objects.create(
            user=user,
            validator=None,
            target_validator_hotkey=validator.ss58_address,
            signature=signature.model_dump(),
        )


@pytest.mark.django_db(transaction=True)
def test__job__selecting_validator__success(user, connected_validator, signature):
    """Check that new job is assigned to connected and authorized validator"""
    job = Job.objects.create(
        user=user,
        validator=None,
        target_validator_hotkey=connected_validator.ss58_address,
        signature=signature.model_dump(),
        download_time_limit=1,
        execution_time_limit=1,
        streaming_start_time_limit=1,
        upload_time_limit=1,
    )
    assert job.validator == connected_validator


@pytest.mark.django_db(transaction=True)
def test__job__send_to_validator_failure_rolls_back(user, connected_validator, signature):
    """Job creation should revert if sending to validator fails after commit."""
    with patch("project.core.models.Job.send_to_validator", side_effect=RuntimeError("validator down")):
        with pytest.raises(RuntimeError):
            Job.objects.create(
                user=user,
                validator=None,
                target_validator_hotkey=connected_validator.ss58_address,
                signature=signature.model_dump(),
                download_time_limit=1,
                execution_time_limit=1,
                streaming_start_time_limit=1,
                upload_time_limit=1,
            )

    assert Job.objects.count() == 0
    assert JobStatus.objects.count() == 0


@pytest.mark.django_db(transaction=True)
def test__job__dispatch_runs_after_commit(user, connected_validator, signature):
    """Validator dispatch must happen after the job row and status commit."""
    captured: dict[str, object] = {}

    def mock_send_to_validator(self, payload):
        captured["job_id"] = self.pk
        captured["job_exists"] = Job.objects.filter(pk=self.pk).exists()
        captured["statuses"] = list(JobStatus.objects.filter(job=self).values_list("status", flat=True))

    with patch.object(Job, "send_to_validator", autospec=True, side_effect=mock_send_to_validator):
        Job.objects.create(
            user=user,
            validator=None,
            target_validator_hotkey=connected_validator.ss58_address,
            signature=signature.model_dump(),
            download_time_limit=1,
            execution_time_limit=1,
            streaming_start_time_limit=1,
            upload_time_limit=1,
        )

    assert captured
    assert captured["job_exists"]
    assert captured["statuses"] == [protocol_consts.JobStatus.SENT.value]
