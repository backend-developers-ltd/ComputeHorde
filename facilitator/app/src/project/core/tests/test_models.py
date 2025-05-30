import pytest

from ..models import Job, Validator


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
