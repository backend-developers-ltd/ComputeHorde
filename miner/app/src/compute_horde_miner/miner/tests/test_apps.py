import pytest
from django.core.management import call_command

from compute_horde_miner.miner.models import Validator

pytestmark = [pytest.mark.django_db]


def test_create_local_miner_validator(settings):
    settings.IS_LOCAL_MINER = True
    settings.LOCAL_MINER_VALIDATOR_PUBLIC_KEY = "test"

    call_command("migrate")

    validator = Validator.objects.get(public_key="test")
    assert validator.active
    assert not validator.debug


def test_create_local_miner_validator_no_public_key(settings):
    settings.IS_LOCAL_MINER = True
    settings.LOCAL_MINER_VALIDATOR_PUBLIC_KEY = ""

    with pytest.raises(AssertionError):
        call_command("migrate")


def test_create_local_miner_validator_non_local_mode(settings):
    settings.IS_LOCAL_MINER = False
    settings.LOCAL_MINER_VALIDATOR_PUBLIC_KEY = "test"

    call_command("migrate")

    assert not Validator.objects.filter(public_key="test").exists()
