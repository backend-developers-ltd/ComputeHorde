from collections.abc import Iterable
from unittest.mock import MagicMock, patch

import pytest
from faker import Faker

from compute_horde.validators.tasks import fetch_validators
from tests.test_app.models import Validator, Validator2


@pytest.fixture(autouse=True)
def mock_get_validators():
    with patch("compute_horde.validators.tasks.get_validators") as p:
        yield p


@pytest.fixture
def active_validators_keys(faker: Faker):
    return [faker.pystr() for _ in range(5)]


@pytest.fixture
def inactive_validators_keys(faker: Faker):
    return [faker.pystr() for _ in range(4)]


def _create_validators(keys: Iterable[str]):
    validator_models = [
        (Validator, "public_key", "active"),
        (Validator2, "ss58_address", "is_active"),
    ]
    for model, key_field, active_field in validator_models:
        model.objects.bulk_create([model(**{key_field: key, active_field: True}) for key in keys])


@pytest.fixture(autouse=True)
def active_validators(active_validators_keys: list[str]):
    _create_validators(active_validators_keys)


@pytest.fixture(autouse=True)
def inactive_validators(inactive_validators_keys: list[str]):
    _create_validators(inactive_validators_keys)


def _generate_validator_mocks(hotkeys: Iterable[str]):
    return [MagicMock(hotkey=key) for key in hotkeys]


@pytest.mark.django_db(transaction=True)
def test_fetch_validators_creates_new_active_validators(
    faker: Faker, mock_get_validators: MagicMock
):
    new_keys = [faker.pystr() for _ in range(3)]
    mock_get_validators.return_value = _generate_validator_mocks(new_keys)

    fetch_validators()

    for key in new_keys:
        assert Validator.objects.filter(public_key=key, active=True).exists()


@pytest.mark.django_db(transaction=True)
def test_fetch_validators_creates_new_active_validators__different_fields(
    faker: Faker, mock_get_validators: MagicMock, settings
):
    settings.COMPUTE_HORDE_VALIDATOR_MODEL = "test_app.Validator2"
    settings.COMPUTE_HORDE_VALIDATOR_KEY_FIELD = "ss58_address"
    settings.COMPUTE_HORDE_VALIDATOR_ACTIVE_FIELD = "is_active"
    settings.COMPUTE_HORDE_VALIDATOR_DEBUG_FIELD = None

    new_keys = [faker.pystr() for _ in range(3)]
    mock_get_validators.return_value = _generate_validator_mocks(new_keys)

    fetch_validators()

    for key in new_keys:
        assert Validator2.objects.filter(ss58_address=key, is_active=True).exists()

        # they should not end up in the wrong model
        assert not Validator.objects.filter(public_key=key, active=True).exists()


@pytest.mark.django_db(transaction=True)
def test_fetch_validators_updates_existing_validators(
    active_validators_keys: list[str],
    inactive_validators_keys: list[str],
    mock_get_validators: MagicMock,
):
    mock_get_validators.return_value = _generate_validator_mocks(inactive_validators_keys)

    fetch_validators()

    assert Validator.objects.count() == len(active_validators_keys) + len(inactive_validators_keys)

    assert Validator.objects.filter(active=True).count() == len(inactive_validators_keys)

    for key in inactive_validators_keys:
        assert Validator.objects.filter(public_key=key, active=True).exists()


@pytest.mark.django_db(transaction=True)
def test_fetch_validators_debug(
    active_validators_keys: list[str],
    inactive_validators_keys: list[str],
    mock_get_validators: MagicMock,
    faker: Faker,
):
    mock_get_validators.return_value = _generate_validator_mocks(active_validators_keys)

    debug_validator_key = faker.pystr()
    Validator.objects.create(public_key=debug_validator_key, active=True, debug=True)

    fetch_validators()

    assert (
        Validator.objects.count() == len(active_validators_keys) + len(inactive_validators_keys) + 1
    )
    assert set(Validator.objects.filter(active=True).values_list("public_key", flat=True)) == set(
        active_validators_keys
    ) | {debug_validator_key}


@pytest.mark.django_db(transaction=True)
def test_fetch_validators_debug_inactive(
    active_validators_keys: list[str],
    mock_get_validators: MagicMock,
    faker: Faker,
):
    mock_get_validators.return_value = _generate_validator_mocks(active_validators_keys)

    debug_validator_key = faker.pystr()
    debug_validator = Validator.objects.create(
        public_key=debug_validator_key, active=False, debug=True
    )

    fetch_validators()

    debug_validator.refresh_from_db()
    assert not debug_validator.active
