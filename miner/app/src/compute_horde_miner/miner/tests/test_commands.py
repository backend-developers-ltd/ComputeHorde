from pathlib import Path
from unittest.mock import MagicMock

import environ
import pytest
from django.core.management import call_command
from pytest_mock import MockerFixture

from compute_horde_miner.miner.models import Validator

pytestmark = [pytest.mark.django_db]


@pytest.fixture(autouse=True)
def mock_subprocess(mocker: MockerFixture):
    return mocker.patch("compute_horde_miner.miner.management.commands.self_test.subprocess")


@pytest.fixture(autouse=True)
def mock_create_database(mocker: MockerFixture):
    return mocker.patch(
        "compute_horde_miner.miner.management.commands.self_test.create_validator_database"
    )


@pytest.fixture
def env_file(tmp_path: Path):
    return tmp_path / ".env.validator"


@pytest.fixture
def hotkey_address():
    return "123qweasd"


@pytest.fixture(autouse=True)
def _patch_hotkey_address(mocker: MockerFixture, hotkey_address):
    mock = mocker.patch(
        "compute_horde_miner.miner.management.commands.self_test.settings.BITTENSOR_WALLET",
    )
    mock.return_value.hotkey.ss58_address = hotkey_address


def test_self_test_ok(mock_subprocess: MagicMock, mock_create_database: MagicMock, env_file: Path):
    validator_db_name = "validator_db_name"
    validator_image = "custom_validator_image"

    call_command(
        "self_test",
        validator_db_name=validator_db_name,
        validator_image=validator_image,
        validator_env_file=env_file,
    )

    run_args = mock_subprocess.run.call_args_list[0][0][0]

    assert "docker" in run_args
    assert str(env_file) in run_args

    mock_create_database.assert_called_once_with(validator_db_name)

    assert not Validator.objects.exists()
    assert not env_file.exists()


def test_self_test_env_file(env_file: Path, hotkey_address: str, settings):
    validator_db_name = "validator_db_name"
    validator_image = "custom_validator_image"

    call_command(
        "self_test",
        validator_db_name=validator_db_name,
        validator_image=validator_image,
        validator_env_file=env_file,
        weights_version=999,
        remove_env_file=False,
    )

    assert env_file.exists()

    content = env_file.read_text().replace("\n", ",")

    env = environ.Env.parse_value(content, cast=dict)

    for arg in [
        "BITTENSOR_NETWORK",
        "BITTENSOR_NETUID",
        "BITTENSOR_WALLET_NAME",
        "BITTENSOR_WALLET_HOTKEY_NAME",
    ]:
        assert env[arg] == str(getattr(settings, arg))

    assert env["DEBUG_MINER_ADDRESS"] == "host.docker.internal"
    assert env["DEBUG_MINER_PORT"] == "8000"
    assert env["DEBUG_OVERRIDE_WEIGHTS_VERSION"] == "999"
    assert env["DEBUG_MINER_KEY"] == hotkey_address


def test_self_test_validator_record_created(hotkey_address: str, env_file: Path):
    call_command(
        "self_test",
        validator_db_name="validator_db_name",
        validator_image="custom_validator_image",
        validator_env_file=env_file,
        weights_version=999,
        clean_validator_records=False,
    )

    assert Validator.objects.filter(public_key=hotkey_address, active=True, debug=True).exists()
