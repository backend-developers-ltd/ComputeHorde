import logging
from unittest.mock import patch

import bittensor_wallet
import pytest
from pytest_mock import MockerFixture

from compute_horde_validator.validator.organic_jobs.miner_driver import execute_organic_job_request

logger = logging.getLogger(__name__)


@pytest.fixture(autouse=True)
def _patch_get_streaming_job_executor_classes(mocker: MockerFixture):
    mocker.patch(
        "compute_horde_validator.validator.synthetic_jobs.batch_run.get_streaming_job_executor_classes",
        return_value={},
    )


@pytest.fixture(autouse=True)
def _patch_current_block():
    with patch(
        "compute_horde_validator.validator.organic_jobs.miner_driver._get_current_block",
        return_value=1337,
    ):
        yield


@pytest.fixture(autouse=True)
def _patch_celery_job_execution():
    with patch(
        "compute_horde_validator.validator.organic_jobs.facilitator_client.execute_organic_job_request_on_worker",
        execute_organic_job_request,
    ):
        yield


@pytest.fixture(autouse=True)
def _patch_collateral_contract_address(mocker: MockerFixture):
    names = [
        "get_collateral_contract_address",
        "get_collateral_contract_address_async",
    ]
    for name in names:
        mocker.patch(
            f"compute_horde_validator.validator.collateral.{name}",
            return_value="0x7b89cb9B1D5B6A9F2296072885019D8Bfecc704A",
        )


@pytest.fixture(scope="session", autouse=True)
def wallet():
    wallet = bittensor_wallet.Wallet(name="test_validator")
    wallet.regenerate_coldkey(
        mnemonic="local ghost evil lizard decade own lecture absurd vote despair predict cage",
        use_password=False,
        overwrite=True,
    )
    wallet.regenerate_hotkey(
        mnemonic="position chicken ugly key sugar expect another require cinnamon rubber rich veteran",
        use_password=False,
        overwrite=True,
    )
    return wallet


@pytest.fixture(scope="function", autouse=True)
def clear_cache():
    yield
    from django.core.cache import cache

    cache.clear()


@pytest.fixture
def validator_keypair():
    return bittensor_wallet.Keypair.create_from_mnemonic(
        "slot excuse valid grief praise rifle spoil auction weasel glove pen share"
    )


@pytest.fixture
def miner_keypair():
    return bittensor_wallet.Keypair.create_from_mnemonic(
        "almost fatigue race slim picnic mass better clog deal solve already champion"
    )
