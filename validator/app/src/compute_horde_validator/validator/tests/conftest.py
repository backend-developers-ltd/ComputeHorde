import logging
import uuid
from collections.abc import Generator
from unittest.mock import create_autospec, patch

import bittensor_wallet
import pytest
from compute_horde.executor_class import EXECUTOR_CLASS
from compute_horde_core.executor_class import ExecutorClass
from pylon_client.v1 import PylonClient

from ..organic_jobs.miner_driver import execute_organic_job_request
from .helpers import MockNeuron

logger = logging.getLogger(__name__)


@pytest.fixture
def some() -> Generator[int, None, None]:
    # setup code
    yield 1
    # teardown code


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


@pytest.fixture(scope="function", autouse=True)
def clear_cache():
    yield
    from django.core.cache import cache

    cache.clear()


@pytest.fixture
def small_spin_up_times(monkeypatch):
    monkeypatch.setattr(EXECUTOR_CLASS[ExecutorClass.spin_up_4min__gpu_24gb], "spin_up_time", 4)


@pytest.fixture
def validators():
    return [MockNeuron(hotkey=f"mock_validator_hotkey_{i}", uid=i) for i in range(11)]


@pytest.fixture
def validators_with_this_hotkey(settings, validators):
    this_hotkey = settings.BITTENSOR_WALLET().get_hotkey().ss58_address
    new_validators = [*validators]
    new_validators[6] = MockNeuron(hotkey=this_hotkey, uid=6)
    return new_validators


@pytest.fixture(scope="session")
def run_uuid():
    return str(uuid.uuid4())


# NOTE: Use this fixture when you need to find dangling asyncio tasks. It is currently commented
#       because redis channels layers keeps dangling tasks, that makes the tests fail -_-
# @pytest_asyncio.fixture(autouse=True)
# async def check_still_running_tasks():
#     yield
#     tasks = asyncio.all_tasks()
#     if len(tasks) > 1:
#         raise ValueError(
#             "\n" + "\n".join(f"{task.get_name()}: {task.get_coro()}" for task in tasks)
#         )


@pytest.fixture
def pylon_client_mock(mocker):
    # This is a temporary solution until pylon client implements its own mocking utility.
    mocked = create_autospec(PylonClient)
    mocked.__enter__.return_value = mocked
    mocked.open_access = create_autospec(PylonClient._open_access_api_cls, instance=True)
    mocked.identity = create_autospec(PylonClient._identity_api_cls, instance=True)
    mocker.patch("compute_horde_validator.validator.pylon.PylonClient", return_value=mocked)
    return mocked
