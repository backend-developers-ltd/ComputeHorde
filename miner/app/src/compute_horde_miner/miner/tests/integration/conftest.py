import pytest
import pytest_asyncio
from pytest_mock import MockerFixture

from compute_horde_miner.miner.models import Validator
from compute_horde_miner.miner.tests.executor_manager import StubExecutorManager
from compute_horde_miner.miner.tests.validator import validator_hotkey


@pytest.fixture
def authentication_on(settings):
    settings.DEBUG_TURN_AUTHENTICATION_OFF = False


@pytest_asyncio.fixture
async def validator() -> Validator:
    return await Validator.objects.acreate(public_key=validator_hotkey(), active=True)


# Somehow the regular dependency mechanism doesn't work with multiple test cases
# Explicit patching with a new instance each time is a temporary workaround
@pytest.fixture
def mock_executor_manager_class(mocker: MockerFixture):
    mocker.patch(
        "compute_horde_miner.miner.executor_manager.current.executor_manager",
        StubExecutorManager(),
    )
