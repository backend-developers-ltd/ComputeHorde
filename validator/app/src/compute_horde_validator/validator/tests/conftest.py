import ipaddress
import logging
import uuid
from collections.abc import Generator
from unittest.mock import AsyncMock, MagicMock, Mock, create_autospec, patch

import bittensor_wallet
import pytest
import turbobt
from compute_horde.executor_class import EXECUTOR_CLASS
from compute_horde_core.executor_class import ExecutorClass
from pytest_mock import MockerFixture

from ..organic_jobs.miner_driver import execute_organic_job_request
from .helpers import MockNeuron, MockSyntheticMinerClient

logger = logging.getLogger(__name__)


@pytest.fixture
def some() -> Generator[int, None, None]:
    # setup code
    yield 1
    # teardown code


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
def override_weights_version_v2(settings):
    settings.DEBUG_OVERRIDE_WEIGHTS_VERSION = 2


@pytest.fixture
def override_weights_version_v1(settings):
    settings.DEBUG_OVERRIDE_WEIGHTS_VERSION = 1


@pytest.fixture
def mocked_synthetic_miner_client():
    with patch(
        "compute_horde_validator.validator.synthetic_jobs.batch_run.MinerClient"
    ) as MockedMinerClient:
        MockedMinerClient.instance = None

        def side_effect(*args, **kwargs):
            if MockedMinerClient.instance is not None:
                raise RuntimeError("You can create only single instance of mocked MinerClient")
            MockedMinerClient.instance = MockSyntheticMinerClient(*args, **kwargs)
            return MockedMinerClient.instance

        MockedMinerClient.side_effect = side_effect
        yield MockedMinerClient


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
def bittensor(mocker, validators):
    mocked = create_autospec(
        turbobt.Bittensor(),
    )
    mocked.__aenter__.return_value = mocked
    mocked.block.return_value = MagicMock(
        number=1,
        hash="0xed0050a68f7027abdf10a5e4bd7951c00d886ddbb83bed5b3236ed642082b464",
    )
    mocked.blocks.head.return_value = MagicMock(
        number=1,
        hash="0xed0050a68f7027abdf10a5e4bd7951c00d886ddbb83bed5b3236ed642082b464",
    )
    mocked.blocks.__getitem__.return_value.get = AsyncMock(
        number=1,
        hash="0xed0050a68f7027abdf10a5e4bd7951c00d886ddbb83bed5b3236ed642082b464",
    )
    mocked.subnet.return_value.get_hyperparameters = AsyncMock(
        return_value={
            "min_allowed_weights": 0,
            "max_weights_limit": 65535,
        },
    )
    mocked.subnet.return_value.get_neuron = AsyncMock(
        return_value=None,
    )
    mocked.subnet.return_value.get_state = AsyncMock(
        return_value={
            "alpha_stake": [1000.0 * (i + 1) for i in range(10)],
            "tao_stake": [1.0 * (i + 1) for i in range(10)],
            "stake": [1001.0 * (i + 1) for i in range(10)],
            "total_stake": [1001.0 * (i + 1) for i in range(10)],
        },
    )
    mocked.subnet.return_value.neurons.__getitem__.return_value.get_certificate = AsyncMock()
    mocked.subnet.return_value.list_neurons = AsyncMock(
        return_value=[
            MagicMock(
                hotkey=f"hotkey_{i}",
                uid=i,
                axon_info=Mock(
                    ip=ipaddress.IPv4Address("127.0.0.1"),
                    port=9999,
                    spec=turbobt.neuron.AxonInfo,
                ),
                spec=turbobt.Neuron,
            )
            for i in range(10)
        ],
    )
    mocked.subnet.return_value.list_validators = AsyncMock(
        return_value=validators,
    )
    mocked.subnet.return_value.weights.commit = AsyncMock()

    mocker.patch(
        "turbobt.Bittensor",
        return_value=mocked,
    )
    mocker.patch(
        "compute_horde_validator.validator.tasks.ShieldedBittensor",
        return_value=mocked,
    )

    return mocked
