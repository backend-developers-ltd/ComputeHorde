import numbers
from typing import NamedTuple

import numpy as np
from compute_horde.executor_class import DEFAULT_EXECUTOR_CLASS
from compute_horde.miner_client.base import BaseRequest
from compute_horde.mv_protocol.miner_requests import (
    V0ExecutorReadyRequest,
    V0JobFinishedRequest,
)
from django.conf import settings

from compute_horde_validator.validator.models import SystemEvent
from compute_horde_validator.validator.organic_jobs.facilitator_api import (
    V0FacilitatorJobRequest,
    V1FacilitatorJobRequest,
)
from compute_horde_validator.validator.organic_jobs.miner_client import MinerClient

NUM_NEURONS = 5


def throw_error(*args):
    raise Exception("Error thrown for testing")


def get_keypair():
    return settings.BITTENSOR_WALLET().get_hotkey()


def get_miner_client(MINER_CLIENT, job_uuid: str):
    return MINER_CLIENT(
        miner_address="ignore",
        my_hotkey="validator_hotkey",
        miner_hotkey="miner_hotkey",
        miner_port=9999,
        job_uuid=job_uuid,
        batch_id=None,
        keypair=get_keypair(),
    )


class MockedAxonInfo(NamedTuple):
    is_serving: bool
    ip: str
    ip_type: int
    port: int


async def mock_get_miner_axon_info(hotkey: str):
    return MockedAxonInfo(is_serving=True, ip_type=4, ip="0000", port=8000)


class MockMinerClient(MinerClient):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._sent_models = []

    def miner_url(self) -> str:
        return "ws://miner"

    async def connect(self):
        return

    def accepted_request_type(self) -> type[BaseRequest]:
        return BaseRequest

    def incoming_generic_error_class(self) -> type[BaseRequest]:
        return BaseRequest

    def outgoing_generic_error_class(self) -> type[BaseRequest]:
        return BaseRequest

    async def handle_message(self, msg):
        pass

    async def send_model(self, model, error_event_callback=None):
        self._sent_models.append(model)

    def _query_sent_models(self, condition=None, model_class=None):
        result = []
        for model in self._sent_models:
            if model_class is not None and not isinstance(model, model_class):
                continue
            if not condition(model):
                continue
            result.append(model)
        return result


class MockJobStateMinerClient(MockMinerClient):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.miner_ready_or_declining_future.set_result(
            V0ExecutorReadyRequest(job_uuid=self.job_uuid)
        )
        self.miner_finished_or_failed_future.set_result(
            V0JobFinishedRequest(
                job_uuid=self.job_uuid,
                docker_process_stdout="",
                docker_process_stderr="",
            )
        )


def get_dummy_job_request_v0(uuid: str) -> V0FacilitatorJobRequest:
    return V0FacilitatorJobRequest(
        type="job.new",
        uuid=uuid,
        miner_hotkey="miner_hotkey",
        executor_class=DEFAULT_EXECUTOR_CLASS,
        docker_image="nvidia",
        raw_script="print('hello world')",
        args=[],
        env={},
        use_gpu=False,
        input_url="fake.com/input",
        output_url="fake.com/output",
    )


def get_dummy_job_request_v1(uuid: str) -> V1FacilitatorJobRequest:
    return V1FacilitatorJobRequest(
        type="job.new",
        uuid=uuid,
        miner_hotkey="miner_hotkey",
        docker_image="nvidia",
        raw_script="print('hello world')",
        args=[],
        env={},
        use_gpu=False,
        volume={
            "volume_type": "multi_volume",
            "volumes": [
                {
                    "volume_type": "single_file",
                    "url": "fake.com/input.txt",
                    "relative_path": "input.txt",
                },
                {
                    "volume_type": "zip_url",
                    "contents": "fake.com/input.zip",
                    "relative_path": "zip/",
                },
            ],
        },
        output_upload={
            "output_upload_type": "multi_upload",
            "uploads": [
                {
                    "output_upload_type": "single_file_post",
                    "url": "http://s3.bucket.com/output1.txt",
                    "relative_path": "output1.txt",
                },
                {
                    "output_upload_type": "single_file_put",
                    "url": "http://s3.bucket.com/output2.zip",
                    "relative_path": "zip/output2.zip",
                },
            ],
            "system_output": {
                "output_upload_type": "zip_and_http_put",
                "url": "http://r2.bucket.com/output.zip",
            },
        },
    )


class MockHyperparameters:
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)


class MockSubtensor:
    def __init__(
        self,
        *args,
        mocked_set_weights=lambda: (True, ""),
        mocked_commit_weights=lambda: (True, ""),
        mocked_reveal_weights=lambda: (True, ""),
        mocked_metagraph=lambda: MockMetagraph(),
        hyperparameters=MockHyperparameters(
            commit_reveal_weights_enabled=False,
            commit_reveal_weights_interval=1000,
            max_weight_limit=65535,
        ),
    ):
        self.mocked_set_weights = mocked_set_weights
        self.mocked_commit_weights = mocked_commit_weights
        self.mocked_reveal_weights = mocked_reveal_weights
        self.mocked_metagraph = mocked_metagraph
        self.hyperparameters = hyperparameters
        self.weights_set: list[list[numbers.Number]] = []
        self.weights_committed: list[list[numbers.Number]] = []
        self.weights_revealed: list[list[numbers.Number]] = []

    def min_allowed_weights(self, netuid):
        return 0

    def max_weight_limit(self, netuid):
        return 99999

    def get_subnet_hyperparameters(self, netuid: int) -> MockHyperparameters:
        return self.hyperparameters

    def metagraph(self, netuid):
        return self.mocked_metagraph()

    def set_weights(
        self,
        wallet,
        netuid,
        uids,
        weights,
        version_key,
        wait_for_inclusion,
        wait_for_finalization,
        **kwargs,
    ) -> tuple[bool, str]:
        if not isinstance(weights, list):
            weights = weights.tolist()
        self.weights_set.append(weights)
        return self.mocked_set_weights()

    def commit_weights(self, weights, **kwargs) -> tuple[bool, str]:
        self.weights_committed.append(weights)
        if self.hyperparameters.commit_reveal_weights_enabled:
            return self.mocked_commit_weights()
        return False, "MockSubtensor doesn't support commit_weights"

    def reveal_weights(self, weights, **kwargs) -> tuple[bool, str]:
        self.weights_revealed.append(weights)
        if self.hyperparameters.commit_reveal_weights_enabled:
            return self.mocked_reveal_weights()
        return False, "MockSubtensor doesn't support reveal_weights"


class MockSubtensorWithInaccessibleHyperparams(MockSubtensor):
    """Subtensor but it fails when getting hyperparameters - just like real subtensor!"""

    def get_subnet_hyperparameters(self, netuid: int) -> MockHyperparameters:
        raise Exception("Subtensor is broken")


class MockNeuron:
    def __init__(self, hotkey, uid):
        self.hotkey = hotkey
        self.uid = uid


class MockBlock:
    def item(self) -> int:
        return 1000


class MockMetagraph:
    def __init__(self, netuid=1, num_neurons=NUM_NEURONS):
        self.n = num_neurons
        self.netuid = netuid
        self.num_neurons = num_neurons
        self.W = np.ones((num_neurons, num_neurons))
        self.hotkeys = [f"hotkey_{i}" for i in range(num_neurons)]
        self.uids = np.array(list(range(num_neurons)))
        self.neurons = [MockNeuron(f"hotkey_{i}", i) for i in range(NUM_NEURONS)]
        self.block = MockBlock()


def check_system_events(
    type: SystemEvent.EventType, subtype: SystemEvent.EventSubType, count: int = 1
):
    assert (
        SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS)
        .filter(
            type=type,
            subtype=subtype,
        )
        .count()
        == count
    )


def always_same_result(result):
    """Return a function which accepts everything and returns the same result"""
    return lambda *args, **kwargs: result
