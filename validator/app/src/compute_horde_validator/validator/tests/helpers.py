import asyncio
from typing import NamedTuple

import bittensor
import numpy as np
from compute_horde.miner_client.base import BaseRequest
from compute_horde.mv_protocol.miner_requests import (
    V0ExecutorReadyRequest,
    V0JobFinishedRequest,
)
from django.conf import settings

from compute_horde_validator.validator.facilitator_client import JobRequest
from compute_horde_validator.validator.models import SystemEvent
from compute_horde_validator.validator.synthetic_jobs.utils import JobState, MinerClient

NUM_NEURONS = 5


def throw_error(*args):
    raise Exception("Error thrown for testing")


def mock_keypair():
    return bittensor.Keypair.create_from_mnemonic(
        mnemonic="arrive produce someone view end scout bargain coil slight festival excess struggle"
    )


class MockWallet:
    def __init__(self, *args):
        pass

    def get_hotkey(self):
        return mock_keypair()


def get_miner_client(MINER_CLIENT, job_uuid: str):
    return MINER_CLIENT(
        loop=asyncio.get_event_loop(),
        miner_address="ignore",
        my_hotkey="validator_hotkey",
        miner_hotkey="miner_hotkey",
        miner_port=9999,
        job_uuid=job_uuid,
        batch_id=None,
        keypair=mock_keypair(),
    )


class MockedAxonInfo(NamedTuple):
    is_serving: bool
    ip: str
    ip_type: int
    port: int


async def mock_get_miner_axon_info(hotkey: str):
    return MockedAxonInfo(is_serving=True, ip_type=4, ip="0000", port=8000)


class MockMinerClient(MinerClient):
    def __init__(self, loop: asyncio.AbstractEventLoop, **args):
        super().__init__(loop, **args)

    def miner_url(self) -> str:
        return "ws://miner"

    async def await_connect(self):
        return

    def accepted_request_type(self) -> type[BaseRequest]:
        return BaseRequest

    def incoming_generic_error_class(self) -> type[BaseRequest]:
        return BaseRequest

    def outgoing_generic_error_class(self) -> type[BaseRequest]:
        return BaseRequest

    async def handle_message(self, msg):
        pass

    async def send_model(self, model):
        pass

    def get_barrier(self):
        return asyncio.Barrier(1)


class MockJobStateMinerClient(MockMinerClient):
    def get_job_state(self, job_uuid):
        job_state = JobState()
        job_state.miner_ready_or_declining_future.set_result(
            V0ExecutorReadyRequest(job_uuid=job_uuid)
        )
        job_state.miner_finished_or_failed_future.set_result(
            V0JobFinishedRequest(
                job_uuid=job_uuid,
                docker_process_stdout="",
                docker_process_stderr="",
            )
        )
        return job_state


def get_dummy_job_request(uuid: str) -> JobRequest:
    return JobRequest(
        type="job.new",
        uuid=uuid,
        miner_hotkey="miner_hotkey",
        docker_image="nvidia",
        raw_script="print('hello world')",
        args=[],
        env={},
        use_gpu=False,
        input_url="fake.com/input",
        output_url="fake.com/output",
    )


class MockSubtensor:
    def __init__(
        self,
        *args,
        mocked_set_weights=lambda: (True, ""),
        mocked_metagraph=lambda: MockMetagraph(),
    ):
        self.mocked_set_weights = mocked_set_weights
        self.mocked_metagraph = mocked_metagraph

    def min_allowed_weights(self, netuid):
        return 0

    def max_weight_limit(self, netuid):
        return 99999

    def get_subnet_hyperparameters(self, *args):
        return None

    def metagraph(self, netuid):
        return self.mocked_metagraph()

    def set_weights(
        self, wallet, netuid, uids, weights, version_key, wait_for_inclusion, wait_for_finalization
    ) -> tuple[bool, str]:
        return self.mocked_set_weights()


class MockNeuron:
    def __init__(self, hotkey, uid):
        self.hotkey = hotkey
        self.uid = uid


class MockMetagraph:
    def __init__(self, netuid=1, num_neurons=NUM_NEURONS):
        self.n = num_neurons
        self.netuid = netuid
        self.num_neurons = num_neurons
        self.W = np.ones((num_neurons, num_neurons))
        self.hotkeys = [f"hotkey_{i}" for i in range(num_neurons)]
        self.uids = np.array(list(range(num_neurons)))
        self.neurons = [MockNeuron(f"hotkey_{i}", i) for i in range(NUM_NEURONS)]


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
