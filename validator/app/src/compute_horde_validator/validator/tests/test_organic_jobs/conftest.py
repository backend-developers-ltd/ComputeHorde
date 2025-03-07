import asyncio
import uuid
from collections.abc import Callable
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio
from compute_horde.executor_class import DEFAULT_EXECUTOR_CLASS
from compute_horde.fv_protocol import facilitator_requests
from compute_horde.miner_client.organic import OrganicMinerClient
from compute_horde.transport import AbstractTransport

from compute_horde_validator.validator.models import Cycle, Miner, MinerManifest, SyntheticJobBatch
from compute_horde_validator.validator.organic_jobs.facilitator_client import FacilitatorClient
from compute_horde_validator.validator.tests.transport import SimulationTransport


@pytest.fixture
def miner(miner_keypair):
    return Miner.objects.create(hotkey=miner_keypair.ss58_address)


@pytest.fixture(autouse=True)
def manifest(miner):
    cycle = Cycle.objects.create(start=1, stop=2)
    batch = SyntheticJobBatch.objects.create(block=5, cycle=cycle)
    return MinerManifest.objects.create(
        miner=miner,
        batch=batch,
        executor_class=DEFAULT_EXECUTOR_CLASS,
        executor_count=5,
        online_executor_count=5,
    )


class _SimulationTransportWsAdapter:
    """
    Dirty hack to adapt the SimulationTransport into FacilitatorClient, which doesn't abstract away its dependence
    on WS socket. Quack.
    TODO: Refactor FacilitatorClient.
    """

    def __init__(self, transport: AbstractTransport):
        self.transport = transport

    async def send(self, msg: str):
        await self.transport.send(msg)

    async def recv(self) -> str:
        return await self.transport.receive()

    def __aiter__(self):
        return self.transport


@pytest_asyncio.fixture
async def miner_transport():
    return SimulationTransport("miner_01")


@pytest_asyncio.fixture
async def faci_transport():
    transport = SimulationTransport("facilitator")
    # This responds to validator authenticating to faci.
    await transport.add_message('{"status":"success"}', send_before=1)
    return transport


@pytest_asyncio.fixture
async def miner_transports(miner_transport):
    """
    In case of multiple job attempts within a single test, the transports will be used sequentially.
    This does not mean each one will use a different miner - the miner used depends on actual job routing.
    The first transport is the same as returned by the singular `miner_transport` fixture.
    """
    return [
        miner_transport,
        SimulationTransport("miner_02"),
        SimulationTransport("miner_03"),
    ]


@pytest.fixture
def execute_scenario(faci_transport, miner_transports, validator_keypair):
    """
    Returns a coroutine that will execute the messages programmed into the faci and miner transports.
    The transports should be requested as fixtures by the test function to define the sequence of messages.
    """

    async def actually_execute_scenario(until: Callable[[], bool], timeout_seconds: int = 1):
        miner_transport_iter = iter(miner_transports)

        def fake_miner_client_factory(*args, **kwargs):
            """
            Creates a real organic miner client, but replaces the WS transport with a pre-programmed sequence.
            """
            return OrganicMinerClient(*args, **kwargs, transport=next(miner_transport_iter))

        # The actual client being tested
        faci_client = FacilitatorClient(validator_keypair, "")
        faci_client.MINER_CLIENT_CLASS = fake_miner_client_factory

        async def wait_for_condition(condition: asyncio.Condition, until: Callable[[], bool]):
            async with condition:
                await condition.wait_for(until)

        finish_events = [
            asyncio.create_task(wait_for_condition(faci_transport.receive_condition, until)),
            *(
                asyncio.create_task(wait_for_condition(miner_transport.receive_condition, until))
                for miner_transport in miner_transports
            ),
        ]

        try:
            async with faci_client, asyncio.timeout(timeout_seconds):
                faci_loop = asyncio.create_task(
                    faci_client.handle_connection(_SimulationTransportWsAdapter(faci_transport))
                )
                _, pending = await asyncio.wait(finish_events, return_when=asyncio.FIRST_COMPLETED)
        except TimeoutError:
            pass

        for future in (*finish_events, faci_loop):
            if not future.done():
                future.cancel()

        # This await is crucial as it allows multiple other tasks to get cancelled properly
        # Otherwise "cancelling" tasks will persist until the end of the event loop and asyncio doesn't like that
        await asyncio.sleep(0)

    with (
        patch.object(FacilitatorClient, "heartbeat", AsyncMock()),
        patch.object(FacilitatorClient, "get_current_block", AsyncMock(return_value=1)),
        patch(
            "compute_horde_validator.validator.organic_jobs.facilitator_client.verify_job_request",
            AsyncMock(),
        ),
    ):
        yield actually_execute_scenario


@pytest.fixture()
def job_request():
    return facilitator_requests.V2JobRequest(
        uuid=str(uuid.uuid4()),
        executor_class=DEFAULT_EXECUTOR_CLASS,
        docker_image="doesntmatter",
        raw_script="doesntmatter",
        args=[],
        env={},
        use_gpu=False,
    )


@pytest.fixture()
def another_job_request(job_request):
    return job_request.__replace__(uuid=str(uuid.uuid4()))
