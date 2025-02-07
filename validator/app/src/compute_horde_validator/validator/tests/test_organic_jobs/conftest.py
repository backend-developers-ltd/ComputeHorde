import asyncio
from collections import namedtuple
from collections.abc import Callable
from unittest.mock import AsyncMock, Mock, patch

import pytest
import pytest_asyncio
from compute_horde.executor_class import DEFAULT_EXECUTOR_CLASS
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
    return SimulationTransport("miner")


@pytest_asyncio.fixture
async def faci_transport():
    transport = SimulationTransport("facilitator")
    # This responds to validator authenticating to faci.
    await transport.add_message('{"status":"success"}', send_before=1)
    return transport


@pytest.fixture
def let_it_rip(faci_transport, miner_transport, validator_keypair):
    """
    Returns a coroutine that will execute the messages programmed into the faci and miner transports.
    The transports should be requested as fixtures by the test function to define the sequence of messages.
    """

    async def actually_let_it_rip(until: Callable[[], bool]):
        def fake_miner_client_factory(*args, **kwargs):
            return OrganicMinerClient(*args, **kwargs, transport=miner_transport)

        faci_client = FacilitatorClient(validator_keypair, "")
        faci_client.MINER_CLIENT_CLASS = fake_miner_client_factory

        async with faci_client, faci_transport.receive_condition, asyncio.timeout(10):
            faci_loop = asyncio.create_task(
                faci_client.handle_connection(_SimulationTransportWsAdapter(faci_transport))
            )
            await faci_transport.receive_condition.wait_for(until)

        faci_loop.cancel()
        try:
            await faci_loop
        except asyncio.CancelledError:
            pass

    with (
        patch.object(FacilitatorClient, "heartbeat", AsyncMock()),
        patch.object(FacilitatorClient, "create_metagraph_refresh_task", Mock()),
        patch.object(FacilitatorClient, "get_current_block", AsyncMock(return_value=1)),
        patch.object(
            FacilitatorClient,
            "get_miner_axon_info",
            AsyncMock(
                return_value=namedtuple("MockAxonInfo", "ip,port,ip_type")(
                    ip="500.500.500.500", port=1, ip_type=4
                )
            ),
        ),
        patch(
            "compute_horde_validator.validator.organic_jobs.facilitator_client.verify_job_request",
            AsyncMock(),
        ),
    ):
        yield actually_let_it_rip
