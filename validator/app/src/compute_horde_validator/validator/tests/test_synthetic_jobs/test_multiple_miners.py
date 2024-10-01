import asyncio
import uuid
from collections.abc import Callable
from unittest.mock import patch

import bittensor
import pytest
import pytest_asyncio
from compute_horde.miner_client.base import AbstractTransport
from compute_horde.mv_protocol import miner_requests

from compute_horde_validator.validator.models import Miner, SyntheticJob, SystemEvent
from compute_horde_validator.validator.synthetic_jobs.batch_run import (
    BatchContext,
    MinerClient,
    execute_synthetic_batch_run,
)
from compute_horde_validator.validator.tests.transport import MinerSimulationTransport

from .helpers import check_miner_job_system_events, check_synthetic_job
from .mock_generator import MOCK_SCORE, NOT_SCORED

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.django_db(databases=["default", "default_alias"], transaction=True),
]


@pytest.fixture
def num_miners():
    return 5


@pytest.fixture
def job_uuids(num_miners: int):
    return [uuid.uuid4() for _ in range(num_miners)]


@pytest.fixture
def miner_hotkeys(num_miners: int):
    return [f"hotkey_{i}" for i in range(num_miners)]


@pytest_asyncio.fixture
async def miners(miner_hotkeys: list[str]):
    objs = [Miner(hotkey=hotkey) for hotkey in miner_hotkeys]
    await Miner.objects.abulk_create(objs)
    return objs


@pytest.fixture
def miner_axon_infos(miner_hotkeys: str):
    return [
        bittensor.AxonInfo(
            version=4,
            ip="ignore",
            ip_type=4,
            port=9999,
            hotkey=hotkey,
            coldkey=hotkey,
        )
        for hotkey in miner_hotkeys
    ]


@pytest.fixture
def axon_dict(miner_axon_infos: list[bittensor.AxonInfo]):
    return {axon.hotkey: axon for axon in miner_axon_infos}


@pytest_asyncio.fixture
async def transports(miner_hotkeys: str):
    return [MinerSimulationTransport(hotkey) for hotkey in miner_hotkeys]


@pytest.fixture
def create_simulation_miner_client(miner_hotkeys: list[str], transports: list[AbstractTransport]):
    transport_dict = {hotkey: transport for hotkey, transport in zip(miner_hotkeys, transports)}

    def _create(ctx: BatchContext, miner_hotkey: str):
        return MinerClient(
            ctx=ctx, miner_hotkey=miner_hotkey, transport=transport_dict[miner_hotkey]
        )

    return _create


async def test_all_succeed(
    axon_dict: dict[str, bittensor.AxonInfo],
    transports: list[MinerSimulationTransport],
    miners: list[Miner],
    create_simulation_miner_client: Callable,
    job_uuids: list[uuid.UUID],
    manifest_message: str,
):
    for job_uuid, transport in zip(job_uuids, transports):
        await transport.add_message(manifest_message, send_before=1)

        accept_message = miner_requests.V0AcceptJobRequest(job_uuid=str(job_uuid)).model_dump_json()
        await transport.add_message(accept_message, send_before=1)

        executor_ready_message = miner_requests.V0ExecutorReadyRequest(
            job_uuid=str(job_uuid)
        ).model_dump_json()
        await transport.add_message(executor_ready_message, send_before=0)

        job_finish_message = miner_requests.V0JobFinishedRequest(
            job_uuid=str(job_uuid), docker_process_stdout="", docker_process_stderr=""
        ).model_dump_json()

        await transport.add_message(job_finish_message, send_before=2)

    await asyncio.wait_for(
        execute_synthetic_batch_run(
            axon_dict,
            miners,
            create_miner_client=create_simulation_miner_client,
        ),
        timeout=1,
    )

    for job_uuid, miner in zip(job_uuids, miners):
        await check_synthetic_job(job_uuid, miner.pk, SyntheticJob.Status.COMPLETED, MOCK_SCORE)


@pytest_asyncio.fixture
async def flow_0(
    transports: list[MinerSimulationTransport], manifest_message: str, job_uuids: list[uuid.UUID]
):
    """
    Job successfully finished
    """

    index = 0
    transport = transports[index]
    job_uuid = job_uuids[index]

    await transport.add_message(manifest_message, send_before=1)

    accept_message = miner_requests.V0AcceptJobRequest(job_uuid=str(job_uuid)).model_dump_json()
    await transport.add_message(accept_message, send_before=1)

    executor_ready_message = miner_requests.V0ExecutorReadyRequest(
        job_uuid=str(job_uuid)
    ).model_dump_json()
    await transport.add_message(executor_ready_message, send_before=0)

    job_finish_message = miner_requests.V0JobFinishedRequest(
        job_uuid=str(job_uuid), docker_process_stdout="", docker_process_stderr=""
    ).model_dump_json()

    await transport.add_message(job_finish_message, send_before=2)


@pytest_asyncio.fixture
async def flow_1(
    transports: list[MinerSimulationTransport], manifest_message: str, job_uuids: list[uuid.UUID]
):
    """
    Job timed out
    """

    index = 1
    transport = transports[index]
    job_uuid = job_uuids[index]

    await transport.add_message(manifest_message, send_before=1)

    accept_message = miner_requests.V0AcceptJobRequest(job_uuid=str(job_uuid)).model_dump_json()
    await transport.add_message(accept_message, send_before=1)

    executor_ready_message = miner_requests.V0ExecutorReadyRequest(
        job_uuid=str(job_uuid)
    ).model_dump_json()
    await transport.add_message(executor_ready_message, send_before=0)

    job_finish_message = miner_requests.V0JobFinishedRequest(
        job_uuid=str(job_uuid), docker_process_stdout="", docker_process_stderr=""
    ).model_dump_json()

    await transport.add_message(job_finish_message, send_before=2, sleep_before=2)


@pytest_asyncio.fixture
async def flow_2(
    transports: list[MinerSimulationTransport], manifest_message: str, job_uuids: list[uuid.UUID]
):
    """
    Job failed
    """

    index = 2
    transport = transports[index]
    job_uuid = job_uuids[index]

    await transport.add_message(manifest_message, send_before=1)

    accept_message = miner_requests.V0AcceptJobRequest(job_uuid=str(job_uuid)).model_dump_json()
    await transport.add_message(accept_message, send_before=1)

    executor_ready_message = miner_requests.V0ExecutorReadyRequest(
        job_uuid=str(job_uuid)
    ).model_dump_json()
    await transport.add_message(executor_ready_message, send_before=0)

    job_failed_message = miner_requests.V0JobFailedRequest(
        job_uuid=str(job_uuid), docker_process_stdout="", docker_process_stderr=""
    ).model_dump_json()

    await transport.add_message(job_failed_message, send_before=2)


@pytest_asyncio.fixture
async def flow_3(
    transports: list[MinerSimulationTransport], manifest_message: str, job_uuids: list[uuid.UUID]
):
    """
    Job declined
    """

    index = 3
    transport = transports[index]
    job_uuid = job_uuids[index]

    await transport.add_message(manifest_message, send_before=1)

    decline_message = miner_requests.V0DeclineJobRequest(job_uuid=str(job_uuid)).model_dump_json()
    await transport.add_message(decline_message, send_before=1)


@pytest_asyncio.fixture
async def flow_4():
    """
    No manifest. Fixture just for indication
    """


@patch("compute_horde_validator.validator.synthetic_jobs.batch_run._GET_MANIFEST_TIMEOUT", 0.2)
@patch(
    "compute_horde_validator.validator.synthetic_jobs.batch_run._JOB_RESPONSE_EXTRA_TIMEOUT", 0.1
)
@patch("compute_horde_validator.validator.synthetic_jobs.batch_run.random.shuffle", lambda x: x)
async def test_complex(
    axon_dict: dict[str, bittensor.AxonInfo],
    miners: list[Miner],
    transports,
    create_simulation_miner_client: Callable,
    job_uuids: list[uuid.UUID],
    flow_0,
    flow_1,
    flow_2,
    flow_3,
    flow_4,
):
    for transport, miner in zip(transports, miners):
        assert transport.name == miner.hotkey

    await asyncio.wait_for(
        execute_synthetic_batch_run(
            axon_dict,
            miners,
            create_miner_client=create_simulation_miner_client,
        ),
        timeout=2,
    )

    assert await SyntheticJob.objects.acount() == 4
    assert (
        await SystemEvent.objects.exclude(type=SystemEvent.EventType.VALIDATOR_TELEMETRY).acount()
        == 5
    )

    await check_synthetic_job(job_uuids[0], miners[0].pk, SyntheticJob.Status.COMPLETED, MOCK_SCORE)
    await check_miner_job_system_events(
        [
            (
                SystemEvent.EventType.MINER_SYNTHETIC_JOB_SUCCESS,
                SystemEvent.EventSubType.SUCCESS,
            ),
            (
                SystemEvent.EventType.VALIDATOR_TELEMETRY,
                SystemEvent.EventSubType.SYNTHETIC_JOB,
            ),
        ],
        miners[0].hotkey,
        job_uuids[0],
    )

    await check_synthetic_job(job_uuids[1], miners[1].pk, SyntheticJob.Status.FAILED, NOT_SCORED)
    await check_miner_job_system_events(
        [
            (
                SystemEvent.EventType.MINER_SYNTHETIC_JOB_FAILURE,
                SystemEvent.EventSubType.JOB_EXECUTION_TIMEOUT,
            ),
            (
                SystemEvent.EventType.VALIDATOR_TELEMETRY,
                SystemEvent.EventSubType.SYNTHETIC_JOB,
            ),
        ],
        miners[1].hotkey,
        job_uuids[1],
    )

    await check_synthetic_job(job_uuids[2], miners[2].pk, SyntheticJob.Status.FAILED, NOT_SCORED)
    await check_miner_job_system_events(
        [
            (
                SystemEvent.EventType.MINER_SYNTHETIC_JOB_FAILURE,
                SystemEvent.EventSubType.FAILURE,
            ),
            (
                SystemEvent.EventType.VALIDATOR_TELEMETRY,
                SystemEvent.EventSubType.SYNTHETIC_JOB,
            ),
        ],
        miners[2].hotkey,
        job_uuids[2],
    )

    await check_synthetic_job(job_uuids[3], miners[3].pk, SyntheticJob.Status.FAILED, NOT_SCORED)
    await check_miner_job_system_events(
        [
            (
                SystemEvent.EventType.MINER_SYNTHETIC_JOB_FAILURE,
                SystemEvent.EventSubType.JOB_NOT_STARTED,
            ),
            (
                SystemEvent.EventType.VALIDATOR_TELEMETRY,
                SystemEvent.EventSubType.SYNTHETIC_JOB,
            ),
        ],
        miners[3].hotkey,
        job_uuids[3],
    )

    # TODO: Make this system event bound to the miner and the job
    assert (
        await SystemEvent.objects.filter(
            type=SystemEvent.EventType.MINER_SYNTHETIC_JOB_FAILURE,
            subtype=SystemEvent.EventSubType.MANIFEST_TIMEOUT,
        ).acount()
        == 1
    )
