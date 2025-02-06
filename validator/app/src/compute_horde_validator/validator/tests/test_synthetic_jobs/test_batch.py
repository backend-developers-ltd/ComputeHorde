import asyncio
import json
import uuid
from collections.abc import Callable

import bittensor
import pytest
from compute_horde.executor_class import DEFAULT_EXECUTOR_CLASS
from compute_horde.mv_protocol import miner_requests

from compute_horde_validator.validator.models import (
    Cycle,
    Miner,
    SyntheticJob,
    SyntheticJobBatch,
)
from compute_horde_validator.validator.synthetic_jobs.batch_run import execute_synthetic_batch_run
from compute_horde_validator.validator.tests.transport import MinerSimulationTransport

from .mock_generator import (
    TimeTookScoreMockSyntheticJobGeneratorFactory,
)

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.django_db(databases=["default", "default_alias"], transaction=True),
]


@pytest.fixture
def job_generator_factory():
    return TimeTookScoreMockSyntheticJobGeneratorFactory()


async def test_synthetic_job_batch(
    job_generator_factory: TimeTookScoreMockSyntheticJobGeneratorFactory,
    miner: Miner,
    axon_dict: dict[str, bittensor.AxonInfo],
    create_simulation_miner_client: Callable,
    transport: MinerSimulationTransport,
    small_spin_up_times,
    override_weights_version_v2,
):
    executor_count = 10
    job_uuids = [uuid.uuid4() for _ in range(executor_count)]
    job_generator_factory._uuids = job_uuids.copy()

    manifest_message = miner_requests.V0ExecutorManifestRequest(
        manifest=miner_requests.ExecutorManifest(
            executor_classes=[
                miner_requests.ExecutorClassManifest(
                    executor_class=DEFAULT_EXECUTOR_CLASS, count=executor_count
                )
            ]
        )
    ).model_dump_json()
    await transport.add_message(manifest_message, send_before=1)

    async def add_job_messages(request_class, send_before=1, sleep_before=0, **kwargs):
        for job_uuid in job_uuids:
            msg = request_class(
                job_uuid=str(job_uuid),
                **kwargs,
            ).model_dump_json()
            await transport.add_message(msg, send_before=send_before, sleep_before=sleep_before)

    await add_job_messages(miner_requests.V0AcceptJobRequest, send_before=1, sleep_before=0.05)
    await add_job_messages(miner_requests.V0ExecutorReadyRequest, send_before=0)
    await add_job_messages(
        miner_requests.V0JobFinishedRequest,
        send_before=2,
        sleep_before=0.05,
        docker_process_stdout="",
        docker_process_stderr="",
    )

    batch = await SyntheticJobBatch.objects.acreate(
        block=1000,
        cycle=await Cycle.objects.acreate(start=708, stop=1430),
    )
    await asyncio.wait_for(
        execute_synthetic_batch_run(
            axon_dict,
            [miner],
            [],
            batch.id,
            create_miner_client=create_simulation_miner_client,
        ),
        timeout=2,
    )

    await check_scores(job_uuids, transport)


async def check_scores(
    job_uuids: list[uuid.UUID],
    transport: MinerSimulationTransport,
):
    job_finished_messages = transport.sent[-len(job_uuids) :]

    for msg in job_finished_messages:
        receipt = json.loads(msg)

        job_uuid = receipt["payload"]["job_uuid"]

        job = await SyntheticJob.objects.aget(job_uuid=job_uuid)

        assert abs(job.score - 1) < 0.0001
