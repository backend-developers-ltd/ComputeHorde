import asyncio
import json
import uuid
from collections.abc import Callable
from unittest.mock import patch

import bittensor
import pytest
from compute_horde.executor_class import DEFAULT_EXECUTOR_CLASS
from compute_horde.mv_protocol import miner_requests
from django.utils import timezone

from compute_horde_validator.validator.models import (
    Miner,
    MinerManifest,
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

MANIFEST_INCENTIVE_MULTIPLIER = 1.05
MANIFEST_DANCE_RATIO_THRESHOLD = 1.4

job_factory = TimeTookScoreMockSyntheticJobGeneratorFactory()


@patch(
    "compute_horde_validator.validator.synthetic_jobs.generator.current.synthetic_job_generator_factory",
    job_factory,
)
@pytest.mark.override_config(
    DYNAMIC_MANIFEST_SCORE_MULTIPLIER=MANIFEST_INCENTIVE_MULTIPLIER,
    DYNAMIC_MANIFEST_DANCE_RATIO_THRESHOLD=MANIFEST_DANCE_RATIO_THRESHOLD,
)
@pytest.mark.parametrize(
    "curr_online_executor_count,prev_online_executor_count,expected_multiplier",
    [
        # None -> 3
        (3, None, MANIFEST_INCENTIVE_MULTIPLIER),
        # 0 -> 3 - e.g. all executors failed to start cause docker images were not cached
        (3, 0, MANIFEST_INCENTIVE_MULTIPLIER),
        # 10 -> below ratio
        (10, int(10 * MANIFEST_DANCE_RATIO_THRESHOLD) - 1, 1),
        # below ratio -> 10
        (int(10 * MANIFEST_DANCE_RATIO_THRESHOLD) - 1, 10, 1),
        # 10 -> above ratio
        (
            int(10 * MANIFEST_DANCE_RATIO_THRESHOLD) + 1,
            10,
            MANIFEST_INCENTIVE_MULTIPLIER,
        ),
        # above ratio -> 10
        (
            10,
            int(10 * MANIFEST_DANCE_RATIO_THRESHOLD) + 1,
            MANIFEST_INCENTIVE_MULTIPLIER,
        ),
    ],
)
async def test_manifest_dance_incentives(
    curr_online_executor_count: int,
    prev_online_executor_count: int,
    expected_multiplier: float,
    miner: Miner,
    axon_dict: dict[str, bittensor.AxonInfo],
    create_simulation_miner_client: Callable,
    transport: MinerSimulationTransport,
    override_weights_version_v2,
    small_spin_up_times,
):
    job_uuids = [uuid.uuid4() for _ in range(curr_online_executor_count)]
    job_factory._uuids = job_uuids.copy()

    if prev_online_executor_count:
        batch = await SyntheticJobBatch.objects.acreate(
            accepting_results_until=timezone.now(), scored=True
        )
        await MinerManifest.objects.acreate(
            miner=miner,
            batch=batch,
            executor_count=prev_online_executor_count,
            online_executor_count=prev_online_executor_count,
        )

    manifest_message = miner_requests.V0ExecutorManifestRequest(
        manifest=miner_requests.ExecutorManifest(
            executor_classes=[
                miner_requests.ExecutorClassManifest(
                    executor_class=DEFAULT_EXECUTOR_CLASS, count=curr_online_executor_count
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

    await asyncio.wait_for(
        execute_synthetic_batch_run(
            axon_dict,
            [miner],
            create_miner_client=create_simulation_miner_client,
        ),
        timeout=2,
    )

    await check_scores(job_uuids, transport, expected_multiplier)


@patch(
    "compute_horde_validator.validator.synthetic_jobs.generator.current.synthetic_job_generator_factory",
    job_factory,
)
@pytest.mark.parametrize(
    "weights_version_override,expected_multiplier,curr_online_executor_count,prev_online_executor_count",
    [
        # no effect on v1
        ("override_weights_version_v1", 1, 1, None),
        # basic test for v2
        ("override_weights_version_v2", MANIFEST_INCENTIVE_MULTIPLIER, 1, None),
        # just basic test for previous executors on single current executor
        ("override_weights_version_v2", MANIFEST_INCENTIVE_MULTIPLIER, 1, 100),
    ],
)
async def test_synthetic_job_batch(
    weights_version_override: str,
    expected_multiplier,
    curr_online_executor_count: int,
    prev_online_executor_count: int | None,
    request: pytest.FixtureRequest,
    miner: Miner,
    axon_dict: dict[str, bittensor.AxonInfo],
    create_simulation_miner_client: Callable,
    transport: MinerSimulationTransport,
    small_spin_up_times,
):
    request.getfixturevalue(weights_version_override)

    job_uuids = [uuid.uuid4() for _ in range(curr_online_executor_count)]
    job_factory._uuids = job_uuids.copy()

    if prev_online_executor_count:
        batch = await SyntheticJobBatch.objects.acreate(
            accepting_results_until=timezone.now(), scored=True
        )
        await MinerManifest.objects.acreate(
            miner=miner,
            batch=batch,
            executor_count=prev_online_executor_count,
            online_executor_count=prev_online_executor_count,
        )

    manifest_message = miner_requests.V0ExecutorManifestRequest(
        manifest=miner_requests.ExecutorManifest(
            executor_classes=[
                miner_requests.ExecutorClassManifest(
                    executor_class=DEFAULT_EXECUTOR_CLASS, count=curr_online_executor_count
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

    await asyncio.wait_for(
        execute_synthetic_batch_run(
            axon_dict,
            [miner],
            create_miner_client=create_simulation_miner_client,
        ),
        timeout=2,
    )

    await check_scores(job_uuids, transport, expected_multiplier)


async def check_scores(
    job_uuids: list[uuid.UUID],
    transport: MinerSimulationTransport,
    expected_multiplier: float,
):
    job_finished_messages = transport.sent[-len(job_uuids) :]

    for msg in job_finished_messages:
        receipt = json.loads(msg)

        job_uuid = receipt["payload"]["job_uuid"]

        time_took_us = receipt["payload"]["time_took_us"]
        time_took = time_took_us / 1_000_000

        job = await SyntheticJob.objects.aget(job_uuid=job_uuid)

        assert abs(job.score * time_took - expected_multiplier) < 0.0001
