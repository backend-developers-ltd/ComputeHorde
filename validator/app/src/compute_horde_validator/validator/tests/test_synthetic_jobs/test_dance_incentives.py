import asyncio
import json
import uuid
from unittest.mock import patch

import bittensor
import pytest
from compute_horde.executor_class import DEFAULT_EXECUTOR_CLASS
from compute_horde.mv_protocol import miner_requests
from constance.test.unittest import override_config

from compute_horde_validator.validator.miner_client import MinerClient
from compute_horde_validator.validator.models import (
    Miner,
    SyntheticJob,
    SyntheticJobBatch,
)
from compute_horde_validator.validator.synthetic_jobs.utils import execute_miner_synthetic_jobs
from compute_horde_validator.validator.tests.transport import MinerSimulationTransport

from .mock_generator import (
    TimeTookScoreMockSyntheticJobGeneratorFactory,
)

MANIFEST_INCENTIVE_MULTIPLIER = 1.05
MANIFEST_DANCE_RATIO_THRESHOLD = 1.4


@pytest.fixture(autouse=True)
def _override_config():
    with override_config(
        DYNAMIC_MANIFEST_SCORE_MULTIPLIER=MANIFEST_INCENTIVE_MULTIPLIER,
        DYNAMIC_MANIFEST_DANCE_RATIO_THRESHOLD=MANIFEST_DANCE_RATIO_THRESHOLD,
    ):
        yield


@patch(
    "compute_horde_validator.validator.synthetic_jobs.generator.current.synthetic_job_generator_factory",
    TimeTookScoreMockSyntheticJobGeneratorFactory(),
)
@pytest.mark.asyncio
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
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
    batch: SyntheticJobBatch,
    miner_hotkey: str,
    miner_axon_info: bittensor.AxonInfo,
    miner_client: MinerClient,
    transport: MinerSimulationTransport,
    override_weights_version_v2,
):
    job_uuids = [uuid.uuid4() for _ in range(curr_online_executor_count)]

    def make_uuid_generator():
        uuids = job_uuids.copy()

        def _generate():
            return uuids.pop(0)

        return _generate

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

    async def add_job_messages(request_class, send_before=1, **kwargs):
        for job_uuid in job_uuids:
            msg = request_class(
                job_uuid=str(job_uuid),
                **kwargs,
            ).model_dump_json()
            await transport.add_message(msg, send_before=send_before)

    await add_job_messages(miner_requests.V0ExecutorReadyRequest, send_before=1)
    await add_job_messages(miner_requests.V0AcceptJobRequest, send_before=2)
    await add_job_messages(
        miner_requests.V0JobFinishedRequest,
        send_before=0,
        docker_process_stdout="",
        docker_process_stderr="",
    )

    await asyncio.wait_for(
        execute_miner_synthetic_jobs(
            batch.pk,
            miner.pk,
            miner_hotkey,
            miner_axon_info,
            prev_online_executor_count,
            miner_client,
            generate_job_uuid=make_uuid_generator(),
        ),
        timeout=1,
    )

    job_finished_messages = miner_client.transport.sent[-len(job_uuids) :]

    for msg in job_finished_messages:
        receipt = json.loads(msg)

        job_uuid = receipt["payload"]["job_uuid"]

        time_took_us = receipt["payload"]["time_took_us"]
        time_took = time_took_us / 1_000_000

        job = await SyntheticJob.objects.aget(job_uuid=job_uuid)

        assert abs(job.score * time_took - expected_multiplier) < 0.0001
