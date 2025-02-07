import asyncio
import re
import uuid
from collections.abc import Callable
from unittest.mock import patch

import bittensor
import pytest
import pytest_asyncio
from compute_horde.executor_class import ExecutorClass
from compute_horde.mv_protocol import miner_requests
from pytest_httpx import HTTPXMock

from compute_horde_validator.validator.models import (
    Miner,
    Prompt,
    PromptSample,
    PromptSeries,
    SolveWorkload,
    SyntheticJob,
)
from compute_horde_validator.validator.s3 import get_public_url
from compute_horde_validator.validator.synthetic_jobs.batch_run import execute_synthetic_batch_run
from compute_horde_validator.validator.synthetic_jobs.generator.base import (
    BaseSyntheticJobGenerator,
)
from compute_horde_validator.validator.synthetic_jobs.generator.factory import (
    DefaultSyntheticJobGeneratorFactory,
)
from compute_horde_validator.validator.tests.transport import SimulationTransport


class JobGeneratorFactory(DefaultSyntheticJobGeneratorFactory):
    async def create(self, executor_class: ExecutorClass, **kwargs) -> BaseSyntheticJobGenerator:
        generator = await super().create(executor_class, **kwargs)
        generator._uuid = self._uuid
        return generator


@pytest_asyncio.fixture
def mocked_job_generator_factory(prompts):
    factory = JobGeneratorFactory()
    with patch(
        "compute_horde_validator.validator.synthetic_jobs.generator.current.synthetic_job_generator_factory",
        factory,
    ):
        yield factory


@pytest.mark.asyncio
@pytest.mark.django_db(databases=["default", "default_alias"], transaction=True)
async def test_llm_synthetic_jobs_flow(
    miner: Miner,
    axon_dict: dict[str, bittensor.AxonInfo],
    create_simulation_miner_client: Callable,
    transport: SimulationTransport,
    override_weights_version_v2,
    small_spin_up_times,
    prompt_series: PromptSeries,
    solve_workload: SolveWorkload,
    prompt_sample: PromptSample,
    prompts: list[Prompt],
    mocked_job_generator_factory: JobGeneratorFactory,
    httpx_mock: HTTPXMock,
    settings,
):
    job_uuid = str(uuid.uuid4())
    mocked_job_generator_factory._uuid = job_uuid
    httpx_mock.add_response(
        url=re.compile(
            get_public_url(key=".*", bucket_name=settings.S3_BUCKET_NAME_ANSWERS, prefix="solved/")
        ),
        json={p.content: p.answer for p in prompts},
    )

    manifest_message = miner_requests.V0ExecutorManifestRequest(
        manifest=miner_requests.ExecutorManifest(
            executor_classes=[
                miner_requests.ExecutorClassManifest(
                    executor_class=ExecutorClass.always_on__llm__a6000,
                    count=1,
                )
            ]
        )
    ).model_dump_json()
    await transport.add_message(manifest_message, send_before=1)

    await transport.add_message(
        miner_requests.V0AcceptJobRequest(job_uuid=job_uuid).model_dump_json(),
        send_before=1,
        sleep_before=0.05,
    )
    await transport.add_message(
        miner_requests.V0ExecutorReadyRequest(job_uuid=job_uuid).model_dump_json(),
        send_before=0,
    )
    await transport.add_message(
        miner_requests.V0JobFinishedRequest(
            job_uuid=job_uuid,
            docker_process_stdout="",
            docker_process_stderr="",
        ).model_dump_json(),
        send_before=2,
        sleep_before=0.05,
    )

    assert prompt_sample.synthetic_job_id is None

    await asyncio.wait_for(
        execute_synthetic_batch_run(
            axon_dict,
            [miner],
            [],
            create_miner_client=create_simulation_miner_client,
        ),
        timeout=2,
    )

    job = await SyntheticJob.objects.aget(job_uuid=job_uuid)
    assert job.status == SyntheticJob.Status.COMPLETED
    assert job.score > 0

    await prompt_sample.arefresh_from_db()
    assert prompt_sample.synthetic_job_id == job.id
