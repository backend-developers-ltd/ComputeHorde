import uuid
from collections.abc import Callable

import pytest
from compute_horde.base.output_upload import MultiUpload
from compute_horde.miner_client.organic import OrganicJobError
from compute_horde.mv_protocol.validator_requests import BaseValidatorRequest

from compute_horde_validator.validator.cross_validation.prompt_generation import generate_prompts
from compute_horde_validator.validator.models import PromptSeries
from compute_horde_validator.validator.tests.transport import MinerSimulationTransport

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.django_db(transaction=True),
    pytest.mark.override_config(
        DYNAMIC_MAX_PROMPT_BATCHES=5,
        DYNAMIC_PROMPTS_BATCHES_IN_A_SINGLE_GO=3,
        DYNAMIC_NUMBER_OF_PROMPTS_IN_BATCH=99,
    ),
]


async def test_generate_prompts(
    transport: MinerSimulationTransport,
    create_miner_client: Callable,
    manifest_message: str,
    executor_ready_message: str,
    accept_job_message: str,
    job_finish_message: str,
    job_uuid: uuid.UUID,
):
    await transport.add_message(manifest_message, send_before=1)
    await transport.add_message(accept_job_message, send_before=1)
    await transport.add_message(executor_ready_message, send_before=0)
    await transport.add_message(job_finish_message, send_before=2)

    await generate_prompts(
        create_miner_client=create_miner_client, job_uuid=job_uuid, wait_timeout=2
    )

    series_uuids = []
    async for series in PromptSeries.objects.all():
        _uuid = str(series.series_uuid)

        assert _uuid in series.s3_url
        # make sure we save the public url
        assert "Signature" not in series.s3_url

        series_uuids.append(_uuid)

    assert len(series_uuids) == 3

    job_request = BaseValidatorRequest.parse(transport.sent[-2])
    assert job_request.job_uuid == str(job_uuid)
    assert isinstance(job_request.output_upload, MultiUpload)

    uuids_from_uploads = []
    for upload in job_request.output_upload.uploads:
        assert upload.relative_path.startswith("prompts_")

        _uuid = upload.relative_path.split("_")[1].split(".")[0]

        assert "Signature" in upload.url
        assert _uuid in upload.url
        uuids_from_uploads.append(_uuid)

    assert set(uuids_from_uploads) == set(series_uuids)

    assert "99" in job_request.docker_run_cmd
    assert ",".join(series_uuids) in job_request.docker_run_cmd


async def test_generate_prompts_job_failed(
    transport: MinerSimulationTransport,
    create_miner_client: Callable,
    manifest_message: str,
    executor_ready_message: str,
    accept_job_message: str,
    job_failed_message: str,
    job_uuid: uuid.UUID,
):
    await transport.add_message(manifest_message, send_before=1)
    await transport.add_message(accept_job_message, send_before=1)
    await transport.add_message(executor_ready_message, send_before=0)
    await transport.add_message(job_failed_message, send_before=2)

    with pytest.raises(OrganicJobError):
        await generate_prompts(
            create_miner_client=create_miner_client, job_uuid=job_uuid, wait_timeout=2
        )

    assert not await PromptSeries.objects.aexists()


async def test_generate_prompts_timeout(
    transport: MinerSimulationTransport,
    create_miner_client: Callable,
    manifest_message: str,
    job_uuid: uuid.UUID,
):
    await transport.add_message(manifest_message, send_before=1)

    with pytest.raises(OrganicJobError):
        await generate_prompts(
            create_miner_client=create_miner_client, job_uuid=job_uuid, wait_timeout=0.5
        )

    assert not await PromptSeries.objects.aexists()


async def test_generate_prompts_max_batches_reached(
    create_miner_client: Callable,
    job_uuid: uuid.UUID,
):
    existing = []
    for _ in range(5):
        existing.append(PromptSeries(s3_url="", generator_version=1))
    await PromptSeries.objects.abulk_create(existing)

    await generate_prompts(
        create_miner_client=create_miner_client, job_uuid=job_uuid, wait_timeout=0.5
    )

    assert await PromptSeries.objects.acount() == 5
