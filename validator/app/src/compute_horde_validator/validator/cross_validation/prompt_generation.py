import logging
import uuid
from collections.abc import Callable, Iterable

import bittensor
from compute_horde.miner_client.organic import (
    OrganicMinerClient,
    run_organic_job,
)
from django.conf import settings

from compute_horde_validator.validator.dynamic_config import aget_config
from compute_horde_validator.validator.models import PromptSeries
from compute_horde_validator.validator.s3 import generate_upload_url, get_public_url

from .generator.current import prompt_job_generator

logger = logging.getLogger(__name__)


async def generate_prompts(
    *,
    create_miner_client: Callable[..., OrganicMinerClient] | None = None,
    job_uuid: uuid.UUID | None = None,
    wait_timeout: int | None = None,
) -> None:
    limit = await aget_config("DYNAMIC_MAX_PROMPT_BATCHES")
    if current_count := await PromptSeries.objects.acount() >= limit:
        logger.warning(
            "There are %s series in the db exceeding the limit of %s, skipping prompt generation",
            current_count,
            limit,
        )
        return

    job_uuid = job_uuid or uuid.uuid4()

    num_batches = await aget_config("DYNAMIC_PROMPTS_BATCHES_IN_A_SINGLE_GO")
    num_prompts_per_batch = await aget_config("DYNAMIC_NUMBER_OF_PROMPTS_IN_BATCH")

    series_uuids, upload_urls, public_urls = _generate_uuids_and_urls(num_batches)

    job_generator = prompt_job_generator(
        job_uuid,
        num_prompts_per_batch=num_prompts_per_batch,
        batch_uuids=series_uuids,
        upload_urls=upload_urls,
    )
    job_details = job_generator.get_job_details()

    create_miner_client = create_miner_client or OrganicMinerClient
    wait_timeout = wait_timeout or job_generator.timeout_seconds()

    miner_client = create_miner_client(
        miner_hotkey=settings.GENERATION_MINER_KEY,
        miner_address=settings.GENERATION_MINER_ADDRESS,
        miner_port=settings.GENERATION_MINER_PORT,
        job_uuid=str(job_uuid),
        my_keypair=_get_keypair(),
    )

    await run_organic_job(miner_client, job_details, wait_timeout=wait_timeout)

    await _persist_series_list(series_uuids, public_urls, job_generator.generator_version())


def _generate_uuids_and_urls(num_batches: int) -> tuple[list[uuid.UUID], list[str], list[str]]:
    series_uuids = [uuid.uuid4() for _ in range(num_batches)]
    upload_urls = [
        generate_upload_url(str(_uuid), bucket_name=settings.S3_BUCKET_NAME_PROMPTS)
        for _uuid in series_uuids
    ]

    public_urls = [
        get_public_url(str(_uuid), bucket_name=settings.S3_BUCKET_NAME_PROMPTS)
        for _uuid in series_uuids
    ]

    return series_uuids, upload_urls, public_urls


async def _persist_series_list(
    series_uuids: Iterable[uuid.UUID], urls: list[str], generator_version: int
) -> list[PromptSeries]:
    objs = [
        PromptSeries(
            series_uuid=_uuid,
            s3_url=url,
            generator_version=generator_version,
        )
        for _uuid, url in zip(series_uuids, urls)
    ]

    return await PromptSeries.objects.abulk_create(objs)


def _get_keypair() -> bittensor.Keypair:
    return settings.BITTENSOR_WALLET().get_hotkey()
