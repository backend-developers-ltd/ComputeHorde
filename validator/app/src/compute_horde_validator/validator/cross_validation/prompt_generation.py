import logging
import uuid
from collections.abc import Callable, Iterable

import bittensor
from compute_horde.miner_client.organic import (
    run_organic_job,
)
from django.conf import settings
from django.utils.timezone import now

from compute_horde_validator.validator.cross_validation.utils import (
    TrustedMinerClient,
)
from compute_horde_validator.validator.dynamic_config import aget_config
from compute_horde_validator.validator.models import PromptSeries, SystemEvent
from compute_horde_validator.validator.s3 import generate_upload_url, get_public_url

from .generator.current import prompt_job_generator
from .utils import trusted_miner_not_configured_system_event

logger = logging.getLogger(__name__)


async def generate_prompts(
    *,
    create_miner_client: Callable[..., TrustedMinerClient] | None = None,
    job_uuid: uuid.UUID | None = None,
    wait_timeout: int | None = None,
) -> None:
    started_at = now()

    if not all(
        [
            settings.TRUSTED_MINER_ADDRESS,
            settings.TRUSTED_MINER_PORT,
        ]
    ):
        await trusted_miner_not_configured_system_event(SystemEvent.EventType.LLM_PROMPT_GENERATION)
        logger.warning("Trusted miner not configured, skipping prompt generation")
        return

    job_uuid = job_uuid or uuid.uuid4()

    num_batches = await aget_config("DYNAMIC_PROMPTS_SERIES_IN_A_SINGLE_GENERATION")
    num_prompts_per_batch = await aget_config("DYNAMIC_NUMBER_OF_PROMPTS_IN_SERIES")

    series_uuids, upload_urls, public_urls = _generate_uuids_and_urls(num_batches)

    job_generator = prompt_job_generator(
        job_uuid,
        num_prompts_per_batch=num_prompts_per_batch,
        batch_uuids=series_uuids,
        upload_urls=upload_urls,
    )
    job_details = job_generator.get_job_details()

    create_miner_client = create_miner_client or TrustedMinerClient
    wait_timeout = wait_timeout or job_generator.timeout_seconds()

    miner_client = create_miner_client(
        miner_address=settings.TRUSTED_MINER_ADDRESS,
        miner_port=settings.TRUSTED_MINER_PORT,
        job_uuid=str(job_uuid),
        my_keypair=_get_keypair(),
    )

    try:
        await run_organic_job(miner_client, job_details, executor_ready_timeout=wait_timeout)
    except Exception as e:
        await SystemEvent.objects.acreate(
            type=SystemEvent.EventType.LLM_PROMPT_GENERATION,
            subtype=SystemEvent.EventSubType.FAILURE,
            long_description=f"Trusted miner failed to run prompt generation job: {e!r}",
            data={},
        )
        logger.warning("Failed to run organic job", exc_info=True)
        return

    await _persist_series_list(series_uuids, public_urls, job_generator.generator_version())

    completed_at = now()
    await SystemEvent.objects.acreate(
        type=SystemEvent.EventType.LLM_PROMPT_GENERATION,
        subtype=SystemEvent.EventSubType.SUCCESS,
        long_description="",
        data={
            "started_at": started_at.isoformat(),
            "completed_at": completed_at.isoformat(),
            "duration": (completed_at - started_at).total_seconds(),
            "count": len(series_uuids),
        },
    )


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
