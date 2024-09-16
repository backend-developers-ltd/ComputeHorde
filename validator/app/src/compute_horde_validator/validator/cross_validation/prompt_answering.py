import logging
import uuid
from datetime import datetime

import bittensor
from asgiref.sync import sync_to_async
from compute_horde.miner_client.organic import (
    OrganicJobDetails,
    OrganicMinerClient,
    run_organic_job,
)
from django.conf import settings
from django.db import transaction
from django.utils.timezone import now

from compute_horde_validator.validator.models import Prompt, SolveWorkload
from compute_horde_validator.validator.synthetic_jobs.generator.llama_prompts import (
    LlamaPromptsSyntheticJobGenerator,
)

logger = logging.getLogger(__name__)


async def get_workload_prompts(workload: SolveWorkload) -> list[Prompt]:
    return [
        x
        async for x in Prompt.objects.select_related("sample").filter(
            sample__workload_id=workload.id, answer__isnull=True
        )
    ]


def _get_keypair() -> bittensor.Keypair:
    return settings.BITTENSOR_WALLET().get_hotkey()


async def answer_prompts(
    workload: SolveWorkload,
    create_miner_client=OrganicMinerClient,
    job_uuid: uuid.UUID | None = None,
    wait_timeout: int | None = None,
) -> None:
    if not all(
        [
            settings.TRUSTED_MINER_KEY,
            settings.TRUSTED_MINER_ADDRESS,
            settings.TRUSTED_MINER_PORT,
        ]
    ):
        logger.warning("Prompt generation miner not configured, skipping prompt generation")
        return

    ts = datetime.now()
    seed = workload.seed
    prompts = await get_workload_prompts(workload)

    job_generator = LlamaPromptsSyntheticJobGenerator(None, prompts, workload.s3_url, seed)
    await job_generator.ainit()

    job_uuid = job_uuid or uuid.uuid4()
    job_details = OrganicJobDetails(
        job_uuid=str(job_uuid),
        docker_image=job_generator.docker_image_name(),
        raw_script=job_generator.raw_script(),
        docker_run_options_preset=job_generator.docker_run_options_preset(),
        docker_run_cmd=job_generator.docker_run_cmd(),
        total_job_timeout=job_generator.timeout_seconds(),
        volume=await job_generator.volume(),
        output=await job_generator.output_upload(),
    )

    wait_timeout = wait_timeout or job_generator.timeout_seconds()

    miner_client = create_miner_client(
        miner_hotkey=settings.TRUSTED_MINER_KEY,
        miner_address=settings.TRUSTED_MINER_ADDRESS,
        miner_port=settings.TRUSTED_MINER_PORT,
        job_uuid=str(job_uuid),
        my_keypair=_get_keypair(),
    )

    await run_organic_job(miner_client, job_details, wait_timeout=wait_timeout)

    try:
        prompt_answers: dict[str, str] = await job_generator.get_prompt_answers()
    except Exception:
        logger.error("Failed to download prompt answers", exc_info=True)
        return

    await sync_to_async(save_workload_answers)(workload, prompts, prompt_answers)
    duration_seconds = (datetime.now() - ts).total_seconds()
    logger.info(
        f"Workload {workload} answered {len(prompts)} prompts in {duration_seconds} seconds"
    )


def save_workload_answers(workload, prompts, prompt_answers):
    with transaction.atomic():
        # update the workload as finished
        workload.finished_at = now()
        workload.save()

        # update the prompts with the answers
        for prompt in prompts:
            if prompt.content in prompt_answers:
                prompt.answer = prompt_answers[prompt.content]
            else:
                logger.warning(f"Prompt {prompt} was not found in the prompt answers generated")
        Prompt.objects.bulk_update(prompts, ["answer"])