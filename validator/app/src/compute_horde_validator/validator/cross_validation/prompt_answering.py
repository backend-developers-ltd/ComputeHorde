import logging
import uuid
from datetime import datetime

import bittensor_wallet
from asgiref.sync import sync_to_async
from compute_horde.executor_class import EXECUTOR_CLASS
from compute_horde.miner_client.organic import (
    MinerRejectedJob,
    OrganicJobDetails,
    execute_organic_job_on_miner,
)
from compute_horde.protocol_consts import JobRejectionReason
from compute_horde_core.executor_class import ExecutorClass
from django.conf import settings
from django.db import transaction
from django.utils.timezone import now

from compute_horde_validator.validator.cross_validation.utils import (
    TrustedMinerClient,
    trusted_miner_not_configured_system_event,
)
from compute_horde_validator.validator.dynamic_config import aget_config
from compute_horde_validator.validator.models import Prompt, SolveWorkload, SystemEvent
from compute_horde_validator.validator.synthetic_jobs.generator.llm_prompts import (
    LlmPromptsJobGenerator,
)
from compute_horde_validator.validator.utils import TRUSTED_MINER_FAKE_KEY

logger = logging.getLogger(__name__)

MIN_SPIN_UP_TIME = 10


def _get_keypair() -> bittensor_wallet.Keypair:
    return settings.BITTENSOR_WALLET().get_hotkey()


async def answer_prompts(
    workload: SolveWorkload,
    create_miner_client=TrustedMinerClient,
    job_uuid: uuid.UUID | None = None,
) -> bool:
    if not all(
        [
            settings.TRUSTED_MINER_ADDRESS,
            settings.TRUSTED_MINER_PORT,
        ]
    ):
        await trusted_miner_not_configured_system_event(SystemEvent.EventType.LLM_PROMPT_ANSWERING)
        logger.warning("Trusted generation miner not configured, skipping prompt answering")
        return False

    ts = datetime.now()
    seed = workload.seed

    job_generator = LlmPromptsJobGenerator(workload.s3_url, seed)
    await job_generator.ainit(miner_hotkey=TRUSTED_MINER_FAKE_KEY)

    # TODO: Should be generated for all the llm executor classes.
    #       SolveWorkload/PromptSample should have a executor_class field saying which
    #       executor_class this sample is for.
    job_uuid = job_uuid or uuid.uuid4()
    job_details = OrganicJobDetails(
        job_uuid=str(job_uuid),
        executor_class=ExecutorClass.always_on__llm__a6000,
        docker_image=job_generator.docker_image_name(),
        docker_run_options_preset=job_generator.docker_run_options_preset(),
        docker_run_cmd=job_generator.docker_run_cmd(),
        total_job_timeout=(
            job_generator.timeout_seconds()
            + max(
                EXECUTOR_CLASS[ExecutorClass.always_on__llm__a6000].spin_up_time,
                MIN_SPIN_UP_TIME,
            )
        ),
        volume=await job_generator.volume(),
        output=await job_generator.output_upload(),
    )

    miner_client = create_miner_client(
        miner_address=settings.TRUSTED_MINER_ADDRESS,
        miner_port=settings.TRUSTED_MINER_PORT,
        job_uuid=str(job_uuid),
        my_keypair=_get_keypair(),
    )

    try:
        await execute_organic_job_on_miner(
            miner_client,
            job_details,
            reservation_time_limit=await aget_config("DYNAMIC_EXECUTOR_RESERVATION_TIME_LIMIT"),
            executor_startup_time_limit=await aget_config("DYNAMIC_EXECUTOR_STARTUP_TIME_LIMIT"),
        )

    except MinerRejectedJob as rejection:
        if rejection.msg.reason == JobRejectionReason.BUSY:
            # Skip sending the system event
            logger.info("Failed to run answer_prompts: trusted miner is busy")
            return False
        await SystemEvent.objects.acreate(
            type=SystemEvent.EventType.LLM_PROMPT_ANSWERING,
            subtype=SystemEvent.EventSubType.JOB_REJECTED,
            long_description=f"Trusted miner failed to run prompt answering job: {rejection!r}",
            data={},
        )
        logger.warning("Failed to run answer_prompts: trusted miner rejected job", exc_info=True)
        return False

    except Exception as e:
        await SystemEvent.objects.acreate(
            type=SystemEvent.EventType.LLM_PROMPT_ANSWERING,
            subtype=SystemEvent.EventSubType.FAILURE,
            long_description=f"Trusted miner failed to run prompt answering job: {e!r}",
            data={},
        )
        logger.warning("Failed to run organic job", exc_info=True)
        return False

    try:
        await job_generator.download_answers()
        prompt_answers: dict[str, str] = job_generator.prompt_answers
    except Exception as e:
        await SystemEvent.objects.acreate(
            type=SystemEvent.EventType.LLM_PROMPT_ANSWERING,
            subtype=SystemEvent.EventSubType.ERROR_DOWNLOADING_FROM_S3,
            long_description=f"Failed to download prompt answers: {e!r}",
            data={},
        )
        logger.warning("Failed to download prompt answers", exc_info=True)
        return False

    success = await sync_to_async(save_workload_answers)(workload, prompt_answers)
    duration_seconds = (datetime.now() - ts).total_seconds()
    logger.info(f"Workload {workload} finished in {duration_seconds} seconds")
    return success


def get_workload_prompts(workload: SolveWorkload) -> list[Prompt]:
    return [
        x
        for x in Prompt.objects.select_related("sample").filter(
            sample__workload_id=workload.id, answer__isnull=True
        )
    ]


def save_workload_answers(workload: SolveWorkload, prompt_answers) -> bool:
    prompts = get_workload_prompts(workload)

    # update the prompts with the answers
    for prompt in prompts:
        if prompt.content in prompt_answers:
            prompt.answer = prompt_answers[prompt.content]
        else:
            logger.error(f"Prompt {prompt} was not found in the generated answers")
            SystemEvent.objects.create(
                type=SystemEvent.EventType.LLM_PROMPT_ANSWERING,
                subtype=SystemEvent.EventSubType.LLM_PROMPT_ANSWERS_MISSING,
                long_description="Prompt answer not found in the prompt answering job output",
                data={
                    "unanswered_prompt_content": prompt.content,
                    "workload_id": str(workload.workload_uuid),
                    "prompt_sample_id": prompt.sample.id,
                },
            )
            # leave workload as unfinished so that it can be re-processed
            return False

    with transaction.atomic():
        # update the workload as finished
        workload.finished_at = now()
        workload.save()
        Prompt.objects.bulk_update(prompts, ["answer"])
    return True
