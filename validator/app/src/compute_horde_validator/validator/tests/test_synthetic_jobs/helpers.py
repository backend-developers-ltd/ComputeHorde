import re
import secrets
import uuid
from collections.abc import Sequence
from datetime import timedelta

from django.utils.timezone import now

from compute_horde_validator.validator.models import (
    Prompt,
    PromptSample,
    PromptSeries,
    SolveWorkload,
    SyntheticJob,
    SystemEvent,
)


def generate_related_uuid(input_uuid: uuid.UUID):
    """Generate one UUID based on another in a stable manner - the output ONLY relies on the input, but they don't
    look similar"""

    namespace_uuid = uuid.UUID("12345678123456781234567812345678")
    related_uuid = uuid.uuid5(namespace_uuid, str(input_uuid))
    return related_uuid


async def check_synthetic_job(
    job_uuid: uuid.UUID, miner_id: int, status: str, score: float, comment: re.Pattern | None = None
):
    job = await SyntheticJob.objects.aget(job_uuid=job_uuid)
    assert (job.miner_id, job.status, job.score, True) == (
        miner_id,
        status,
        score,
        bool(comment.match(job.comment)) if comment else True,
    ), (
        f"{job.miner_id=} != {miner_id=} or {job.status=} != {status=} or {job.score=} != {score=} "
        f"or '{job.comment}' does not match '{comment}'"
    )


async def check_miner_job_system_events(
    expected: Sequence[tuple[SystemEvent.EventType, SystemEvent.EventSubType]],
    miner_hotkey: str,
    job_uuid: str | uuid.UUID | None = None,
):
    qs = SystemEvent.objects.filter(data__miner_hotkey=miner_hotkey, data__job_uuid=str(job_uuid))
    results = {(event.type, event.subtype) async for event in qs}

    assert results == set(expected), f"Expected {expected}\ngot {results}"


async def generate_prompts(num_miners: int = 1) -> (list[Prompt], list[PromptSample]):
    current_time = now()

    prompt_series = await PromptSeries.objects.acreate(
        s3_url="https://example.com/series/mock_series",
        generator_version=1,
    )

    workloads = [
        SolveWorkload(
            seed=i,
            s3_url=f"https://example.com/workload/mock_workload_{i}",
            created_at=current_time,
            finished_at=current_time + timedelta(hours=2),
        )
        for i in range(num_miners)
    ]
    created_workloads = await SolveWorkload.objects.abulk_create(workloads)

    prompt_samples = await PromptSample.objects.abulk_create(
        [
            PromptSample(
                series=prompt_series,
                workload=workload,
                synthetic_job=None,
                created_at=current_time,
            )
            for workload in created_workloads
        ]
    )
    prompts = await Prompt.objects.abulk_create(
        [
            Prompt(
                sample=prompt_sample,
                content=secrets.token_urlsafe(),
                answer=secrets.token_urlsafe(),
            )
            for prompt_sample in prompt_samples
        ]
    )
    return prompts, prompt_samples
