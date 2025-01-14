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


async def check_synthetic_job(job_uuid: uuid.UUID, miner_id: int, status: str, score: float):
    job = await SyntheticJob.objects.aget(job_uuid=job_uuid)
    assert job.miner_id == miner_id, f"{job.miner_id} != {miner_id}"
    assert job.status == status, f"{job.status} != {status}"
    assert job.score == score, f"{job.score} != {score}"


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
                content="mock",
                answer="mock",
            )
            for prompt_sample in prompt_samples
        ]
    )
    return prompts, prompt_samples
