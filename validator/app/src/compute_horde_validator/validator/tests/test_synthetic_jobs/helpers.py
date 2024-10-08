import uuid
from collections.abc import Sequence

from compute_horde_validator.validator.models import SyntheticJob, SystemEvent


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
