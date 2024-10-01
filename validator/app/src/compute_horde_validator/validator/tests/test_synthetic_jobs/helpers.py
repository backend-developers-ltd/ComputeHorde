import uuid

from compute_horde_validator.validator.models import SyntheticJob


async def check_synthetic_job(job_uuid: uuid.UUID, miner_id: int, status: str, score: float):
    job = await SyntheticJob.objects.aget(job_uuid=job_uuid)
    assert job.miner_id == miner_id
    assert job.status == status
    assert job.score == score
