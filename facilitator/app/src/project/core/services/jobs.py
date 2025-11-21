from datetime import timedelta
from functools import partial

import structlog
from compute_horde import protocol_consts
from django.db import transaction
from django.db.models import OuterRef, Subquery
from django.utils import timezone

from project.core.models import Job, JobStatus, Validator
from project.core.tasks import send_job_to_validator_task

logger = structlog.get_logger(__name__)


@transaction.atomic()
def create_job(*, validated_data: dict) -> Job:
    """
    Create a new `Job`, assign a `Validator` and dispatch it to the validator.

    Steps:
    1. Create the `Job` instance
       (set the `validator` based on `target_validator_hotkey`)
    2. Create the initial `JobStatus` (PENDING)
    3. On `transaction.on_commit`, send the job
       request to the validator using a Celery task
    """
    job = Job(**validated_data)
    job.validator = Validator.objects.get(ss58_address=job.target_validator_hotkey)
    job.save()
    JobStatus.objects.create(job=job, status=protocol_consts.JobStatus.PENDING.value)

    transaction.on_commit(partial(_enqueue_send_job_to_validator, job=job))

    return job


def _enqueue_send_job_to_validator(job: Job) -> None:
    """Enqueue the `send_job_to_validator` task after transaction commits."""
    try:
        send_job_to_validator_task.apply_async(args=(job.uuid,))
    except Exception as e:
        JobStatus.objects.create(job=job, status=protocol_consts.JobStatus.HORDE_FAILED.value)
        logger.exception("failed to enqueue job", job_uuid=job.uuid, error=str(e))


def timeout_sent_jobs() -> None:
    """Timeout Jobs that are "SENT" but not accepted."""
    cutoff = timezone.now() - timedelta(seconds=60)

    latest_job_statuses_qs = JobStatus.objects.filter(job=OuterRef("pk")).order_by("-created_at")
    jobs = Job.objects.annotate(latest_status=Subquery(latest_job_statuses_qs.values("status")[:1])).filter(
        created_at__lte=cutoff, latest_status__in=[protocol_consts.JobStatus.SENT.value]
    )

    for job in jobs:
        JobStatus.objects.create(job=job, status=protocol_consts.JobStatus.HORDE_FAILED.value)
        logger.debug("failing job because it is still in SENT status", job_uuid=job.uuid)
