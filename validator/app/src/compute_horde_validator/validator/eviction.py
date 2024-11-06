from datetime import timedelta

from compute_horde.receipts.models import JobAcceptedReceipt, JobFinishedReceipt, JobStartedReceipt
from django.conf import settings
from django.utils.timezone import now

from .models import OrganicJob, SolveWorkload, SyntheticJobBatch, SystemEvent

RECEIPTS_RETENTION_PERIOD = timedelta(days=2)
SYNTHETIC_JOBS_RETENTION_PERIOD = timedelta(days=2)
ORGANIC_JOBS_RETENTION_PERIOD = timedelta(days=2)
SYSTEM_EVENTS_RETENTION_PERIOD = timedelta(days=2)
PROMPTS_RETENTION_PERIOD = timedelta(days=2)


def evict_all() -> None:
    evict_organic_jobs()
    evict_receipts()
    evict_synthetic_jobs()
    evict_dangling_prompts()
    evict_system_events()


def evict_system_events() -> None:
    cutoff = now() - SYSTEM_EVENTS_RETENTION_PERIOD
    SystemEvent.objects.using(settings.DEFAULT_DB_ALIAS).filter(timestamp__lt=cutoff).delete()


def evict_synthetic_jobs() -> None:
    cutoff = now() - SYNTHETIC_JOBS_RETENTION_PERIOD
    SyntheticJobBatch.objects.filter(created_at__lt=cutoff).delete()


def evict_dangling_prompts():
    # Remove SolveWorkload's that are dangling after PromptSample's get deleted
    # along with SyntheticJobBatch (cascading: SyntheticJobBatch->SyntheticJob->PromptSample).
    cutoff = now() - PROMPTS_RETENTION_PERIOD
    SolveWorkload.objects.filter(samples__isnull=True, created_at__lt=cutoff).delete()


def evict_organic_jobs() -> None:
    cutoff = now() - ORGANIC_JOBS_RETENTION_PERIOD
    OrganicJob.objects.filter(updated_at__lt=cutoff).delete()


def evict_receipts() -> None:
    cutoff = now() - RECEIPTS_RETENTION_PERIOD
    JobStartedReceipt.objects.filter(timestamp__lt=cutoff).delete()
    JobAcceptedReceipt.objects.filter(timestamp__lt=cutoff).delete()
    JobFinishedReceipt.objects.filter(timestamp__lt=cutoff).delete()
