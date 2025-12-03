from datetime import timedelta

import pytest
from django.utils.timezone import now

from project.core.eviction import JOBS_RETENTION_PERIOD, evict_jobs
from project.core.models import CheatedJobReport, Job


@pytest.mark.django_db(transaction=True)
def test_evict_jobs__old_jobs_deleted(user, validator, signature, dummy_job_params):
    """
    A job beyond the retention period should be evicted
    """
    cutoff = now() - JOBS_RETENTION_PERIOD

    old_job = Job.objects.create(
        user=user,
        validator=validator,
        target_validator_hotkey=validator.ss58_address,
        signature=signature.model_dump(),
        created_at=cutoff - timedelta(days=1),
        **dummy_job_params,
    )

    evict_jobs()

    assert not Job.objects.filter(pk=old_job.pk).exists()


@pytest.mark.django_db(transaction=True)
def test_evict_jobs__new_jobs_kept(user, validator, signature, dummy_job_params):
    """
    A job within the retention period should not be evicted
    """
    cutoff = now() - JOBS_RETENTION_PERIOD

    new_job = Job.objects.create(
        user=user,
        validator=validator,
        target_validator_hotkey=validator.ss58_address,
        signature=signature.model_dump(),
        created_at=cutoff + timedelta(days=1),
        **dummy_job_params,
    )

    evict_jobs()

    assert Job.objects.filter(pk=new_job.pk).exists()


@pytest.mark.django_db(transaction=True)
def test_evict_jobs__old_cheated_job_deleted_with_report(user, validator, signature, dummy_job_params):
    """
    Eviction should not be blocked by an existing cheat report
    The newer trusted job should persist even if the cheated job is deleted - it's within retention period
    """
    cutoff = now() - JOBS_RETENTION_PERIOD

    old_cheated_job = Job.objects.create(
        user=user,
        validator=validator,
        target_validator_hotkey=validator.ss58_address,
        signature=signature.model_dump(),
        created_at=cutoff - timedelta(days=1),
        **dummy_job_params,
    )

    new_trusted_job = Job.objects.create(
        user=user,
        validator=validator,
        target_validator_hotkey=validator.ss58_address,
        signature=signature.model_dump(),
        created_at=cutoff + timedelta(days=1),
        **dummy_job_params,
    )

    report = CheatedJobReport.objects.create(
        job=old_cheated_job,
        trusted_job=new_trusted_job,
    )

    evict_jobs()

    assert not Job.objects.filter(pk=old_cheated_job.pk).exists()
    assert Job.objects.filter(pk=new_trusted_job.pk).exists()
    assert not CheatedJobReport.objects.filter(pk=report.pk).exists()


@pytest.mark.django_db(transaction=True)
def test_evict_jobs__both_old_jobs_deleted_with_report(user, validator, signature, dummy_job_params):
    """
    Eviction should still work fine if both cheated and trusted jobs are to be removed
    """
    cutoff = now() - JOBS_RETENTION_PERIOD

    old_cheated_job = Job.objects.create(
        user=user,
        validator=validator,
        target_validator_hotkey=validator.ss58_address,
        signature=signature.model_dump(),
        created_at=cutoff - timedelta(days=1),
        **dummy_job_params,
    )

    old_trusted_job = Job.objects.create(
        user=user,
        validator=validator,
        target_validator_hotkey=validator.ss58_address,
        signature=signature.model_dump(),
        created_at=cutoff - timedelta(days=2),
        **dummy_job_params,
    )

    report = CheatedJobReport.objects.create(
        job=old_cheated_job,
        trusted_job=old_trusted_job,
    )

    evict_jobs()

    assert not Job.objects.filter(pk=old_cheated_job.pk).exists()
    assert not Job.objects.filter(pk=old_trusted_job.pk).exists()
    assert not CheatedJobReport.objects.filter(pk=report.pk).exists()
