import threading
from concurrent.futures import ThreadPoolExecutor

import pytest
from django.db import connection, transaction
from django.test.utils import CaptureQueriesContext

from ..models import Job, JobStatus, Miner, Validator


@pytest.mark.django_db(transaction=True)
def test__job__selecting_validator__failure__not_exist(user):
    with pytest.raises(Validator.DoesNotExist):
        Job.objects.create(
            user=user,
            validator=None,
        )


@pytest.mark.django_db(transaction=True)
def test__job__selecting_validator__failure__not_connected(user, validator):
    """Check that new job is not assigned to a validator which exists but is not connected"""
    with pytest.raises(Validator.DoesNotExist):
        Job.objects.create(
            user=user,
            validator=None,
        )


@pytest.mark.django_db(transaction=True)
def test__job__selecting_validator__failure__not_authorized(user, validator):
    """Check that new job is not assigned to connected but not authorized validator"""
    with pytest.raises(Validator.DoesNotExist):
        Job.objects.create(
            user=user,
            validator=None,
        )


@pytest.mark.django_db(transaction=True)
def test__job__selecting_validator__success(user, connected_validator, miner):
    """Check that new job is assigned to connected and authorized validator"""
    job = Job.objects.create(
        user=user,
        validator=None,
    )
    assert job.validator == connected_validator


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test__job__selecting_miner__doesnt_exist(user, validator):
    """Check that new job is not created when there are no miners"""
    with pytest.raises(Miner.DoesNotExist):
        await Job.objects.acreate(
            user=user,
            validator=validator,
            miner=None,
        )


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test__job__selecting_miner__unavailable(
    user,
    validator,
    inactive_miner,
    active_miner_with_executing_job,
):
    """Check that new job is not created when all miners are busy"""

    with pytest.raises(Miner.DoesNotExist):
        await Job.objects.acreate(
            user=user,
            validator=validator,
            miner=None,
        )


@pytest.mark.django_db(transaction=True)
def test__job__selecting_miner__success(
    user,
    validator,
    inactive_miner,
    active_miner_with_executing_job,
    miner,
    miner_with_recently_completed_jobs,
    miner_with_old_completed_jobs,
    miner_with_old_active_jobs,
    dummy_job_params,
):
    """
    Check that new job is assigned to available miners in correct order.

    The expected order is:
    - miner with no jobs at all
    - miner with old completed jobs
    - miner with old active jobs
    - miner with recently completed jobs
    """
    miner_with_no_jobs = miner

    job = Job.objects.create(user=user, validator=validator, miner=None, **dummy_job_params)
    assert job.miner == miner_with_no_jobs

    job = Job.objects.create(user=user, validator=validator, miner=None, **dummy_job_params)
    assert job.miner == miner_with_old_completed_jobs

    job = Job.objects.create(user=user, validator=validator, miner=None, **dummy_job_params)
    assert job.miner == miner_with_old_active_jobs

    job = Job.objects.create(user=user, validator=validator, miner=None, **dummy_job_params)
    assert job.miner == miner_with_recently_completed_jobs

    with pytest.raises(Miner.DoesNotExist):
        Job.objects.create(user=user, validator=validator, miner=None, **dummy_job_params)


@pytest.mark.django_db(transaction=True)
def test__job__selecting_miner__success_after_completing_job(
    user,
    validator,
    active_miner_with_executing_job,
    dummy_job_params,
):
    """Check that new job can be assigned to a miner after completing job"""

    with pytest.raises(Miner.DoesNotExist):
        Job.objects.create(
            user=user,
            validator=validator,
            miner=None,
            **dummy_job_params,
        )

    job = active_miner_with_executing_job.jobs.order_by("-created_at")[0]
    JobStatus.objects.create(job=job, status=JobStatus.Status.COMPLETED)

    job = Job.objects.create(
        user=user,
        validator=validator,
        miner=None,
        **dummy_job_params,
    )
    assert job.miner == active_miner_with_executing_job


@pytest.mark.django_db(transaction=True)
def test__job__selecting_miner__synchronization(
    user,
    validator,
    inactive_miner,
    active_miner_with_executing_job,
    miner,
    miner_with_recently_completed_jobs,
    dummy_job_params,
):
    start_event = threading.Event()

    def create_job():
        start_event.wait()
        job = Job.objects.create(user=user, validator=validator, miner=None, **dummy_job_params)
        return job.miner.ss58_address

    with ThreadPoolExecutor(max_workers=4) as pool:
        selected_miner_1 = pool.submit(create_job)
        selected_miner_2 = pool.submit(create_job)

        start_event.set()
        assert selected_miner_1.result() != selected_miner_2.result()


@pytest.mark.django_db(transaction=True)
def test__job__selecting_miner__constant_queries(
    user,
    validator,
    miner,
    dummy_job_params,
):
    """The number of db queries while selecting a miner should be constant regardless of the number of jobs"""

    def create_job():
        job = Job.objects.create(
            user=user,
            validator=validator,
            miner=miner,
            **dummy_job_params,
        )
        JobStatus.objects.create(job=job, status=JobStatus.Status.ACCEPTED)
        JobStatus.objects.create(job=job, status=JobStatus.Status.COMPLETED)
        return job

    def get_query_count():
        job = Job.objects.all()[0]
        with CaptureQueriesContext(connection) as context:
            with transaction.atomic():
                job.select_miner()
        return len(context)

    create_job()
    count1 = get_query_count()

    create_job()
    count2 = get_query_count()

    assert count1 == count2
