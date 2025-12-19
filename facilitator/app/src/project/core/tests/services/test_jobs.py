import base64
import time
from datetime import datetime, timedelta
from typing import Any
from unittest import mock

import pytest
from compute_horde import protocol_consts
from django.contrib.auth.models import User
from django.utils import timezone
from freezegun import freeze_time

from project.core.models import Channel, Job, JobStatus, Signature, Validator
from project.core.services import jobs as jobs_services


@pytest.fixture
def user(db) -> User:
    return User.objects.create(username="job_test_user", email="job@test.com")


@pytest.fixture
def signature() -> Signature:
    return Signature(
        signature_type="dummy_signature_type",
        signatory="dummy_signatory",
        timestamp_ns=time.time_ns(),
        signature=base64.b64encode(b"dummy_signature"),
    )


@pytest.fixture
def validator(db) -> Validator:
    return Validator.objects.create(ss58_address="5HTestValidatorAddress", is_active=True)


@pytest.fixture
def connected_validator(db, validator: Validator) -> Validator:
    Channel.objects.create(name="test_channel", validator=validator)
    return validator


@pytest.fixture
def valid_job_data() -> dict[str, Any]:
    return {
        "docker_image": "alpine:latest",
        "args": ["echo", "hello"],
        "env": {"TEST_ENV": "test"},
        "download_time_limit": 300,
        "execution_time_limit": 300,
        "streaming_start_time_limit": 300,
        "upload_time_limit": 300,
        "use_gpu": False,
        "executor_class": "cpu-executor",
        "on_trusted_miner": False,
        "artifacts_dir": "/tmp/artifacts",
        "job_namespace": "test-namespace",
    }


@pytest.mark.django_db
def test_create_job_success(user, signature, connected_validator, valid_job_data):
    channel = connected_validator.channels.first()
    channel.last_heartbeat = timezone.now()
    channel.save()

    validated_data = valid_job_data.copy()
    validated_data.update(
        {"target_validator_hotkey": connected_validator.ss58_address, "user": user, "signature": signature.model_dump()}
    )

    with (
        mock.patch("django.db.transaction.on_commit", side_effect=lambda func: func()),
        mock.patch("project.core.services.jobs.send_job_to_validator_task.apply_async") as mock_task,
    ):
        job = jobs_services.create_job(validated_data=validated_data)

        assert job.pk is not None
        assert job.validator == connected_validator
        assert Job.objects.count() == 1

        assert JobStatus.objects.filter(job=job, status=protocol_consts.JobStatus.PENDING.value).exists()

        mock_task.assert_called_once_with(args=(job.uuid,))


@pytest.mark.django_db
def test_create_job_raises_exception_if_validator_inactive(user, signature, validator, valid_job_data):
    validated_data = valid_job_data.copy()
    validated_data.update(
        {"target_validator_hotkey": validator.ss58_address, "user": user, "signature": signature.model_dump()}
    )

    with pytest.raises(Validator.DoesNotExist):
        jobs_services.create_job(validated_data=validated_data)


@pytest.mark.django_db
def test_create_job_fails_if_task_enqueue_fails(user, signature, connected_validator, valid_job_data):
    channel = connected_validator.channels.first()
    channel.last_heartbeat = timezone.now()
    channel.save()

    validated_data = valid_job_data.copy()
    validated_data.update(
        {"target_validator_hotkey": connected_validator.ss58_address, "user": user, "signature": signature.model_dump()}
    )

    with (
        mock.patch("django.db.transaction.on_commit", side_effect=lambda func: func()),
        mock.patch(
            "project.core.services.jobs.send_job_to_validator_task.apply_async", side_effect=Exception("Queue down")
        ),
    ):
        job = jobs_services.create_job(validated_data=validated_data)

        assert job.pk is not None
        assert job.validator == connected_validator
        assert Job.objects.count() == 1

        assert list(JobStatus.objects.filter(job=job).order_by("created_at").values_list("status", flat=True)) == [
            protocol_consts.JobStatus.PENDING.value,
            protocol_consts.JobStatus.HORDE_FAILED.value,
        ]
        assert job.status.status == protocol_consts.JobStatus.HORDE_FAILED.value


@pytest.mark.django_db
def test_timeout_sent_jobs(user, validator, signature, valid_job_data):
    validated_data = valid_job_data.copy()
    validated_data.update(
        {"target_validator_hotkey": validator.ss58_address, "user": user, "signature": signature.model_dump()}
    )

    timeout_sent_jobs_runtime = timezone.make_aware(datetime(2025, 11, 24, 10, 1, 0))

    # Job create before cutoff time in SENT status (will timeout)
    with freeze_time(
        timeout_sent_jobs_runtime - timedelta(seconds=jobs_services.TIMEOUT_SENT_JOB_CUTOFF_SECONDS + 1)
    ) as frozen_time:
        job_old_sent = Job.objects.create(validator=validator, **validated_data)
        JobStatus.objects.create(job=job_old_sent, status=protocol_consts.JobStatus.PENDING.value)

        frozen_time.move_to(timedelta(milliseconds=1))
        JobStatus.objects.create(job=job_old_sent, status=protocol_consts.JobStatus.SENT.value)

    # Job created before cutoff time, but SENT after (will not timeout)
    with freeze_time(
        timeout_sent_jobs_runtime - timedelta(seconds=jobs_services.TIMEOUT_SENT_JOB_CUTOFF_SECONDS + 1)
    ) as frozen_time:
        job_old_create_recent_sent = Job.objects.create(validator=validator, **validated_data)
        JobStatus.objects.create(job=job_old_create_recent_sent, status=protocol_consts.JobStatus.PENDING.value)

        frozen_time.move_to("2025-11-24 10:00:31")
        JobStatus.objects.create(job=job_old_create_recent_sent, status=protocol_consts.JobStatus.SENT.value)

    # Job created after cutoff time in SENT status (will not timeout)
    with freeze_time(
        timeout_sent_jobs_runtime - timedelta(seconds=jobs_services.TIMEOUT_SENT_JOB_CUTOFF_SECONDS - 1)
    ) as frozen_time:
        job_recent_sent = Job.objects.create(validator=validator, **validated_data)
        JobStatus.objects.create(job=job_recent_sent, status=protocol_consts.JobStatus.PENDING.value)

        frozen_time.move_to(timedelta(milliseconds=1))
        JobStatus.objects.create(job=job_recent_sent, status=protocol_consts.JobStatus.SENT.value)

    # Jobs in other statuses created before cutoff (should NOT timeout)
    other_status_jobs = {}
    with freeze_time(timeout_sent_jobs_runtime - timedelta(seconds=jobs_services.TIMEOUT_SENT_JOB_CUTOFF_SECONDS + 1)):
        for status in protocol_consts.JobStatus:
            if status in (protocol_consts.JobStatus.SENT, protocol_consts.JobStatus.HORDE_FAILED):
                continue

            job_in_other_status = Job.objects.create(validator=validator, **validated_data)
            JobStatus.objects.create(job=job_in_other_status, status=status.value)
            other_status_jobs[status] = job_in_other_status

    with freeze_time(timeout_sent_jobs_runtime):
        jobs_services.timeout_sent_jobs()

        assert job_old_sent.status.status == protocol_consts.JobStatus.HORDE_FAILED.value
        assert job_old_create_recent_sent.status.status == protocol_consts.JobStatus.SENT.value
        assert job_recent_sent.status.status == protocol_consts.JobStatus.SENT.value

        for status, job_in_other_status in other_status_jobs.items():
            assert job_in_other_status.status.status == status.value, (
                f"Job with status {status} should not have changed"
            )
