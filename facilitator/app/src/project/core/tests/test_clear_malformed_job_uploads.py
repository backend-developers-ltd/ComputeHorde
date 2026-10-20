import importlib
import json
from datetime import timedelta

import pytest
from compute_horde_core.output_upload import SingleFilePutUpload
from django.apps import apps
from django.core.exceptions import ValidationError
from django.db import connection
from django.utils.timezone import now
from rest_framework.test import APIClient

from project.core.eviction import JOBS_RETENTION_PERIOD, evict_jobs
from project.core.models import Job

migration = importlib.import_module("project.core.migrations.0055_clear_malformed_job_uploads")

VALID_UPLOAD = SingleFilePutUpload(url="https://example.com/upload", relative_path="result.json")
VALID_UPLOADS = [VALID_UPLOAD.model_dump(mode="json")]
MALFORMED_UPLOADS = [{**VALID_UPLOADS[0], "relative_path": "/proc/self/environ"}]


def store_raw_uploads(job: Job, uploads: list[dict]) -> None:
    with connection.cursor() as cursor:
        cursor.execute(f"UPDATE {Job._meta.db_table} SET uploads = %s WHERE uuid = %s", [json.dumps(uploads), job.uuid])


@pytest.fixture
def make_job(user, validator, signature, dummy_job_params):
    def make(created_at, uploads):
        job = Job.objects.create(
            user=user,
            validator=validator,
            target_validator_hotkey=validator.ss58_address,
            signature=signature.model_dump(),
            created_at=created_at,
            **dummy_job_params,
        )
        store_raw_uploads(job, uploads)
        return job

    return make


@pytest.fixture
def recent_malformed_job(make_job):
    return make_job(now(), MALFORMED_UPLOADS)


@pytest.fixture
def expired_malformed_job(make_job):
    return make_job(now() - JOBS_RETENTION_PERIOD - timedelta(days=1), MALFORMED_UPLOADS)


@pytest.fixture
def recent_valid_job(make_job):
    return make_job(now(), VALID_UPLOADS)


@pytest.fixture
def owner_api_client(user):
    client = APIClient()
    client.force_authenticate(user=user)
    return client


@pytest.mark.django_db(transaction=True)
def test_malformed_uploads_are_cleared(recent_malformed_job, recent_valid_job):
    with pytest.raises(ValidationError):
        Job.objects.get(pk=recent_malformed_job.pk)

    migration.clear_malformed_uploads(apps, None)

    assert {job.uuid: job.uploads for job in Job.objects.all()} == {
        recent_malformed_job.uuid: [],
        recent_valid_job.uuid: [VALID_UPLOAD],
    }


@pytest.mark.django_db(transaction=True)
def test_jobs_with_cleared_uploads_can_be_evicted_and_listed(
    recent_malformed_job, expired_malformed_job, owner_api_client
):
    migration.clear_malformed_uploads(apps, None)

    evict_jobs()
    response = owner_api_client.get("/api/v1/jobs/")

    assert list(Job.objects.values_list("uuid", flat=True)) == [recent_malformed_job.uuid]
    assert response.status_code == 200
    assert [job["uuid"] for job in response.data["results"]] == [str(recent_malformed_job.uuid)]
