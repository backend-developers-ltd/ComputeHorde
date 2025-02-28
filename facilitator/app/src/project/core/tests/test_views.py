import pytest
from django.utils.timezone import now
from freezegun import freeze_time

from ..models import JobStatus


@pytest.mark.django_db(transaction=True)
def test__job_detail__download_url_regeneration(settings, job, client, monkeypatch):
    JobStatus.objects.create(job=job, status=JobStatus.Status.COMPLETED)

    assert job.is_download_url_expired() is False
    response = client.get(job.get_absolute_url())
    assert response.status_code == 200
    assert job.output_download_url in response.content.decode()

    monkeypatch.setattr("project.core.models.create_signed_download_url", lambda _: "http://new-download-url")
    with freeze_time(now() + settings.DOWNLOAD_PRESIGNED_URL_LIFETIME):
        assert job.is_download_url_expired() is True

        response = client.get(job.get_absolute_url())
        assert response.status_code == 200
        assert "http://new-download-url" in response.content.decode()
