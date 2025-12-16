import json
import time
from datetime import UTC, datetime
from http import HTTPStatus
from unittest.mock import patch

import jwt
import pytest
from bittensor_wallet import Wallet
from compute_horde_core.executor_class import ExecutorClass
from django.conf import settings
from django.contrib.auth.models import User
from rest_framework.exceptions import ErrorDetail
from rest_framework.test import APIClient

from project.core.models import CheatedJobReport, HotkeyWhitelist, Job, JobFeedback, Validator


@pytest.fixture
def api_client():
    return APIClient()


@pytest.fixture
def user(db):
    return User.objects.create_user(username="testuser", password="testpass")


@pytest.fixture
def another_user(db):
    return User.objects.create_user(username="anotheruser", password="anotherpass")


@pytest.fixture
def authenticated_api_client(api_client, user):
    api_client.force_authenticate(user=user)
    return api_client


@pytest.fixture
def mock_signature_from_request(signature):
    with patch("project.core.middleware.signature_middleware.signature_from_request") as mock:
        mock.return_value = signature
        yield mock


@pytest.fixture
def job_docker(db, user, connected_validator, signature):
    return Job.objects.create(
        user=user,
        validator=connected_validator,
        target_validator_hotkey=connected_validator.ss58_address,
        docker_image="hello-world",
        args=["my", "args"],
        env={"MY_ENV": "my value"},
        use_gpu=True,
        signature=signature.model_dump(),
        download_time_limit=3,
        execution_time_limit=3,
        streaming_start_time_limit=1,
        upload_time_limit=3,
    )


@pytest.fixture
def another_user_job_docker(db, another_user, connected_validator, signature):
    return Job.objects.create(
        user=another_user,
        validator=connected_validator,
        target_validator_hotkey=connected_validator.ss58_address,
        docker_image="hello-world",
        args=["my", "args"],
        env={"MY_ENV": "my value"},
        use_gpu=True,
        signature=signature.model_dump(),
        download_time_limit=3,
        execution_time_limit=3,
        streaming_start_time_limit=3,
        upload_time_limit=3,
    )


@pytest.fixture
def trusted_job_docker(db, user, connected_validator, signature):
    return Job.objects.create(
        user=user,
        validator=connected_validator,
        target_validator_hotkey=connected_validator.ss58_address,
        docker_image="hello-world",
        args=["my", "args"],
        env={"MY_ENV": "my value"},
        use_gpu=True,
        on_trusted_miner=True,
        signature=signature.model_dump(),
        download_time_limit=3,
        execution_time_limit=3,
        streaming_start_time_limit=3,
        upload_time_limit=3,
    )


@pytest.fixture
def whitelisted_hotkey(db, wallet):
    return HotkeyWhitelist.objects.create(ss58_address=wallet.hotkey.ss58_address)


def check_docker_job(job_result):
    generated_fields = {
        "created_at",
        "last_update",
        "status",
    }
    assert job_result["docker_image"] == "hello-world"
    assert job_result["args"] == ["my", "args"]
    assert job_result["env"] == {"MY_ENV": "my value"}
    assert set(job_result.keys()) & generated_fields == generated_fields


@pytest.mark.django_db
def test_job_viewset_list(api_client, user, job_docker, another_user_job_docker):
    api_client.force_authenticate(user=user)
    response = api_client.get("/api/v1/jobs/")
    assert response.status_code == 200
    assert len(response.data["results"]) == 1

    for job_result in response.data["results"]:
        check_docker_job(job_result)


@pytest.mark.django_db
def test_job_viewset_list_object_permissions(api_client, user, job_docker, another_user_job_docker):
    api_client.force_authenticate(user=user)
    response = api_client.get("/api/v1/jobs/")
    assert response.status_code == 200
    assert len(response.data["results"]) == 1

    uuids = {job["uuid"] for job in response.data["results"]}
    assert uuids == {str(job_docker.uuid)}


@pytest.mark.django_db
def test_job_viewset_retrieve_docker(api_client, user, job_docker):
    api_client.force_authenticate(user=user)
    response = api_client.get(f"/api/v1/jobs/{job_docker.uuid}/")
    assert response.status_code == 200
    check_docker_job(response.data)


@pytest.mark.django_db
def test_docker_job_viewset_create(api_client, user, connected_validator, mock_signature_from_request):
    api_client.force_authenticate(user=user)
    data = {
        "docker_image": "hello-world",
        "args": ["my", "args"],
        "env": {"MY_ENV": "my value"},
        "use_gpu": True,
        "target_validator_hotkey": connected_validator.ss58_address,
        "download_time_limit": 1,
        "execution_time_limit": 1,
        "streaming_start_time_limit": 1,
        "upload_time_limit": 1,
    }
    response = api_client.post("/api/v1/job-docker/", data)
    assert response.status_code == 201
    assert Job.objects.count() == 1
    job = Job.objects.first()
    assert job.docker_image == "hello-world"
    assert job.args == ["my", "args"]
    assert job.env == {"MY_ENV": "my value"}
    assert job.user == user


@pytest.mark.django_db
def test_docker_job_viewset_create_streaming(api_client, user, connected_validator, mock_signature_from_request):
    api_client.force_authenticate(user=user)
    data = {
        "docker_image": "hello-world",
        "args": ["my", "args"],
        "env": {"MY_ENV": "my value"},
        "use_gpu": True,
        "target_validator_hotkey": connected_validator.ss58_address,
        "download_time_limit": 1,
        "execution_time_limit": 1,
        "streaming_start_time_limit": 1,
        "upload_time_limit": 1,
        "streaming_details": {"public_key": "dummy-client-cert"},
    }
    response = api_client.post("/api/v1/job-docker/", data, format="json")
    assert response.status_code == 201
    job = Job.objects.first()
    assert job.docker_image == "hello-world"
    assert job.streaming_client_cert == "dummy-client-cert"

    job.streaming_server_cert = "dummy-server-cert"
    job.streaming_server_address = "127.0.0.1"
    job.streaming_server_port = 12345
    job.save()

    response = api_client.get(f"/api/v1/jobs/{job.uuid}/")
    assert response.status_code == 200
    assert response.data["streaming_server_cert"] == "dummy-server-cert"
    assert response.data["streaming_server_address"] == "127.0.0.1"
    assert response.data["streaming_server_port"] == 12345


def generate_signed_headers(
    wallet: Wallet,
    url: str,
    subnet_id: int,
    method: str = "POST",
    subnet_chain: str = "mainnet",
) -> dict:
    headers = {
        "Realm": subnet_chain,
        "SubnetID": str(subnet_id),
    }

    headers_str = json.dumps(headers, sort_keys=True)
    data_to_sign = f"{method}{url}{headers_str}".encode()
    signature = wallet.hotkey.sign(
        data_to_sign,
    ).hex()
    headers["Signature"] = signature

    return headers


def get_auth_token(api_client: APIClient, wallet: Wallet) -> str:
    """Perform the nonce->login flow to return a valid JWT token."""
    nonce_response = api_client.get("/auth/nonce")
    assert nonce_response.status_code == 200, "Failed to get nonce"
    nonce = nonce_response.json().get("nonce")
    signature = wallet.hotkey.sign(nonce.encode("utf-8")).hex()
    payload = {"hotkey": wallet.hotkey.ss58_address, "signature": signature, "nonce": nonce}
    login_response = api_client.post("/auth/login", payload)
    assert login_response.status_code == 200, "Login failed"
    token = login_response.json().get("token")
    return token


def build_http_headers(headers: dict[str, str]) -> dict[str, str]:
    """Convert headers dict to HTTP_ prefixed keyword arguments for the test client."""
    return {f"HTTP_{key.upper()}": value for key, value in headers.items()}


@pytest.mark.django_db
def test_hotkey_authentication__job_create(
    api_client, wallet, whitelisted_hotkey, connected_validator, mock_signature_from_request
):
    data = {
        "docker_image": "hello-world",
        "target_validator_hotkey": connected_validator.ss58_address,
        "download_time_limit": 1,
        "execution_time_limit": 1,
        "streaming_start_time_limit": 1,
        "upload_time_limit": 1,
    }
    # First call without any authentication must return 401.
    response = api_client.post("/api/v1/job-docker/", data)
    assert response.status_code == 401, response.content

    # Create an active validator for this wallet.
    Validator.objects.create(ss58_address=wallet.hotkey.ss58_address, is_active=True)

    # Create a token for a non-whitelisted hotkey and test unauthorized access.
    token_payload = {
        "sub": "non-whiletelisted-hotkey",
        "iat": int(time.time()),
        "exp": int(time.time()) + 3600,
    }
    token = jwt.encode(token_payload, settings.SECRET_KEY, algorithm="HS256")

    signed_headers = generate_signed_headers(
        wallet=wallet,
        url="http://testserver/api/v1/job-docker/",
        method="POST",
        subnet_id=12,
    )
    signed_headers["Authorization"] = f"Bearer {token}"

    response = api_client.post(
        "/api/v1/job-docker/",
        data,
        **build_http_headers(signed_headers),
    )
    # Still unauthorized since the hotkey in the token isn’t allowed.
    assert response.status_code == 401

    # Now get a valid token using the authentication flow.
    token = get_auth_token(api_client, wallet)
    signed_headers["Authorization"] = f"Bearer {token}"
    response = api_client.post(
        "/api/v1/job-docker/",
        data,
        **build_http_headers(signed_headers),
    )
    assert response.status_code == 201, response.content

    # Verify job creation.
    assert Job.objects.count() == 1
    job = Job.objects.first()
    assert job.docker_image == "hello-world"
    assert job.use_gpu is False
    assert job.user is None
    assert job.hotkey == wallet.hotkey.ss58_address


@pytest.mark.django_db
def test_hotkey_authentication__job_details(api_client, wallet, job_with_hotkey, whitelisted_hotkey):
    # No authentication returns 401.
    response = api_client.get(f"/api/v1/jobs/{job_with_hotkey.uuid}/")
    assert response.status_code == 401, response.content

    signed_headers = generate_signed_headers(
        wallet=wallet,
        url=f"http://testserver/api/v1/jobs/{job_with_hotkey.uuid}/",
        method="GET",
        subnet_id=12,
    )

    # Test without Authorization header.
    response = api_client.get(
        f"/api/v1/jobs/{job_with_hotkey.uuid}/",
        **build_http_headers(signed_headers),
    )
    assert response.status_code == 401, response.content

    # Create an active validator for this wallet.
    Validator.objects.create(ss58_address=wallet.hotkey.ss58_address, is_active=True)

    # Test with a non-allowed token.
    token_payload = {
        "sub": "non-whiletelisted-hotkey",
        "iat": int(time.time()),
        "exp": int(time.time()) + 3600,
    }
    token = jwt.encode(token_payload, settings.SECRET_KEY, algorithm="HS256")
    signed_headers["Authorization"] = f"Bearer {token}"
    response = api_client.get(
        f"/api/v1/jobs/{job_with_hotkey.uuid}/",
        **build_http_headers(signed_headers),
    )
    assert response.status_code == 401, response.content

    # Now obtain a valid token.
    token = get_auth_token(api_client, wallet)
    signed_headers["Authorization"] = f"Bearer {token}"
    # With proper authentication, access succeeds.
    response = api_client.get(
        f"/api/v1/jobs/{job_with_hotkey.uuid}/",
        **build_http_headers(signed_headers),
    )
    assert response.status_code == 200, response.content


@pytest.mark.django_db
def test_hotkey_authentication__job_list(api_client, wallet, job_with_hotkey):
    # even if everything is valid, we don't allow listing jobs by hotkey
    signed_headers = generate_signed_headers(
        wallet=wallet,
        url="http://testserver/api/v1/jobs/",
        method="GET",
        subnet_id=12,
    )
    Validator.objects.create(ss58_address=wallet.hotkey.ss58_address, is_active=True)

    response = api_client.get(
        "/api/v1/jobs/",
        **build_http_headers(signed_headers),
    )
    assert response.status_code == 401, response.content


def test_job_feedback__create__requires_signature(authenticated_api_client):
    response = authenticated_api_client.put("/api/v1/jobs/123/feedback/", {"result_correctness": 1})
    assert (response.status_code, response.data) == (
        400,
        {
            "details": "Request signature not found, but is required",
            "error": "Signature not found",
        },
    )


def test_job_feedback__create_n_retrieve(authenticated_api_client, mock_signature_from_request, job_docker):
    data = {"result_correctness": 0.5, "expected_duration": 10.0}

    response = authenticated_api_client.put(f"/api/v1/jobs/{job_docker.uuid}/feedback/", data)
    assert (response.status_code, response.data) == (201, data)

    assert job_docker.feedback.result_correctness == data["result_correctness"]
    assert job_docker.feedback.expected_duration == data["expected_duration"]

    response = authenticated_api_client.get(f"/api/v1/jobs/{job_docker.uuid}/feedback/")
    assert (response.status_code, response.data) == (200, data)


def test_job_feedback__already_exists(authenticated_api_client, mock_signature_from_request, job_docker, user):
    job_feedback = JobFeedback.objects.create(job=job_docker, result_correctness=1, user=user)
    data = {"result_correctness": 0.5, "expected_duration": 10.0}

    response = authenticated_api_client.put(f"/api/v1/jobs/{job_docker.uuid}/feedback/", data)
    assert (response.status_code, response.data) == (
        409,
        {"detail": ErrorDetail(string="Feedback already exists", code="conflict")},
    )
    assert JobFeedback.objects.get() == job_feedback


@pytest.mark.django_db
def test_cheated_job_viewset(authenticated_api_client, job_docker, trusted_job_docker, mock_signature_from_request):
    cheat_payload = {
        "job_uuid": str(job_docker.uuid),
        "trusted_job_uuid": str(trusted_job_docker.uuid),
        "details": {
            "reason": "hash_mismatch",
        },
    }

    frozen_timestamp = datetime(2024, 1, 1, tzinfo=UTC)

    with patch("django.utils.timezone.now", return_value=frozen_timestamp):
        with patch("project.core.models.Job.report_cheated") as report_mock:
            response = authenticated_api_client.post("/api/v1/cheated-job/", cheat_payload, format="json")

    assert response.data == {"message": "Job reported as cheated"}
    assert response.status_code == 201

    report_mock.assert_called_once()

    report = CheatedJobReport.objects.get(job=job_docker)
    assert CheatedJobReport.objects.count() == 1
    assert report.created_at == frozen_timestamp
    assert report.details == cheat_payload["details"]
    assert report.trusted_job == trusted_job_docker

    job_docker.refresh_from_db()
    assert job_docker.cheated is True

    later_timestamp = datetime(2024, 1, 1, 0, 0, 10, tzinfo=UTC)
    with patch("django.utils.timezone.now", return_value=later_timestamp):
        response = authenticated_api_client.post("/api/v1/cheated-job/", cheat_payload, format="json")
    assert response.status_code == 400
    assert response.data == {"error": "Cheat report already exists for this job"}
    assert CheatedJobReport.objects.count() == 1
    report.refresh_from_db()
    assert report.created_at == frozen_timestamp

    response = authenticated_api_client.post(
        "/api/v1/cheated-job/",
        {
            "job_uuid": "00000000-0000-0000-0000-000000000000",
            "trusted_job_uuid": str(trusted_job_docker.uuid),
        },
        format="json",
    )
    assert response.status_code == 404
    assert response.data == {"error": "Job not found"}


@pytest.mark.django_db
def test_cheated_job_viewset_nonexistent_trusted_job(authenticated_api_client, job_docker):
    response = authenticated_api_client.post(
        "/api/v1/cheated-job/",
        {
            "job_uuid": str(job_docker.uuid),
            "trusted_job_uuid": "00000000-0000-0000-0000-000000000000",
        },
        format="json",
    )
    assert response.status_code == 404
    assert response.data == {"error": "Trusted job not found"}


@pytest.mark.django_db
def test_cheated_job_viewset_optional_fields_empty_payload(
    authenticated_api_client, job_docker, trusted_job_docker, mock_signature_from_request
):
    frozen_timestamp = datetime(2024, 2, 1, tzinfo=UTC)
    with patch("django.utils.timezone.now", return_value=frozen_timestamp):
        with patch("project.core.models.Job.report_cheated") as report_mock:
            response = authenticated_api_client.post(
                "/api/v1/cheated-job/",
                {
                    "job_uuid": str(job_docker.uuid),
                    "trusted_job_uuid": str(trusted_job_docker.uuid),
                },
                format="json",
            )

    assert response.data == {"message": "Job reported as cheated"}
    assert response.status_code == 201
    report_mock.assert_called_once()

    report = CheatedJobReport.objects.get(job=job_docker)
    assert CheatedJobReport.objects.count() == 1
    assert report.created_at == frozen_timestamp
    assert report.details is None
    assert report.trusted_job == trusted_job_docker


@pytest.mark.django_db
def test_cheated_job_viewset_trusted_job_not_on_trusted_miner(
    authenticated_api_client, job_docker, user, connected_validator, signature
):
    # Create a job that is NOT on a trusted miner
    non_trusted_job = Job.objects.create(
        user=user,
        validator=connected_validator,
        target_validator_hotkey=connected_validator.ss58_address,
        docker_image="hello-world",
        args=[],
        env={},
        use_gpu=False,
        on_trusted_miner=False,
        signature=signature.model_dump(),
        download_time_limit=3,
        execution_time_limit=3,
        streaming_start_time_limit=3,
        upload_time_limit=3,
    )

    response = authenticated_api_client.post(
        "/api/v1/cheated-job/",
        {
            "job_uuid": str(job_docker.uuid),
            "trusted_job_uuid": str(non_trusted_job.uuid),
        },
        format="json",
    )

    assert response.status_code == 404
    assert response.data == {"error": "Trusted job not found"}


@pytest.mark.django_db
def test_create_job_with_namespace(api_client, user, connected_validator, mock_signature_from_request):
    """
    Test facilitator API creates a job with namespace.
    """
    job_data = {
        "job_namespace": "SN123.1.0",
        "docker_image": "test/image",
        "executor_class": ExecutorClass.always_on__llm__a6000,
        "target_validator_hotkey": connected_validator.ss58_address,
        "download_time_limit": 300,
        "execution_time_limit": 600,
        "streaming_start_time_limit": 600,
        "upload_time_limit": 300,
    }
    api_client.force_authenticate(user=user)
    response = api_client.post("/api/v1/job-docker/", job_data, format="json")
    data = response.json()
    assert response.status_code == HTTPStatus.CREATED
    job = Job.objects.get(uuid=data["uuid"])
    assert job.job_namespace == job_data["job_namespace"]
