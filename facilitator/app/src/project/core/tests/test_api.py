import json
import time
from unittest.mock import patch

import jwt
import pytest
from bittensor_wallet import Wallet
from django.conf import settings
from django.contrib.auth.models import User
from rest_framework.exceptions import ErrorDetail
from rest_framework.test import APIClient

from project.core.models import HotkeyWhitelist, Job, JobFeedback, Validator


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
    assert job_result["use_gpu"] is True
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
    }
    response = api_client.post("/api/v1/job-docker/", data)
    assert response.status_code == 201
    assert Job.objects.count() == 1
    job = Job.objects.first()
    assert job.docker_image == "hello-world"
    assert job.args == ["my", "args"]
    assert job.env == {"MY_ENV": "my value"}
    assert job.use_gpu is True
    assert job.user == user


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
    data = {"docker_image": "hello-world", "target_validator_hotkey": connected_validator.ss58_address}
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
