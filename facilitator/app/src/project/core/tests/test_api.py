import base64
import json
import time
from unittest.mock import patch

import pytest
from bittensor_wallet import Wallet
from compute_horde.fv_protocol.facilitator_requests import Signature
from django.contrib.auth.models import User
from rest_framework.exceptions import ErrorDetail
from rest_framework.test import APIClient

from project.core.models import Job, JobFeedback, Validator


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
def signature():
    return Signature(
        signature_type="dummy_signature_type",
        signatory="dummy_signatory",
        timestamp_ns=time.time_ns(),
        signature=base64.b64encode(b"dummy_signature"),
    )


@pytest.fixture
def mock_signature_from_request(signature):
    with patch("project.core.middleware.signature_middleware.signature_from_request") as mock:
        mock.return_value = signature
        yield mock


@pytest.fixture
def job_docker(db, user, connected_validator, miner):
    return Job.objects.create(
        user=user,
        validator=connected_validator,
        miner=miner,
        docker_image="hello-world",
        args=["my", "args"],
        env={"MY_ENV": "my value"},
        use_gpu=True,
    )


@pytest.fixture
def another_user_job_docker(db, another_user, connected_validator, miner):
    return Job.objects.create(
        user=another_user,
        validator=connected_validator,
        miner=miner,
        docker_image="hello-world",
        args=["my", "args"],
        env={"MY_ENV": "my value"},
        use_gpu=True,
    )


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
def test_docker_job_viewset_create(api_client, user, connected_validator, miner):
    api_client.force_authenticate(user=user)
    data = {"docker_image": "hello-world", "args": ["my", "args"], "env": {"MY_ENV": "my value"}, "use_gpu": True}
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
        "Nonce": str(time.time()),
        "Hotkey": wallet.hotkey.ss58_address,
    }

    headers_str = json.dumps(headers, sort_keys=True)
    data_to_sign = f"{method}{url}{headers_str}".encode()
    signature = wallet.hotkey.sign(
        data_to_sign,
    ).hex()
    headers["Signature"] = signature

    return headers


# TODO: convert this test into job-docker if possible, otherwise remove entirely
# @pytest.mark.django_db
# def test_hotkey_authentication__job_create(api_client, wallet, connected_validator, miner):
#     data = {}
#     response = api_client.post("/api/v1/job-raw/", data)
#     assert response.status_code == 403
#
#     signed_headers = generate_signed_headers(
#         wallet=wallet,
#         url="http://testserver/api/v1/job-raw/",
#         method="POST",
#         subnet_id=12,
#     )
#
#     response = api_client.post(
#         "/api/v1/job-raw/",
#         data,
#         **{f"HTTP_{header.upper()}": value for header, value in signed_headers.items()},
#     )
#     assert response.status_code == 403
#
#     Validator.objects.create(ss58_address=wallet.hotkey.ss58_address, is_active=True)
#     response = api_client.post(
#         "/api/v1/job-raw/",
#         data,
#         **{f"HTTP_{header.upper()}": value for header, value in signed_headers.items()},
#     )
#     assert response.status_code == 201, response.content
#
#     assert Job.objects.count() == 1
#     job = Job.objects.first()
#     assert job.use_gpu is False
#     assert job.user is None
#     assert job.hotkey == wallet.hotkey.ss58_address


@pytest.mark.django_db
def test_hotkey_authentication__job_details(api_client, wallet, job_with_hotkey):
    # no authentication -> 403
    response = api_client.get(f"/api/v1/jobs/{job_with_hotkey.uuid}/")
    assert response.status_code == 403, response.content

    signed_headers = generate_signed_headers(
        wallet=wallet,
        url=f"http://testserver/api/v1/jobs/{job_with_hotkey.uuid}/",
        method="GET",
        subnet_id=12,
    )

    # unknown hotkey -> 403
    response = api_client.get(
        f"/api/v1/jobs/{job_with_hotkey.uuid}/",
        **{f"HTTP_{header.upper()}": value for header, value in signed_headers.items()},
    )
    assert response.status_code == 403, response.content

    # known hotkey -> success
    Validator.objects.create(ss58_address=wallet.hotkey.ss58_address, is_active=True)
    response = api_client.get(
        f"/api/v1/jobs/{job_with_hotkey.uuid}/",
        **{f"HTTP_{header.upper()}": value for header, value in signed_headers.items()},
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
        **{f"HTTP_{header.upper()}": value for header, value in signed_headers.items()},
    )
    assert response.status_code == 403, response.content


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
