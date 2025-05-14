import json
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import bittensor_wallet
import httpx
import pytest

from compute_horde_core.signature import (
    BittensorWalletVerifier,
    SignatureInvalidException,
    SignatureScope,
    SignedFields,
    signature_from_headers,
)
from compute_horde_sdk._internal.sdk import (
    DEFAULT_MAX_JOB_RUN_ATTEMPTS,
    HTTP_RETRY_MAX_ATTEMPTS,
    RETRYABLE_HTTP_EXCEPTIONS,
)
from tests.utils import INITIAL_FROZEN_TIME

if TYPE_CHECKING:
    from compute_horde_sdk._internal.sdk import ComputeHordeClient, ComputeHordeJob, ComputeHordeJobSpec

TEST_FACILITATOR_URL = "http://localhost:4321"
TEST_JOB_UUID = "1c4904f0-b614-4e27-8e50-7bfc5ddab8fd"
TEST_JOB_UUID2 = "a9c0ccd4-0900-46b1-b8ff-9a958157c2f5"
TEST_DOCKER_IMAGE = "example-com/test-image"


def get_job_response(uuid: str = TEST_JOB_UUID, status: str = "Accepted", **kwargs):
    return {
        "uuid": uuid,
        "executor_class": "spin_up_4min__gpu_24gb",
        "created_at": "2025-01-29T14:32:46Z",
        "last_update": "2025-01-29T14:32:46Z",
        "status": status,
        "docker_image": TEST_DOCKER_IMAGE,
        "args": ["--test", "yes"],
        "env": {},
        "use_gpu": True,
        "hf_repo_id": None,
        "hf_revision": None,
        "input_url": "",
        "output_download_url": "",
        "tag": "",
        "stdout": "",
        "volumes": [],
        "uploads": [],
        "target_validator_hotkey": "abcd1234",
        **kwargs,
    }


# Copied from facilitator.
def assert_signature(request: httpx.Request):
    """
    Extracts the signature from the request and verifies it.

    :param request: httpx.Request object
    :return: Signature from the request
    :raises SignatureNotFound: if the signature is not found in the request
    :raises SignatureInvalidException: if the signature is invalid
    """
    signature = signature_from_headers(request.headers)  # type: ignore
    verifier = BittensorWalletVerifier()
    try:
        json_body = json.loads(request.content)
    except ValueError:
        json_body = None

    if signature.signature_scope == SignatureScope.SignedFields:
        signed_fields = SignedFields.from_facilitator_sdk_json(json_body)
        verifier.verify(signed_fields.model_dump_json(), signature)
    elif signature.signature_scope == SignatureScope.FullRequest:
        signed_fields = json.dumps(json_body, sort_keys=True)
        verifier.verify(signed_fields, signature)
    else:
        raise SignatureInvalidException(f"Invalid signature scope: {signature}")


def setup_successful_authentication(httpx_mock):
    httpx_mock.add_response(
        url=f"{TEST_FACILITATOR_URL}/auth/nonce",
        json={"nonce": "test_nonce"},
    )
    httpx_mock.add_response(
        url=f"{TEST_FACILITATOR_URL}/auth/login",
        json={"token": "test_token"},
    )


@pytest.fixture
def keypair():
    return bittensor_wallet.Keypair.create_from_mnemonic(
        "slot excuse valid grief praise rifle spoil auction weasel glove pen share"
    )


@pytest.fixture
def compute_horde_client(keypair, apiver_module) -> "ComputeHordeClient":
    client = apiver_module.ComputeHordeClient(
        hotkey=keypair,
        compute_horde_validator_hotkey="abcdef",
        job_queue="sn123",
        facilitator_url=TEST_FACILITATOR_URL,
    )
    client._token = "test_jwt_token"
    return client


@pytest.fixture
def job_spec(apiver_module, compute_horde_client) -> "ComputeHordeJobSpec":
    return apiver_module.ComputeHordeJobSpec(
        executor_class=apiver_module.ExecutorClass.spin_up_4min__gpu_24gb,
        job_namespace="SN123.0",
        docker_image=TEST_DOCKER_IMAGE,
    )


@pytest.fixture
def job(apiver_module, compute_horde_client) -> "ComputeHordeJob":
    return apiver_module.ComputeHordeJob(
        compute_horde_client,
        TEST_JOB_UUID,
        apiver_module.ComputeHordeJobStatus.ACCEPTED,
    )


@pytest.mark.asyncio
async def test_job_e2e(apiver_module, httpx_mock, keypair, async_sleep_mock):
    httpx_mock.add_response(
        url=TEST_FACILITATOR_URL + "/api/v1/job-docker/",
        json=get_job_response(
            info=TEST_JOB_UUID,
            status="Sent",
        ),
    )

    client: ComputeHordeClient = apiver_module.ComputeHordeClient(
        hotkey=keypair,
        compute_horde_validator_hotkey="abcdef",
        job_queue="sn123",
        facilitator_url=TEST_FACILITATOR_URL,
    )

    client._token = "test_jwt_token"

    job = await client.create_job(
        apiver_module.ComputeHordeJobSpec(
            executor_class=apiver_module.ExecutorClass.spin_up_4min__gpu_24gb,
            job_namespace="SN123.0",
            docker_image=TEST_DOCKER_IMAGE,
        )
    )

    assert job.uuid == TEST_JOB_UUID
    assert job.status == "Sent"
    assert_signature(httpx_mock.get_request())

    httpx_mock.add_response(json=get_job_response(status="Accepted"))
    httpx_mock.add_response(json=get_job_response(status="Completed"))

    await job.wait()

    assert job.status is apiver_module.ComputeHordeJobStatus.COMPLETED


@pytest.mark.asyncio
async def test_get_jobs(apiver_module, compute_horde_client, httpx_mock):
    job1_uuid = "7b522daa-e807-4094-8d96-99b9a863f960"
    job1_status = "Accepted"
    job2_uuid = "6b6b4ef2-5174-4a45-ae2f-8bfae3915168"
    job2_status = "Failed"
    httpx_mock.add_response(
        json={
            "count": 2,
            "previous": None,
            "next": None,
            "results": [
                get_job_response(uuid=job1_uuid, status=job1_status),
                get_job_response(uuid=job2_uuid, status=job2_status),
            ],
        }
    )

    jobs = await compute_horde_client.get_jobs()

    assert len(jobs) == 2
    assert jobs[0].uuid == job1_uuid
    assert jobs[0].status is apiver_module.ComputeHordeJobStatus.ACCEPTED
    assert jobs[1].uuid == job2_uuid
    assert jobs[1].status is apiver_module.ComputeHordeJobStatus.FAILED


@pytest.mark.asyncio
async def test_get_jobs__http_error(apiver_module, compute_horde_client, httpx_mock):
    httpx_mock.add_response(status_code=400)

    with pytest.raises(apiver_module.ComputeHordeError):
        await compute_horde_client.get_jobs()


@pytest.mark.asyncio
async def test_get_jobs__not_found(apiver_module, compute_horde_client, httpx_mock):
    httpx_mock.add_response(status_code=404)

    with pytest.raises(apiver_module.ComputeHordeNotFoundError):
        await compute_horde_client.get_jobs()


@pytest.mark.asyncio
async def test_get_jobs__malformed_response(apiver_module, compute_horde_client, httpx_mock):
    httpx_mock.add_response(json=[{"derp": "dunno"}])

    with pytest.raises(apiver_module.ComputeHordeError):
        await compute_horde_client.get_jobs()


@pytest.mark.asyncio
async def test_iter_jobs(apiver_module, compute_horde_client, httpx_mock):
    job1_uuid = "7b522daa-e807-4094-8d96-99b9a863f960"
    job1_status = "Accepted"
    job2_uuid = "6b6b4ef2-5174-4a45-ae2f-8bfae3915168"
    job2_status = "Failed"
    httpx_mock.add_response(
        json={
            "count": 11,
            "previous": None,
            "next": f"{TEST_FACILITATOR_URL}/api/v1/jobs/?page=2",
            "results": [get_job_response(uuid=job1_uuid, status=job1_status) for _ in range(10)],
        }
    )
    httpx_mock.add_response(
        json={
            "count": 11,
            "previous": f"{TEST_FACILITATOR_URL}/api/v1/jobs/?page=1",
            "next": None,
            "results": [
                get_job_response(uuid=job2_uuid, status=job2_status),
            ],
        }
    )

    jobs = []
    async for job in compute_horde_client.iter_jobs():
        jobs.append(job)

    assert len(jobs) == 11
    assert jobs[0].uuid == job1_uuid
    assert jobs[0].status is apiver_module.ComputeHordeJobStatus.ACCEPTED
    assert jobs[-1].uuid == job2_uuid
    assert jobs[-1].status is apiver_module.ComputeHordeJobStatus.FAILED


@pytest.mark.asyncio
async def test_iter_jobs__http_error(apiver_module, compute_horde_client, httpx_mock):
    httpx_mock.add_response(status_code=400)

    with pytest.raises(apiver_module.ComputeHordeError):
        async for _ in compute_horde_client.iter_jobs():
            pass


@pytest.mark.asyncio
async def test_iter_jobs__malformed_response(apiver_module, compute_horde_client, httpx_mock):
    httpx_mock.add_response(json=[{"derp": "dunno"}])

    with pytest.raises(apiver_module.ComputeHordeError):
        async for _ in compute_horde_client.iter_jobs():
            pass


@pytest.mark.asyncio
async def test_get_job(compute_horde_client, httpx_mock):
    job_uuid = TEST_JOB_UUID
    job_status = "Accepted"
    httpx_mock.add_response(json=get_job_response(uuid=job_uuid, status=job_status))

    job = await compute_horde_client.get_job(job_uuid)

    assert job.uuid == job_uuid
    assert job.status == job_status


@pytest.mark.asyncio
async def test_get_job__http_error(apiver_module, compute_horde_client, httpx_mock):
    httpx_mock.add_response(status_code=400)

    with pytest.raises(apiver_module.ComputeHordeError):
        await compute_horde_client.get_job(TEST_JOB_UUID)


@pytest.mark.asyncio
async def test_get_job__not_found(apiver_module, compute_horde_client, httpx_mock):
    httpx_mock.add_response(status_code=404)

    with pytest.raises(apiver_module.ComputeHordeNotFoundError):
        await compute_horde_client.get_job(TEST_JOB_UUID)


@pytest.mark.asyncio
async def test_get_job__malformed_response(apiver_module, compute_horde_client, httpx_mock):
    httpx_mock.add_response(json={"derp": "dunno"})

    with pytest.raises(apiver_module.ComputeHordeError):
        await compute_horde_client.get_job(TEST_JOB_UUID)


@pytest.mark.asyncio
async def test_create_job(apiver_module, compute_horde_client, httpx_mock):
    httpx_mock.add_response(
        json=get_job_response(
            uuid=TEST_JOB_UUID,
            status="Accepted",
            docker_image="my-image",
            args=["--arg1", "value1"],
            env={"ENV_VAR": "value"},
            use_gpu=True,
            input_url="https://example.com/input",
        )
    )

    job = await compute_horde_client.create_job(
        apiver_module.ComputeHordeJobSpec(
            executor_class=apiver_module.ExecutorClass.spin_up_4min__gpu_24gb,
            job_namespace="SN123.0",
            docker_image=TEST_DOCKER_IMAGE,
            args=["--block", "10000"],
            env={"TEST_ENV": "1"},
            artifacts_dir="/artifacts",
            input_volumes={
                "/volume/models/model01": apiver_module.HuggingfaceInputVolume(repo_id="myrepo/mymodel"),
                "/volume/version.txt": apiver_module.InlineInputVolume(contents="dmVyc2lvbj0y"),
                "/volume/dataset.json": apiver_module.HTTPInputVolume(
                    url="https://s3.aws.something.com/mybucket/myfile.json"
                ),
            },
            output_volumes={
                "/output/results.json": apiver_module.HTTPOutputVolume(
                    http_method="PUT", url="https://s3.aws.something.com/mybucket/myfile.json"
                ),
                "/output/image.png": apiver_module.HTTPOutputVolume(
                    http_method="POST", url="https://s3.aws.something.com/mybucket/images"
                ),
            },
        )
    )

    assert job.uuid == TEST_JOB_UUID

    request = httpx_mock.get_request()
    assert_signature(request)
    req_json = json.loads(request.content)
    assert req_json["executor_class"] == "spin_up-4min.gpu-24gb"
    assert req_json["docker_image"] == TEST_DOCKER_IMAGE
    assert req_json["args"] == ["--block", "10000"]
    assert req_json["env"] == {"TEST_ENV": "1"}
    assert req_json["volumes"] == [
        {
            "volume_type": "huggingface_volume",
            "relative_path": "models/model01",
            "repo_id": "myrepo/mymodel",
            "repo_type": None,
            "revision": None,
            "allow_patterns": None,
            "token": None,
        },
        {
            "volume_type": "inline",
            "relative_path": "version.txt",
            "contents": "dmVyc2lvbj0y",
        },
        {
            "volume_type": "single_file",
            "relative_path": "dataset.json",
            "url": "https://s3.aws.something.com/mybucket/myfile.json",
        },
    ]
    assert req_json["uploads"] == [
        {
            "output_upload_type": "single_file_put",
            "relative_path": "results.json",
            "url": "https://s3.aws.something.com/mybucket/myfile.json",
            "signed_headers": None,
        },
        {
            "output_upload_type": "single_file_post",
            "relative_path": "image.png",
            "form_fields": None,
            "signed_headers": None,
            "url": "https://s3.aws.something.com/mybucket/images",
        },
    ]


@pytest.mark.asyncio
async def test_create_job__http_error(apiver_module, compute_horde_client, httpx_mock):
    httpx_mock.add_response(json=[{"derp": "dunno"}])

    with pytest.raises(apiver_module.ComputeHordeError):
        await compute_horde_client.create_job(
            apiver_module.ComputeHordeJobSpec(
                executor_class=apiver_module.ExecutorClass.spin_up_4min__gpu_24gb,
                job_namespace="SN123.0",
                docker_image=TEST_DOCKER_IMAGE,
            )
        )


@pytest.mark.asyncio
async def test_create_job__malformed_response(apiver_module, compute_horde_client, httpx_mock):
    httpx_mock.add_response(json=[{"derp": "dunno"}])

    with pytest.raises(apiver_module.ComputeHordeError):
        await compute_horde_client.create_job(
            apiver_module.ComputeHordeJobSpec(
                executor_class=apiver_module.ExecutorClass.spin_up_4min__gpu_24gb,
                job_namespace="SN123.0",
                docker_image=TEST_DOCKER_IMAGE,
            )
        )


@pytest.mark.parametrize("final_status", ["Completed", "Failed", "Rejected"])
@pytest.mark.asyncio
async def test_job_wait__various_end_states(
    apiver_module,
    final_status,
    job,
    httpx_mock,
    async_sleep_mock,
):
    httpx_mock.add_response(json=get_job_response(uuid=TEST_JOB_UUID, status=final_status))

    await job.wait()

    assert job.status is apiver_module.ComputeHordeJobStatus(final_status)


@pytest.mark.asyncio
async def test_wait_for_job__immediate_completion(apiver_module, compute_horde_client, async_sleep_mock):
    job = apiver_module.ComputeHordeJob(
        compute_horde_client, TEST_JOB_UUID, apiver_module.ComputeHordeJobStatus.COMPLETED
    )

    await job.wait()

    assert job.status is apiver_module.ComputeHordeJobStatus.COMPLETED


@pytest.mark.asyncio
async def test_wait_for_job__timeout(apiver_module, job, httpx_mock, async_sleep_mock):
    httpx_mock.add_response(json=get_job_response(uuid=TEST_JOB_UUID, status="Accepted"))

    with pytest.raises(apiver_module.ComputeHordeJobTimeoutError) as e:
        await job.wait(timeout=0)

    assert f"Job {TEST_JOB_UUID} did not complete within 0 seconds" in str(e.value)


@pytest.mark.asyncio
async def test_wait_for_job__http_error(apiver_module, job, httpx_mock, async_sleep_mock):
    httpx_mock.add_response(status_code=400)

    with pytest.raises(apiver_module.ComputeHordeError):
        await job.wait()


@pytest.mark.asyncio
async def test_wait_for_job__malformed_response(apiver_module, job, httpx_mock, async_sleep_mock):
    httpx_mock.add_response(json={"derp": "dunno"})

    with pytest.raises(apiver_module.ComputeHordeError):
        await job.wait()


@pytest.mark.parametrize("exc_class", RETRYABLE_HTTP_EXCEPTIONS)
@pytest.mark.asyncio
async def test_http_connection_error_is_retried__fail(
    apiver_module, compute_horde_client, httpx_mock, exc_class, async_sleep_mock
):
    for _ in range(HTTP_RETRY_MAX_ATTEMPTS):
        httpx_mock.add_exception(exc_class("some error message"))

    with pytest.raises(apiver_module.ComputeHordeError, match="ComputeHorde request failed: some error message"):
        await compute_horde_client.get_job(TEST_JOB_UUID)


@pytest.mark.parametrize("exc_class", RETRYABLE_HTTP_EXCEPTIONS)
@pytest.mark.asyncio
async def test_http_connection_error_is_retried__success(compute_horde_client, httpx_mock, exc_class, async_sleep_mock):
    for _ in range(HTTP_RETRY_MAX_ATTEMPTS - 1):
        httpx_mock.add_exception(exc_class("some error message"))
    httpx_mock.add_response(json=get_job_response(uuid=TEST_JOB_UUID, status="Accepted"))

    await compute_horde_client.get_job(TEST_JOB_UUID)


@pytest.mark.asyncio
async def test_http_too_many_requests_is_retried__fail(
    apiver_module, compute_horde_client, httpx_mock, async_sleep_mock
):
    for _ in range(HTTP_RETRY_MAX_ATTEMPTS):
        httpx_mock.add_response(status_code=429)

    with pytest.raises(apiver_module.ComputeHordeError, match="ComputeHorde responded with status code 429"):
        await compute_horde_client.get_job(TEST_JOB_UUID)


@pytest.mark.asyncio
async def test_http_too_many_requests_is_retried__success(compute_horde_client, httpx_mock, async_sleep_mock):
    for _ in range(HTTP_RETRY_MAX_ATTEMPTS - 1):
        httpx_mock.add_response(status_code=429)
    httpx_mock.add_response(json=get_job_response(uuid=TEST_JOB_UUID, status="Accepted"))
    await compute_horde_client.get_job(TEST_JOB_UUID)


@pytest.mark.asyncio
async def test_run_until_complete__happy_path(
    apiver_module, compute_horde_client, job_spec, httpx_mock, async_sleep_mock
):
    httpx_mock.add_response(
        url=f"{TEST_FACILITATOR_URL}/api/v1/job-docker/",
        json=get_job_response(info=TEST_JOB_UUID, status="Sent"),
    )
    httpx_mock.add_response(
        url=f"{TEST_FACILITATOR_URL}/api/v1/jobs/{TEST_JOB_UUID}/",
        json=get_job_response(status="Completed"),
    )

    job = await compute_horde_client.run_until_complete(job_spec)

    assert job.status == apiver_module.ComputeHordeJobStatus.COMPLETED


async def helper_run_until_complete__job_attempt_callback(
    apiver_module, compute_horde_client, job_spec, httpx_mock, job_attempt_callback, jobs_store: list["ComputeHordeJob"]
):
    assert len(jobs_store) == 0  # `job_attempt_callback` should populate this empty list

    # 1st attempt
    httpx_mock.add_response(
        url=f"{TEST_FACILITATOR_URL}/api/v1/job-docker/",
        json=get_job_response(uuid=TEST_JOB_UUID, status="Sent", info=TEST_JOB_UUID),
    )
    httpx_mock.add_response(
        url=f"{TEST_FACILITATOR_URL}/api/v1/jobs/{TEST_JOB_UUID}/",
        json=get_job_response(uuid=TEST_JOB_UUID, status="Failed"),
    )
    # 2nd attempt
    httpx_mock.add_response(
        url=f"{TEST_FACILITATOR_URL}/api/v1/job-docker/",
        json=get_job_response(uuid=TEST_JOB_UUID2, status="Sent", info=TEST_JOB_UUID2),
    )
    httpx_mock.add_response(
        url=f"{TEST_FACILITATOR_URL}/api/v1/jobs/{TEST_JOB_UUID2}/",
        json=get_job_response(uuid=TEST_JOB_UUID2, status="Completed"),
    )

    await compute_horde_client.run_until_complete(job_spec, job_attempt_callback=job_attempt_callback)

    assert len(jobs_store) == 2
    assert jobs_store[0].uuid == TEST_JOB_UUID
    assert jobs_store[1].uuid == TEST_JOB_UUID2
    assert jobs_store[0].status == apiver_module.ComputeHordeJobStatus.FAILED
    assert jobs_store[1].status == apiver_module.ComputeHordeJobStatus.COMPLETED


@pytest.mark.asyncio
async def test_run_until_complete__job_attempt_callback__sync(
    apiver_module, compute_horde_client, job_spec, httpx_mock, async_sleep_mock
):
    jobs_store = []
    job_attempt_callback = jobs_store.append
    await helper_run_until_complete__job_attempt_callback(
        apiver_module,
        compute_horde_client,
        job_spec,
        httpx_mock,
        job_attempt_callback,
        jobs_store,
    )


@pytest.mark.asyncio
async def test_run_until_complete__job_attempt_callback__async(
    apiver_module, compute_horde_client, job_spec, httpx_mock, async_sleep_mock
):
    jobs_store = []

    async def job_attempt_callback(job):
        jobs_store.append(job)

    await helper_run_until_complete__job_attempt_callback(
        apiver_module,
        compute_horde_client,
        job_spec,
        httpx_mock,
        job_attempt_callback,
        jobs_store,
    )


@pytest.mark.parametrize("max_attempts", [1, DEFAULT_MAX_JOB_RUN_ATTEMPTS])
@pytest.mark.asyncio
async def test_run_until_complete__attempts__finite(
    compute_horde_client, job_spec, httpx_mock, max_attempts, async_sleep_mock
):
    for _ in range(max_attempts):
        httpx_mock.add_response(
            url=f"{TEST_FACILITATOR_URL}/api/v1/job-docker/",
            json=get_job_response(info=TEST_JOB_UUID, status="Sent"),
        )
    httpx_mock.add_response(
        url=f"{TEST_FACILITATOR_URL}/api/v1/jobs/{TEST_JOB_UUID}/",
        json=get_job_response(status="Failed"),
        is_reusable=True,
    )
    jobs = []

    await compute_horde_client.run_until_complete(job_spec, job_attempt_callback=jobs.append, max_attempts=max_attempts)

    assert len(jobs) == max_attempts


@pytest.mark.asyncio
async def test_run_until_complete__attempts__infinite(compute_horde_client, job_spec, httpx_mock, async_sleep_mock):
    # this test also verifies the behaviour of None timeout
    # i.e. should not stop on its own, should not raise ComputeHordeJobTimeoutError
    httpx_mock.add_response(
        url=f"{TEST_FACILITATOR_URL}/api/v1/job-docker/",
        json=get_job_response(info=TEST_JOB_UUID, status="Sent"),
        is_reusable=True,
    )
    httpx_mock.add_response(
        url=f"{TEST_FACILITATOR_URL}/api/v1/jobs/{TEST_JOB_UUID}/",
        json=get_job_response(status="Failed"),
        is_reusable=True,
    )
    jobs = []

    class Stop(Exception): ...

    def stop_callback(job):
        jobs.append(jobs)
        # stop after running for a while
        if datetime.now(UTC) - INITIAL_FROZEN_TIME > timedelta(minutes=10):
            raise Stop

    with pytest.raises(Stop):
        await compute_horde_client.run_until_complete(job_spec, job_attempt_callback=stop_callback, max_attempts=-1)

    # Make sure we did actually attempt to run jobs multiple times before we stopped it
    # Because of the `async_sleep_mock` fixture the count is stable at 202 and not flaky.
    # Still using a lower number as the count can change based on changes of the fixture.
    assert len(jobs) > 100


@pytest.mark.parametrize("timeout", list(range(5)))
@pytest.mark.asyncio
async def test_run_until_complete__timeout(
    apiver_module, compute_horde_client, job_spec, httpx_mock, async_sleep_mock, timeout
):
    httpx_mock.add_response(
        url=f"{TEST_FACILITATOR_URL}/api/v1/job-docker/",
        json=get_job_response(info=TEST_JOB_UUID, status="Sent"),
    )
    # Each refresh_from_facilitator() call is preceded by a 3s sleep,
    # so the number of http calls mocked is adjusted here.
    # If the number of http calls does not match, httpx_mock should scream at us.
    # Don't use `is_reusable=True` here, we want httpx_mock to scream. :)
    for _ in range(1 + timeout // 3):
        httpx_mock.add_response(
            url=f"{TEST_FACILITATOR_URL}/api/v1/jobs/{TEST_JOB_UUID}/",
            json=get_job_response(status="Sent"),
        )

    with pytest.raises(apiver_module.ComputeHordeJobTimeoutError):
        await compute_horde_client.run_until_complete(job_spec, timeout=timeout)


@pytest.mark.asyncio
async def test_lazy_authentication_flow(apiver_module, compute_horde_client, httpx_mock):
    """
    Test that authentication is performed lazily on the first request.
    """

    # Ensure the client is not authenticated
    compute_horde_client._token = None

    setup_successful_authentication(httpx_mock)

    httpx_mock.add_response(
        url=f"{TEST_FACILITATOR_URL}/api/v1/jobs/{TEST_JOB_UUID}/",
        json=get_job_response(uuid=TEST_JOB_UUID, status="Accepted"),
    )

    job = await compute_horde_client.get_job(TEST_JOB_UUID)

    assert compute_horde_client._token == "test_token"
    requests = httpx_mock.get_requests()
    job_request = requests[-1]
    assert "Authorization" in job_request.headers
    assert job_request.headers["Authorization"] == "Bearer test_token"
    assert job.uuid == TEST_JOB_UUID
    assert job.status == "Accepted"


@pytest.mark.asyncio
async def test_retry_on_token_expire(apiver_module, compute_horde_client, httpx_mock):
    """
    Test that when a request returns a 401 "Token expired",
    the client re-authenticates and then retries request.
    """
    compute_horde_client._token = "expired_token"

    httpx_mock.add_response(
        url=f"{TEST_FACILITATOR_URL}/api/v1/jobs/{TEST_JOB_UUID}/", status_code=401, text="Token expired"
    )

    setup_successful_authentication(httpx_mock)

    # Retry: successful response for the retried job request.
    httpx_mock.add_response(
        url=f"{TEST_FACILITATOR_URL}/api/v1/jobs/{TEST_JOB_UUID}/",
        json=get_job_response(uuid=TEST_JOB_UUID, status="Accepted"),
    )

    job = await compute_horde_client.get_job(TEST_JOB_UUID)

    # Verify that the client's token was updated.
    assert compute_horde_client._token == "test_token"

    # Ensure that the final job request used the new token.
    requests = httpx_mock.get_requests()
    job_requests = [r for r in requests if r.url == f"{TEST_FACILITATOR_URL}/api/v1/jobs/{TEST_JOB_UUID}/"]
    # We expect two job requests: the initial (failed) one and the retried successful one.
    assert len(job_requests) == 2
    final_job_request = job_requests[-1]
    assert final_job_request.headers.get("Authorization") == "Bearer test_token"

    # Validate that the job response is successful.
    assert job.uuid == TEST_JOB_UUID
    assert job.status == "Accepted"
