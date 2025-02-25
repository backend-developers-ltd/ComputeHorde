import json
from typing import TYPE_CHECKING

import bittensor
import httpx
import pytest

from _compute_horde_models.signature import (
    BittensorWalletVerifier,
    SignedFields,
    signature_from_headers,
)

if TYPE_CHECKING:
    from compute_horde_sdk._internal.sdk import ComputeHordeClient, ComputeHordeJob

TEST_FACILITATOR_URL = "http://localhost:4321/api/v1"
TEST_JOB_UUID = "1c4904f0-b614-4e27-8e50-7bfc5ddab8fd"
TEST_DOCKER_IMAGE = "example-com/test-image"


def get_job_response(uuid: str = TEST_JOB_UUID, status: str = "Accepted", **kwargs):
    return {
        "uuid": uuid,
        "executor_class": "spin_up_4min__gpu_24gb",
        "created_at": "2025-01-29T14:32:46Z",
        "last_update": "2025-01-29T14:32:46Z",
        "status": status,
        "docker_image": TEST_DOCKER_IMAGE,
        "raw_script": None,
        "args": "--test yes",
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

    signed_fields = SignedFields.from_facilitator_sdk_json(json_body)
    verifier.verify(signed_fields.model_dump_json(), signature)


@pytest.fixture
def keypair():
    return bittensor.Keypair.create_from_mnemonic(
        "slot excuse valid grief praise rifle spoil auction weasel glove pen share"
    )


@pytest.fixture
def compute_horde_client(keypair, apiver_module) -> "ComputeHordeClient":
    return apiver_module.ComputeHordeClient(
        hotkey=keypair,
        compute_horde_validator_hotkey="abcdef",
        job_queue="sn123",
        facilitator_url=TEST_FACILITATOR_URL,
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
        url=TEST_FACILITATOR_URL + "/job-docker/",
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

    job = await client.create_job(
        executor_class=apiver_module.ExecutorClass.spin_up_4min__gpu_24gb,
        job_namespace="SN123.0",
        docker_image=TEST_DOCKER_IMAGE,
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
            "next": f"{TEST_FACILITATOR_URL}/jobs/?page=2",
            "results": [get_job_response(uuid=job1_uuid, status=job1_status) for _ in range(10)],
        }
    )
    httpx_mock.add_response(
        json={
            "count": 11,
            "previous": f"{TEST_FACILITATOR_URL}/jobs/?page=1",
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
            args="--arg1 value1",
            env={"ENV_VAR": "value"},
            use_gpu=True,
            input_url="https://example.com/input",
        )
    )

    job = await compute_horde_client.create_job(
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

    assert job.uuid == TEST_JOB_UUID

    request = httpx_mock.get_request()
    assert_signature(request)
    req_json = json.loads(request.content)
    assert req_json["executor_class"] == "spin_up-4min.gpu-24gb"
    assert req_json["docker_image"] == TEST_DOCKER_IMAGE
    assert req_json["args"] == "--block 10000"
    assert req_json["env"] == {"TEST_ENV": "1"}
    assert req_json["volumes"] == [
        {
            "volume_type": "huggingface_volume",
            "relative_path": "models/model01",
            "repo_id": "myrepo/mymodel",
            "repo_type": None,
            "revision": None,
            "allow_patterns": None,
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
            executor_class=apiver_module.ExecutorClass.spin_up_4min__gpu_24gb,
            job_namespace="SN123.0",
            docker_image=TEST_DOCKER_IMAGE,
        )


@pytest.mark.asyncio
async def test_create_job__malformed_response(apiver_module, compute_horde_client, httpx_mock):
    httpx_mock.add_response(json=[{"derp": "dunno"}])

    with pytest.raises(apiver_module.ComputeHordeError):
        await compute_horde_client.create_job(
            executor_class=apiver_module.ExecutorClass.spin_up_4min__gpu_24gb,
            job_namespace="SN123.0",
            docker_image=TEST_DOCKER_IMAGE,
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
