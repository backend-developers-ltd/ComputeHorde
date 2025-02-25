import json

import pydantic
import pytest
import pytest_asyncio
from compute_horde.base.output_upload import (
    OutputUploadType,
    SingleFilePostUpload,
)
from compute_horde.base.volume import (
    SingleFileVolume,
    VolumeType,
    ZipUrlVolume,
)
from compute_horde.executor_class import DEFAULT_EXECUTOR_CLASS, ExecutorClass  # type: ignore

from compute_horde_facilitator_sdk._internal.api_models import Volume


class RawJobPayload(pydantic.BaseModel):
    raw_script: str
    input_url: str = ""
    uploads: list[SingleFilePostUpload] | None = None
    volumes: list[Volume] | None = None


class DockerJobPayload(pydantic.BaseModel):
    docker_image: str
    args: str = ""
    env: dict[str, str] | None = None
    use_gpu: bool = False
    input_url: str = ""
    executor_class: ExecutorClass = DEFAULT_EXECUTOR_CLASS
    uploads: list[SingleFilePostUpload] | None = None
    volumes: list[Volume] | None = None
    target_validator_hotkey: str | None = None


@pytest.fixture
def base_url():
    return "https://example.com"


@pytest.fixture
def token():
    return "your_token"


@pytest.fixture
def signer(apiver_module, bittensor_wallet):
    return apiver_module.BittensorWalletSigner(bittensor_wallet)


@pytest.fixture
def verifier(apiver_module):
    return apiver_module.BittensorWalletVerifier()


@pytest.fixture
def verified_httpx_mock(httpx_mock, verifier, apiver_module):
    yield

    for request in httpx_mock.get_requests():
        signature = None
        try:
            signature = apiver_module.signature_from_headers(request.headers)
        except Exception:
            pass
        if signature and request.content:
            verifier = apiver_module.VERIFIERS_REGISTRY.get(signature.signature_type)
            signed_fields = apiver_module.SignedFields.from_facilitator_sdk_json(json.loads(request.content))
            verifier.verify(signed_fields.model_dump_json(), signature)


@pytest.fixture
def facilitator_client(apiver_module, base_url, token, signer):
    return apiver_module.FacilitatorClient(token, base_url, signer=signer)


@pytest_asyncio.fixture
async def async_facilitator_client(apiver_module, base_url, token, signer):
    async with apiver_module.AsyncFacilitatorClient(token, base_url, signer=signer) as client:
        yield client


@pytest.fixture
def output_uploads():
    return [
        SingleFilePostUpload(
            output_upload_type=str(OutputUploadType.single_file_post),
            url="https://example.com/upload?signature=eedd1234",
            relative_path="output.txt",
            signed_headers={
                "content-type": "application/text",
                "content-disposition": 'attachment; filename="filename.jpg"',
            },
        )
    ]


@pytest.fixture
def volumes():
    return [
        ZipUrlVolume(volume_type=VolumeType.zip_url, contents="https://example.com/input.zip", relative_path="input"),
        SingleFileVolume(
            volume_type=VolumeType.single_file,
            url="https://example.com/example.jpg",
            relative_path="examples/example.jpg",
        ),
    ]


def test_get_jobs(facilitator_client, httpx_mock, verified_httpx_mock):
    expected_response = {"results": [{"id": 1, "name": "Job 1"}, {"id": 2, "name": "Job 2"}]}
    httpx_mock.add_response(json=expected_response)
    response = facilitator_client.get_jobs()
    assert response == expected_response


def test_get_job(facilitator_client, httpx_mock, verified_httpx_mock):
    job_uuid = "abc123"
    expected_response = {"id": 1, "name": "Job 1"}
    httpx_mock.add_response(json=expected_response)
    response = facilitator_client.get_job(job_uuid)
    assert response == expected_response


def test_create_raw_job(facilitator_client, httpx_mock, verified_httpx_mock):
    raw_script = "echo 'Hello, World!'"
    input_url = "https://example.com/input"
    expected_response = {"id": 1, "status": "queued"}
    httpx_mock.add_response(json=expected_response)
    response = facilitator_client.create_raw_job(raw_script, input_url)
    assert response == expected_response

    request = httpx_mock.get_request()
    payload = RawJobPayload.model_validate_json(request.content)
    assert payload == RawJobPayload(
        raw_script=raw_script,
        input_url=input_url,
    )


def test_create_docker_job(facilitator_client, httpx_mock, verified_httpx_mock):
    docker_image = "my-image"
    args = "--arg1 value1"
    env = {"ENV_VAR": "value"}
    use_gpu = True
    input_url = "https://example.com/input"
    expected_response = {"id": 1, "status": "queued"}
    httpx_mock.add_response(json=expected_response)
    response = facilitator_client.create_docker_job(
        executor_class=DEFAULT_EXECUTOR_CLASS,
        docker_image=docker_image,
        args=args,
        env=env,
        use_gpu=use_gpu,
        input_url="https://example.com/input",
        target_validator_hotkey="5Hotkey",
    )
    assert response == expected_response

    request = httpx_mock.get_request()
    payload = DockerJobPayload.model_validate_json(request.content)
    assert payload == DockerJobPayload(
        docker_image=docker_image,
        args=args,
        env=env,
        use_gpu=use_gpu,
        input_url=input_url,
        executor_class=DEFAULT_EXECUTOR_CLASS,
        target_validator_hotkey="5Hotkey",
    )


def test_create_raw_job_uploads_volumes(facilitator_client, httpx_mock, verified_httpx_mock, output_uploads, volumes):
    raw_script = "echo 'Hello, World!'"
    input_url = "https://example.com/input"
    expected_response = {"id": 1, "status": "queued"}
    httpx_mock.add_response(json=expected_response)
    response = facilitator_client.create_raw_job(raw_script, input_url, uploads=output_uploads, volumes=volumes)
    assert response == expected_response

    request = httpx_mock.get_request()
    payload = RawJobPayload.model_validate_json(request.content)
    assert payload == RawJobPayload(
        raw_script=raw_script,
        input_url=input_url,
        uploads=output_uploads,
        volumes=volumes,
    )


def test_create_docker_job_uploads_volumes(
    facilitator_client, httpx_mock, verified_httpx_mock, output_uploads, volumes
):
    docker_image = "my-image"
    args = "--arg1 value1"
    env = {"ENV_VAR": "value"}
    use_gpu = True
    input_url = "https://example.com/input"
    expected_response = {"id": 1, "status": "queued"}
    httpx_mock.add_response(json=expected_response)

    response = facilitator_client.create_docker_job(
        docker_image, args, env, use_gpu, input_url, uploads=output_uploads, volumes=volumes
    )
    assert response == expected_response

    request = httpx_mock.get_request()
    payload = json.loads(request.content)
    payload = DockerJobPayload.model_validate_json(request.content)
    assert payload == DockerJobPayload(
        docker_image=docker_image,
        args=args,
        env=env,
        use_gpu=use_gpu,
        input_url=input_url,
        uploads=output_uploads,
        volumes=volumes,
        executor_class=DEFAULT_EXECUTOR_CLASS,
        target_validator_hotkey=None,
    )


@pytest.mark.asyncio
async def test_async_get_jobs(async_facilitator_client, httpx_mock, verified_httpx_mock):
    expected_response = {"results": [{"id": 1, "name": "Job 1"}, {"id": 2, "name": "Job 2"}]}
    httpx_mock.add_response(json=expected_response)
    response = await async_facilitator_client.get_jobs()
    assert response == expected_response


@pytest.mark.asyncio
async def test_async_get_job(async_facilitator_client, httpx_mock, verified_httpx_mock):
    job_uuid = "abc123"
    expected_response = {"id": 1, "name": "Job 1"}
    httpx_mock.add_response(json=expected_response)
    response = await async_facilitator_client.get_job(job_uuid)
    assert response == expected_response


@pytest.mark.asyncio
async def test_async_create_raw_job(async_facilitator_client, httpx_mock, verified_httpx_mock):
    raw_script = "echo 'Hello, World!'"
    input_url = "https://example.com/input"
    expected_response = {"id": 1, "status": "queued"}
    httpx_mock.add_response(json=expected_response)
    response = await async_facilitator_client.create_raw_job(raw_script, input_url)
    assert response == expected_response

    request = httpx_mock.get_request()
    payload = json.loads(request.content)
    payload = RawJobPayload.model_validate_json(request.content)
    assert payload == RawJobPayload(
        raw_script=raw_script,
        input_url=input_url,
    )


@pytest.mark.asyncio
async def test_async_create_docker_job(async_facilitator_client, httpx_mock, verified_httpx_mock):
    executor_class = DEFAULT_EXECUTOR_CLASS
    docker_image = "my-image"
    args = "--arg1 value1"
    env = {"ENV_VAR": "value"}
    use_gpu = True
    input_url = "https://example.com/input"
    expected_response = {"id": 1, "status": "queued"}
    httpx_mock.add_response(json=expected_response)
    response = await async_facilitator_client.create_docker_job(
        docker_image, args, env, use_gpu, input_url, executor_class
    )
    assert response == expected_response

    request = httpx_mock.get_request()
    payload = json.loads(request.content)
    payload = DockerJobPayload.model_validate_json(request.content)
    assert payload == DockerJobPayload(
        docker_image=docker_image,
        args=args,
        env=env,
        use_gpu=use_gpu,
        input_url=input_url,
        executor_class=DEFAULT_EXECUTOR_CLASS,
        target_validator_hotkey=None,
    )


@pytest.mark.asyncio
async def test_async_create_raw_job_uploads_volumes(
    async_facilitator_client, httpx_mock, verified_httpx_mock, output_uploads, volumes
):
    raw_script = "echo 'Hello, World!'"
    input_url = "https://example.com/input"
    expected_response = {"id": 1, "status": "queued"}
    httpx_mock.add_response(json=expected_response)
    response = await async_facilitator_client.create_raw_job(
        raw_script, input_url, uploads=output_uploads, volumes=volumes
    )
    assert response == expected_response

    request = httpx_mock.get_request()
    payload = json.loads(request.content)
    payload = RawJobPayload.model_validate_json(request.content)
    assert payload == RawJobPayload(
        raw_script=raw_script,
        input_url=input_url,
        uploads=output_uploads,
        volumes=volumes,
    )


@pytest.mark.asyncio
async def test_async_create_docker_job_uploads_volumes(
    async_facilitator_client, httpx_mock, verified_httpx_mock, output_uploads, volumes
):
    docker_image = "my-image"
    args = "--arg1 value1"
    env = {"ENV_VAR": "value"}
    use_gpu = True
    input_url = "https://example.com/input"
    expected_response = {"id": 1, "status": "queued"}
    httpx_mock.add_response(json=expected_response)
    response = await async_facilitator_client.create_docker_job(
        docker_image, args, env, use_gpu, input_url, uploads=output_uploads, volumes=volumes
    )
    assert response == expected_response

    request = httpx_mock.get_request()
    payload = json.loads(request.content)
    payload = DockerJobPayload.model_validate_json(request.content)
    assert payload == DockerJobPayload(
        docker_image=docker_image,
        args=args,
        env=env,
        use_gpu=use_gpu,
        input_url=input_url,
        uploads=output_uploads,
        volumes=volumes,
        executor_class=DEFAULT_EXECUTOR_CLASS,
        target_validator_hotkey=None,
    )


@pytest.fixture
def job_uuid():
    return "test-job-uuid"


@pytest.fixture
def sent_job_state(job_uuid):
    return {"uuid": job_uuid, "status": "Sent"}


@pytest.fixture
def completed_job_state(job_uuid):
    return {"uuid": job_uuid, "status": "Completed"}


@pytest.mark.parametrize("final_status", ["Completed", "Failed", "Error", "Cancelled"])
def test_wait_for_job__various_end_states(
    final_status,
    facilitator_client,
    httpx_mock,
    verified_httpx_mock,
    sleep_mock,
    job_uuid,
    sent_job_state,
    completed_job_state,
):
    expected_state = {**completed_job_state, "status": final_status}
    httpx_mock.add_response(json=sent_job_state)
    httpx_mock.add_response(json=expected_state)

    assert facilitator_client.wait_for_job(job_uuid) == expected_state


@pytest.mark.parametrize("final_status", ["Completed", "Failed", "Error", "Cancelled"])
@pytest.mark.asyncio
async def test_async_wait_for_job__various_end_states(
    final_status,
    async_facilitator_client,
    httpx_mock,
    verified_httpx_mock,
    async_sleep_mock,
    job_uuid,
    sent_job_state,
    completed_job_state,
):
    expected_state = {**completed_job_state, "status": final_status}
    httpx_mock.add_response(json=sent_job_state)
    httpx_mock.add_response(json=expected_state)

    assert (await async_facilitator_client.wait_for_job(job_uuid)) == expected_state


def test_wait_for_job__immediate_completion(
    facilitator_client, httpx_mock, verified_httpx_mock, sleep_mock, completed_job_state
):
    job_uuid = "test-job-uuid"

    httpx_mock.add_response(json=completed_job_state)

    result = facilitator_client.wait_for_job(job_uuid)
    assert result == completed_job_state


def test_wait_for_job__timeout(
    apiver_module, facilitator_client, httpx_mock, verified_httpx_mock, sleep_mock, job_uuid, sent_job_state
):
    httpx_mock.add_response(json=sent_job_state)

    with pytest.raises(apiver_module.FacilitatorClientTimeoutException) as exc_info:
        facilitator_client.wait_for_job(job_uuid, timeout=0)

    assert f"Job {job_uuid} did not complete within 0 seconds" in str(exc_info.value)


def test_submit_job_feedback(facilitator_client, httpx_mock, verified_httpx_mock):
    job_uuid = "abc123"
    result_correctness = 0.9
    expected_duration = 10.0
    feedback = {"result_correctness": result_correctness, "expected_duration": expected_duration}
    httpx_mock.add_response(method="PUT", url=f"https://example.com/jobs/{job_uuid}/feedback/", json=feedback)

    response = facilitator_client.submit_job_feedback(
        job_uuid, result_correctness=result_correctness, expected_duration=expected_duration
    )

    assert response == feedback
    assert json.loads(httpx_mock.get_request().content) == feedback


@pytest.mark.asyncio
async def test_async_submit_job_feedback(async_facilitator_client, httpx_mock, verified_httpx_mock):
    job_uuid = "abc123"
    result_correctness = 0.9
    expected_duration = 10.0
    feedback = {"result_correctness": result_correctness, "expected_duration": expected_duration}
    httpx_mock.add_response(method="PUT", url=f"https://example.com/jobs/{job_uuid}/feedback/", json=feedback)

    response = await async_facilitator_client.submit_job_feedback(
        job_uuid, result_correctness=result_correctness, expected_duration=expected_duration
    )

    assert response == feedback
    assert json.loads(httpx_mock.get_request().content) == feedback
