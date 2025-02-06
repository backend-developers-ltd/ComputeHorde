import asyncio
import hashlib
import logging
import tempfile
import time
import uuid

import httpx
import pydantic
from compute_horde.base.docker import DockerRunOptionsPreset
from compute_horde.base.output_upload import MultiUpload, OutputUpload, SingleFilePutUpload
from compute_horde.base.volume import MultiVolume, SingleFileVolume, Volume
from compute_horde.mv_protocol.miner_requests import V0JobFinishedRequest
from django.conf import settings

from compute_horde_validator.validator.models import Prompt, PromptSample
from compute_horde_validator.validator.s3 import (
    download_file_content,
    generate_upload_url,
    get_public_url,
)

from ...dynamic_config import aget_config
from .base import BaseSyntheticJobGenerator

logger = logging.getLogger(__name__)


STREAMING_PROCESSING_TIMEOUT = 30
STREAMING_PROCESSING_TIMEOUT_LEEWAY = 5


class LlmPromptsJobGenerator(BaseSyntheticJobGenerator):
    def __init__(
        self,
        s3_url: str,
        seed: int,
        streaming: bool = False,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.seed = seed
        self.s3_url = s3_url
        self.streaming = streaming
        file_uuid = str(uuid.uuid4())
        self.input_filename = file_uuid + ".txt"
        self.s3_output_key = file_uuid + ".json"
        self.s3_output_prefix = "solved/"
        self.s3_output_bucket = settings.S3_BUCKET_NAME_ANSWERS

        self.prompt_answers: dict[str, str] = {}
        self.streaming_processing_time: float | None = None

        self.downloaded_answers_hash: str | None = None

    def _url_for_upload(self) -> str:
        return generate_upload_url(
            self.s3_output_key,
            bucket_name=self.s3_output_bucket,
            prefix=self.s3_output_prefix,
        )

    def url_for_download(self) -> str:
        return get_public_url(
            key=self.s3_output_key,
            bucket_name=self.s3_output_bucket,
            prefix=self.s3_output_prefix,
        )

    def timeout_seconds(self) -> int:
        return 60  # it takes around 42s - add buffer for streaming

    def base_docker_image_name(self) -> str:
        return "docker.io/backenddevelopersltd/compute-horde-prompt-solver:v0-latest"

    def docker_image_name(self) -> str:
        return "docker.io/backenddevelopersltd/compute-horde-prompt-solver:v0-latest"

    def docker_run_options_preset(self) -> DockerRunOptionsPreset:
        return "nvidia_all"

    def docker_run_cmd(self) -> list[str]:
        cmd = [
            "--temperature=0.5",
            "--top-p=0.8",
            "--max-tokens=256",
        ]
        if self.streaming:
            # for streaming jobs, do not pass the seed yet, just run it in server mode
            # the job will be triggered hitting /execute-job endpoint with the seed as payload
            cmd.append("--server")
        else:
            cmd.extend(["--seed", str(self.seed)])
        # cmd.append("--mock")
        cmd.append(f"/volume/{self.input_filename}")
        return cmd

    async def volume(self) -> Volume | None:
        return MultiVolume(
            volumes=[
                SingleFileVolume(
                    url=self.s3_url,
                    relative_path=self.input_filename,
                ),
            ]
        )

    async def output_upload(self) -> OutputUpload | None:
        return MultiUpload(
            uploads=[
                SingleFilePutUpload(
                    url=self._url_for_upload(),
                    relative_path=self.s3_output_key,
                ),
            ]
        )

    async def download_answers(self, client: httpx.AsyncClient | None = None):
        response = await download_file_content(self.url_for_download(), client=client)
        self.downloaded_answers_hash = hashlib.sha256(response).hexdigest()
        self.prompt_answers = pydantic.TypeAdapter(dict[str, str]).validate_json(response)

    def verify_time(self, time_took: float) -> bool | None:
        return True

    def verify_correctness(self, msg: V0JobFinishedRequest) -> tuple[bool, str]:
        # just check if there are any answers
        if self.prompt_answers == {}:
            return False, "no answers"
        return True, "answers exist"

    def job_description(self) -> str:
        return "LLM prompts job"

    def volume_in_initial_req(self) -> bool:
        return True

    async def streaming_preparation_timeout(self) -> float | None:
        """For streaming jobs, the timeout between sending a JobRequest and receiving StreamingReadyRequest"""
        # TODO: caching
        val = await aget_config("DYNAMIC_SYNTHETIC_STREAMING_JOB_READY_TIMEOUT")
        return float(val) if val is not None else None


class LlmPromptsSyntheticJobGenerator(LlmPromptsJobGenerator):
    def __init__(
        self,
        prompt_sample: PromptSample,
        expected_prompts: list[Prompt],
        s3_url: str,
        seed: int,
        streaming: bool = False,
        **kwargs,
    ):
        super().__init__(
            s3_url=s3_url,
            seed=seed,
            streaming=streaming,
            **kwargs,
        )
        self.response_hash: str | None = None
        self.prompt_sample: PromptSample = prompt_sample
        self.expected_prompts: list[Prompt] = expected_prompts

    def verify(self, msg: V0JobFinishedRequest, time_took: float) -> tuple[bool, str, float]:
        for expected_prompt in self.expected_prompts:
            if expected_prompt.content not in self.prompt_answers:
                return False, "result does not contain all answers", 0.0
            if expected_prompt.answer != self.prompt_answers[expected_prompt.content]:
                return False, "results does not match expected answers", 0.0

        return True, "", 1.0

    def verify_time(self, time_took: float) -> bool | None:
        if not self.streaming:
            return time_took <= self.timeout_seconds()
        if self.streaming_processing_time is None:
            return None
        return self.streaming_processing_time <= STREAMING_PROCESSING_TIMEOUT

    def verify_correctness(self, msg: V0JobFinishedRequest) -> tuple[bool, str]:
        if self.response_hash is None:
            return False, "Response did not contain a valid answer file hash or timed out"
        if self.response_hash != self.downloaded_answers_hash:
            return False, (f"Response hash and downloaded file hash don't match: "
                           f"{self.response_hash=}, {self.downloaded_answers_hash=}")
        for expected_prompt in self.expected_prompts:
            if expected_prompt.content not in self.prompt_answers:
                return False, "result does not contain all answers"
            if expected_prompt.answer != self.prompt_answers[expected_prompt.content]:
                return False, "results does not match expected answers"

        return True, ""

    def job_description(self) -> str:
        return ("Streaming " if self.streaming else "") + "LLM prompts synthetic job"

    async def trigger_streaming_job_execution(
        self,
        job_uuid,
        start_barrier: asyncio.Barrier,
        server_public_key,
        client_key_pair,
        server_address,
        server_port,
    ):
        with tempfile.NamedTemporaryFile(delete=True, mode="w+") as executor_cert_file:
            executor_cert_file.write(server_public_key)
            executor_cert_file.flush()

            logger.debug(f"Waiting for streaming start barrier for {job_uuid}")
            await start_barrier.wait()
            logger.debug(f"Passed streaming start barrier for {job_uuid}")

            url = f"https://{server_address}:{server_port}/execute-job"
            logger.debug("About to send seed to %s (job_uuid=%s)", url, job_uuid)

            async with httpx.AsyncClient(
                verify=executor_cert_file.name,
                cert=client_key_pair,
                timeout=STREAMING_PROCESSING_TIMEOUT + STREAMING_PROCESSING_TIMEOUT_LEEWAY,
            ) as client:
                # send the seed to the executor to start the streaming job
                try:
                    t_before = time.time()
                    r = await client.post(
                        url, json={"seed": self.seed}, headers={"Host": server_address}
                    )
                except Exception as e:
                    msg = f"Failed to execute streaming job {job_uuid} on {url}: {e}"
                    logger.debug(msg)
                else:
                    try:
                        r.raise_for_status()
                        self.streaming_processing_time = time.time() - t_before
                    except Exception as e:
                        msg = f"Failed to execute streaming job {job_uuid} on {url}: {e}, the response was: {r.content[:100]!r}"
                        logger.debug(msg)
                    else:
                        logger.debug(
                            "Successfully sent seed to %s (job_uuid=%s), the result is: %s",
                            url,
                            job_uuid,
                            r.content[:100],
                        )
                        try:
                            self.response_hash = next(iter(r.json().values()), None)
                        except:
                            logger.debug("Malformed response from %s (job_uuid=%s)", url, job_uuid)

                url = f"https://{server_address}:{server_port}/terminate"
                logger.debug("About to terminate (job_uuid=%s)", job_uuid)
                try:
                    r = await client.get(url, headers={"Host": server_address})
                except Exception as e:
                    msg = f"Failed to terminate streaming job {job_uuid} on {url}: {e}"
                    logger.debug(msg)
                else:
                    try:
                        r.raise_for_status()
                    except Exception as e:
                        msg = f"Failed to terminate streaming job {job_uuid} on {url}: {e}, the response was: {r.content[:100]!r}"
                        logger.debug(msg)
                    else:
                        logger.debug(
                            "Successfully terminated %s (job_uuid=%s)",
                            url,
                            job_uuid,
                        )
