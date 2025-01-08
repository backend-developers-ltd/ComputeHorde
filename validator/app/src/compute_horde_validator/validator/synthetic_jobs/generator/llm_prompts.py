import uuid

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

from .base import BaseSyntheticJobGenerator


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

    def _url_for_upload(self) -> str:
        return generate_upload_url(
            self.s3_output_key,
            bucket_name=self.s3_output_bucket,
            prefix=self.s3_output_prefix,
        )

    def _url_for_download(self) -> str:
        return get_public_url(
            key=self.s3_output_key,
            bucket_name=self.s3_output_bucket,
            prefix=self.s3_output_prefix,
        )

    def timeout_seconds(self) -> int:
        return 120

    def base_docker_image_name(self) -> str:
        return "docker.io/backenddevelopersltd/compute-horde-prompt-solver-staging:v0-latest"

    def docker_image_name(self) -> str:
        return "docker.io/backenddevelopersltd/compute-horde-prompt-solver-staging:v0-latest"

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

    async def download_answers(self):
        response = await download_file_content(self._url_for_download())
        self.prompt_answers = pydantic.TypeAdapter(dict[str, str]).validate_json(response)

    def verify(self, msg: V0JobFinishedRequest, time_took: float) -> tuple[bool, str, float]:
        # just check if there are any answers
        if self.prompt_answers == {}:
            return False, "no answers", 0.0
        return True, "answers exist", 1.0

    def job_description(self) -> str:
        return "LLM prompts job"

    def volume_in_initial_req(self) -> bool:
        return True


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
        self.prompt_sample: PromptSample = prompt_sample
        self.expected_prompts: list[Prompt] = expected_prompts

    def verify(self, msg: V0JobFinishedRequest, time_took: float) -> tuple[bool, str, float]:
        for expected_prompt in self.expected_prompts:
            if expected_prompt.content not in self.prompt_answers:
                return False, "result does not contain all answers", 0.0
            if expected_prompt.answer != self.prompt_answers[expected_prompt.content]:
                return False, "results does not match expected answers", 0.0

        return True, "", 1.0

    def job_description(self) -> str:
        return "LLM prompts synthetic job"
