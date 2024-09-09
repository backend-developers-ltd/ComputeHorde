import uuid

import httpx
import pydantic
from compute_horde.base.output_upload import OutputUpload, SingleFilePutUpload
from compute_horde.base.volume import SingleFileVolume, Volume
from compute_horde.mv_protocol.miner_requests import V0JobFinishedRequest
from django.conf import settings
from pydantic import BaseModel

from compute_horde_validator.validator.models import PromptSample
from compute_horde_validator.validator.s3 import generate_upload_url, get_public_url

from .base import BaseSyntheticJobGenerator


class PromptAnswer(BaseModel):
    prompt: str
    answer: str


class LlamaPromptsSyntheticJobGenerator(BaseSyntheticJobGenerator):
    def __init__(self, prompt_sample: PromptSample):
        super().__init__()
        self.prompt_sample: PromptSample = prompt_sample

        self.s3_output_key = str(uuid.uuid4()) + ".json"
        self.s3_output_prefix = "solved/"
        self.s3_output_bucket = settings.S3_BUCKET_NAME_ANSWERS

        self.prompt_answers: list[PromptAnswer] | None = None

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
        # TODO: ???
        return 80

    def base_docker_image_name(self) -> str:
        return "TODO"

    def docker_image_name(self) -> str:
        return "TODO"

    def docker_run_options_preset(self) -> str:
        return "nvidia_all"

    def docker_run_cmd(self) -> list[str]:
        return ["--seed", str(self.prompt_sample.workload.seed)]

    async def volume(self) -> Volume | None:
        return SingleFileVolume(url=self.prompt_sample.series.s3_url, relative_path="prompts.txt")

    async def output_upload(self) -> OutputUpload | None:
        return SingleFilePutUpload(url=self._url_for_upload(), relative_path="answers.json")

    async def _download_answers(self):
        async with httpx.AsyncClient() as client:
            response = await client.get(self._url_for_download(), timeout=5)
            self.prompt_answers = pydantic.TypeAdapter(list[PromptAnswer]).validate_json(
                response.content
            )

    def verify(self, msg: V0JobFinishedRequest, time_took: float) -> tuple[bool, str, float]:
        if not self.prompt_answers:
            raise RuntimeError("_download_answers must be called before calling verify")

        for prompt in self.prompt_sample.prompts.all():
            for prompt_answer in self.prompt_answers:
                if prompt_answer.prompt != prompt.content:
                    continue
                if prompt_answer.answer != prompt.answer:
                    return False, "results does not match expected answers", 0
                break
            else:
                # did not find answer for this prompt
                return False, "result does not contain all answers", 0

        return True, "", 1

    def job_description(self) -> str:
        return "LLAMA prompts synthetic job"
