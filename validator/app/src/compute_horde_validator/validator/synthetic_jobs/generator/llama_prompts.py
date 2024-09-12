import uuid

import httpx
import pydantic
from compute_horde.base.output_upload import MultiUpload, OutputUpload, SingleFilePutUpload
from compute_horde.base.volume import MultiVolume, SingleFileVolume, Volume
from compute_horde.mv_protocol.miner_requests import V0JobFinishedRequest
from django.conf import settings

from compute_horde_validator.validator.models import Prompt, PromptSample
from compute_horde_validator.validator.s3 import generate_upload_url, get_public_url

from .base import BaseSyntheticJobGenerator

_PROMPT_SAMPLE_RELATED_NOT_CACHED = (
    "The related objects of PromptSample needs to be cached before passing to this class"
)


class LlamaPromptsSyntheticJobGenerator(BaseSyntheticJobGenerator):
    def __init__(self, prompt_sample: PromptSample):
        super().__init__()

        assert PromptSample.series.is_cached(prompt_sample), _PROMPT_SAMPLE_RELATED_NOT_CACHED
        assert PromptSample.workload.is_cached(prompt_sample), _PROMPT_SAMPLE_RELATED_NOT_CACHED
        assert (
            getattr(prompt_sample, "_prefetched_objects_cache", {}).get("prompts") is not None
        ), _PROMPT_SAMPLE_RELATED_NOT_CACHED

        self.prompt_sample: PromptSample = prompt_sample
        self.prompts: list[Prompt] = list(self.prompt_sample.prompts.all())

        self.input_filename = str(uuid.uuid4()) + ".txt"
        self.s3_output_key = str(uuid.uuid4()) + ".json"
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
        # TODO: ???
        return 80

    def base_docker_image_name(self) -> str:
        return "docker.io/backenddevelopersltd/compute-horde-prompt-solver:v0-latest"

    def docker_image_name(self) -> str:
        return "docker.io/backenddevelopersltd/compute-horde-prompt-solver:v0-latest"

    def docker_run_options_preset(self) -> str:
        return "nvidia_all"

    def docker_run_cmd(self) -> list[str]:
        return [
            "--temperature=0.5",
            "--top-p=0.8",
            "--max-tokens=256",
            "--seed",
            str(self.prompt_sample.workload.seed),
            f"/volume/{self.input_filename}",
        ]

    async def volume(self) -> Volume | None:
        return MultiVolume(
            volumes=[
                SingleFileVolume(
                    url=self.prompt_sample.series.s3_url,
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

    async def _download_answers(self):
        async with httpx.AsyncClient() as client:
            response = await client.get(self._url_for_download(), timeout=5)
            response.raise_for_status()
            self.prompt_answers = pydantic.TypeAdapter(dict[str, str]).validate_json(
                response.content
            )

    def verify(self, msg: V0JobFinishedRequest, time_took: float) -> tuple[bool, str, float]:
        for prompt in self.prompts:
            if prompt.content not in self.prompt_answers:
                return False, "result does not contain all answers", 0.0
            if prompt.answer != self.prompt_answers[prompt.content]:
                return False, "results does not match expected answers", 0.0

        return True, "", 1.0

    def job_description(self) -> str:
        return "LLAMA prompts synthetic job"