from compute_horde_core.executor_class import ExecutorClass
from compute_horde_core.output_upload import (
    MultiUpload,
    OutputUpload,
    SingleFilePutUpload,
    SingleFileUpload,
)
from django.conf import settings

from ..base import BasePromptJobGenerator


class PromptJobGenerator(BasePromptJobGenerator):
    def generator_version(self) -> int:
        return 0

    def timeout_seconds(self) -> int:
        return 5 * 60

    def docker_image_name(self) -> str:
        return f"backenddevelopersltd/compute-horde-prompt-gen-{settings.PROMPT_GENERATION_MODEL}:v0-latest"

    def executor_class(self) -> ExecutorClass:
        return ExecutorClass.always_on__llm__a6000

    def docker_run_cmd(self) -> list[str]:
        return [
            "--quantize",
            "--model_name",
            settings.PROMPT_GENERATION_MODEL,
            "--batch_size=250",  # on A6000 we want 240 prompts generated in single file, but not all results are valid
            "--num_return_sequences=1",
            "--max_new_tokens=40",  # 40 new tokens is enough for reasonable length prompt - 30 caused too much cut off prompts
            "--number_of_prompts_per_batch",
            str(self.num_prompts_per_batch),
            "--uuids",
            str(",".join(map(str, self.batch_uuids))),
        ]

    def output(self) -> OutputUpload | None:
        uploads: list[SingleFileUpload] = []

        for batch_uuid, url in zip(self.batch_uuids, self.upload_urls):
            uploads.append(
                SingleFilePutUpload(
                    url=url,
                    relative_path=f"prompts_{batch_uuid}.txt",
                )
            )

        return MultiUpload(uploads=uploads)
