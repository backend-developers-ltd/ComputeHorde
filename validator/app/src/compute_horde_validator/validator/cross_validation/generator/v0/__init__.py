from compute_horde.base.output_upload import MultiUpload, SingleFilePutUpload
from compute_horde.executor_class import ExecutorClass
from django.conf import settings

from ..base import BasePromptJobGenerator


class PromptJobGenerator(BasePromptJobGenerator):
    def generator_version(self) -> int:
        return 0

    def timeout_seconds(self) -> int:
        return 3600

    def docker_image_name(self) -> str:
        return f"backenddevelopersltd/compute-horde-prompt-gen-{settings.PROMPT_GENERATION_MODEL}:v0-latest"

    def executor_class(self) -> ExecutorClass:
        return ExecutorClass.always_on__llm__a6000

    def docker_run_cmd(self) -> list[str]:
        return [
            "--model_name",
            settings.PROMPT_GENERATION_MODEL,
            "--number_of_prompts_per_batch",
            str(self.num_prompts_per_batch),
            "--uuids",
            str(",".join(map(str, self.batch_uuids))),
        ]

    def output(self) -> str | None:
        uploads = []

        for batch_uuid, url in zip(self.batch_uuids, self.upload_urls):
            uploads.append(
                SingleFilePutUpload(
                    url=url,
                    relative_path=f"prompts_{batch_uuid}.txt",
                )
            )

        return MultiUpload(uploads=uploads)
