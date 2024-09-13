from compute_horde.base.output_upload import MultiUpload, SingleFilePutUpload

from ..base import BasePromptJobGenerator


class PromptJobGenerator(BasePromptJobGenerator):
    def generator_version(self) -> int:
        return 0

    def timeout_seconds(self) -> int:
        return 3600

    def docker_image_name(self) -> str:
        return "backenddevelopersltd/compute-horde-prompt-gen:v0-latest"

    def docker_run_cmd(self) -> list[str]:
        return [
            "--model_name",
            "phi3",
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
