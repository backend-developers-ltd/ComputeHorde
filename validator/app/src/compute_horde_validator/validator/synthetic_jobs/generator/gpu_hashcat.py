import base64
import io
import zipfile

from compute_horde.mv_protocol.miner_requests import V0JobFinishedRequest
from compute_horde_validator.validator.jobs import V0SyntheticJob
from compute_horde_validator.validator.synthetic_jobs.generator.base import AbstractSyntheticJobGenerator


MAX_SCORE = 2


class GPUHashcatSyntheticJobGenerator(AbstractSyntheticJobGenerator):
    def __init__(self):
        self.hash_job = V0SyntheticJob.generate()
        self.expected_answer = f'{self.hash_job.payload}:{self.hash_job.answer}'

    def timeout_seconds(self) -> int:
        return 90

    def base_docker_image_name(self) -> str:
        return "ghcr.io/backend-developers-ltd/computehorde/job:v0"

    def docker_image_name(self) -> str:
        return "ghcr.io/backend-developers-ltd/computehorde/job:v0"

    def volume_contents(self) -> str:
        in_memory_output = io.BytesIO()
        zipf = zipfile.ZipFile(in_memory_output, 'w')
        zipf.writestr('payload.txt', self.hash_job.payload)
        zipf.close()
        in_memory_output.seek(0)
        zip_contents = in_memory_output.read()
        return base64.b64encode(zip_contents).decode()

    def verify(self, msg: V0JobFinishedRequest, time_took: float) -> tuple[bool, str, float]:
        if msg.docker_process_stdout.strip() != self.expected_answer:
            return (
                False,
                f'result does not match expected answer: expected answer={self.expected_answer} msg={msg.json()}',
                0,
            )
        score = MAX_SCORE * (1 - (time_took / (2 * self.timeout_seconds())))
        return True, '', score
