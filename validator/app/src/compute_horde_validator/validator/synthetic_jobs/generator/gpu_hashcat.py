import base64
import datetime
import io
import zipfile

from compute_horde.mv_protocol.miner_requests import V0JobFinishedRequest

from compute_horde_validator.validator.jobs import Algorithm, V0SyntheticJob
from compute_horde_validator.validator.synthetic_jobs.generator.base import (
    AbstractSyntheticJobGenerator,
)

MAX_SCORE = 2


class GPUHashcatSyntheticJobGenerator(AbstractSyntheticJobGenerator):
    algorithms = [Algorithm.SHA256, Algorithm.SHA384, Algorithm.SHA512]

    def __init__(self):
        self.algorithm = self.get_algorithm()
        self.password_length, self.hashcat_hash_type, self._timeout = self.algo_to_params(self.algorithm)
        self.hash_job = V0SyntheticJob.generate(self.algorithm, password_length=self.password_length)
        self.expected_answer = self.hash_job.answer

    @classmethod
    def get_algorithm(cls) -> Algorithm:
        return cls.algorithms[datetime.datetime.utcnow().hour % len(cls.algorithms)]

    @classmethod
    def algo_to_params(cls, algo: Algorithm):
        if algo == Algorithm.SHA256:
            return 6, "1410", 90
        elif algo == Algorithm.SHA384:
            return 5, "10810", 45
        elif algo == Algorithm.SHA512:
            return 5, "1710", 45
        else:
            raise ValueError(f"Invalid algorithm: {algo}")

    def timeout_seconds(self) -> int:
        return self._timeout

    def base_docker_image_name(self) -> str:
        return "backenddevelopersltd/compute-horde-job:v0-latest"

    def docker_image_name(self) -> str:
        return "backenddevelopersltd/compute-horde-job:v0-latest"

    def docker_run_options_preset(self) -> str:
        return 'nvidia_all'

    def docker_run_cmd(self) -> list[str]:
        return [
            "--runtime",
            "600",
            "--restore-disable",
            "--attack-mode",
            "3",
            "--workload-profile",
            "3",
            "--optimized-kernel-enable",
            "--hash-type",
            self.hashcat_hash_type,
            "--hex-salt",
            "-1",
            "?l?d?u",
            "--outfile-format",
            "2",
            "--quiet",
            self.hash_job.payload,
            "?1" * self.password_length,
        ]

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

    def job_description(self):
        return (f'Hashcat {self.algorithm.value}, password length = {self.password_length}, '
                f'timeout={self.timeout_seconds()}s')
