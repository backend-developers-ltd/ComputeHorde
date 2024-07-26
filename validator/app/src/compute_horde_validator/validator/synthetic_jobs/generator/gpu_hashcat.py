from asgiref.sync import sync_to_async
from compute_horde.mv_protocol.miner_requests import V0JobFinishedRequest

from compute_horde_validator.validator.dynamic_config import aget_weights_version
from compute_horde_validator.validator.synthetic_jobs.generator.base import (
    BaseSyntheticJobGenerator,
)
from compute_horde_validator.validator.synthetic_jobs.synthetic_job import (
    HASHJOB_PARAMS,
    Algorithm,
)
from compute_horde_validator.validator.synthetic_jobs.v0_synthetic_job import V0SyntheticJob
from compute_horde_validator.validator.synthetic_jobs.v1_synthetic_job import V1SyntheticJob
from compute_horde_validator.validator.utils import single_file_zip

MAX_SCORE = 2


class GPUHashcatSyntheticJobGenerator(BaseSyntheticJobGenerator):
    def __init__(self):
        # set synthetic_jobs based on subnet weights_version
        self.weights_version = None
        self.hash_job = None
        self.expected_answer = None

    async def ainit(self, miner_hotkey: str | None = None):
        """Allow to initialize generator in asyncio and non blocking"""
        self.weights_version = await aget_weights_version()
        self.hash_job, self.expected_answer = await self._get_hash_job(miner_hotkey)

    @sync_to_async(thread_sensitive=False)
    def _get_hash_job(self, miner_hotkey: str | None = None):
        if self.weights_version == 0:
            algorithm = Algorithm.get_random_algorithm()
            hash_job = V0SyntheticJob.generate(
                algorithm, HASHJOB_PARAMS[self.weights_version][algorithm]
            )
        elif self.weights_version in [1, 2]:
            algorithms = Algorithm.get_all_algorithms()
            params = [HASHJOB_PARAMS[self.weights_version][algorithm] for algorithm in algorithms]
            hash_job = V1SyntheticJob.generate(algorithms, params)
        elif self.weights_version == 3:
            algorithms = Algorithm.get_all_algorithms()
            params = [HASHJOB_PARAMS[self.weights_version][algorithm] for algorithm in algorithms]
            # prefix the salt with the first N letters to of the hotkey
            hash_job = V1SyntheticJob.generate(algorithms, params, salt_prefix=miner_hotkey)
        else:
            raise RuntimeError(f"No SyntheticJob for weights_version: {self.weights_version}")

        # precompute answer when already in thread
        return hash_job, hash_job.answer

    def timeout_seconds(self) -> int:
        return self.hash_job.timeout_seconds

    def base_docker_image_name(self) -> str:
        if self.weights_version == 0:
            return "backenddevelopersltd/compute-horde-job:v0-latest"
        elif self.weights_version in [1, 2, 3]:
            return "backenddevelopersltd/compute-horde-job:v1-latest"
        else:
            raise RuntimeError(f"No base_docker_image for weights_version: {self.weights_version}")

    def docker_image_name(self) -> str:
        if self.weights_version in [0, 1, 2, 3]:
            return self.base_docker_image_name()
        else:
            raise RuntimeError(f"No docker_image for weights_version: {self.weights_version}")

    def docker_run_options_preset(self) -> str:
        return "nvidia_all"

    def docker_run_cmd(self) -> list[str]:
        return self.hash_job.docker_run_cmd()

    def raw_script(self) -> str | None:
        return self.hash_job.raw_script()

    @sync_to_async(thread_sensitive=False)
    def volume_contents(self) -> str:
        return single_file_zip("payload.txt", self.hash_job.payload)

    def score(self, time_took: float) -> float:
        if self.weights_version == 0:
            return MAX_SCORE * (1 - (time_took / (2 * self.timeout_seconds())))
        elif self.weights_version in [1, 2, 3]:
            return 1 / time_took
        else:
            raise RuntimeError(f"No score function for weights_version: {self.weights_version}")

    def verify(self, msg: V0JobFinishedRequest, time_took: float) -> tuple[bool, str, float]:
        if str(msg.docker_process_stdout).strip() != str(self.expected_answer):
            return (
                False,
                f"result does not match expected answer: {self.expected_answer}, msg: {msg.model_dump_json()}",
                0,
            )

        return True, "", self.score(time_took)

    def job_description(self) -> str:
        return f"Hashcat {self.hash_job}"
