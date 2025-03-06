from asgiref.sync import sync_to_async
from compute_horde.base.docker import DockerRunOptionsPreset
from compute_horde.protocol_messages import V0JobFinishedRequest
from compute_horde_core.volume import InlineVolume, Volume

from compute_horde_validator.validator.dynamic_config import aget_weights_version
from compute_horde_validator.validator.synthetic_jobs.generator.base import (
    BaseSyntheticJobGenerator,
)
from compute_horde_validator.validator.synthetic_jobs.synthetic_job import (
    HASHJOB_PARAMS,
    Algorithm,
    SyntheticJob,
)
from compute_horde_validator.validator.synthetic_jobs.v0_synthetic_job import V0SyntheticJob
from compute_horde_validator.validator.synthetic_jobs.v1_synthetic_job import V1SyntheticJob
from compute_horde_validator.validator.synthetic_jobs.v2_synthetic_job import V2SyntheticJob
from compute_horde_validator.validator.utils import single_file_zip

MAX_SCORE = 2


class JobNotInitialized(Exception): ...


class GPUHashcatSyntheticJobGenerator(BaseSyntheticJobGenerator):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # set synthetic_jobs based on subnet weights_version
        self.weights_version: int | None = None
        self._hash_job: SyntheticJob | None = None
        self.expected_answer: str | None = None
        self._miner_hotkey: str | None = None

    async def ainit(self, miner_hotkey: str):
        """Allow to initialize generator in asyncio and non blocking"""
        self._miner_hotkey = miner_hotkey
        self.weights_version = await aget_weights_version()
        self._hash_job, self.expected_answer = await self._get_hash_job()

    @property
    def hash_job(self) -> SyntheticJob:
        if self._hash_job is None:
            raise JobNotInitialized("ainit() must be called first")
        return self._hash_job

    @property
    def miner_hotkey(self) -> str:
        if self._miner_hotkey is None:
            raise JobNotInitialized("ainit() must be called first")
        return self._miner_hotkey

    @sync_to_async(thread_sensitive=False)
    def _get_hash_job(self) -> tuple[SyntheticJob, str]:
        hash_job: SyntheticJob

        if self.weights_version == 0:
            algorithm = Algorithm.get_random_algorithm()
            hash_job = V0SyntheticJob.generate(
                algorithm, HASHJOB_PARAMS[self.weights_version][algorithm]
            )
        elif self.weights_version in [1, 2, 3]:
            algorithms = Algorithm.get_all_algorithms()
            params = [HASHJOB_PARAMS[self.weights_version][algorithm] for algorithm in algorithms]
            hash_job = V1SyntheticJob.generate(algorithms, params)
        elif self.weights_version == 4:
            algorithms = Algorithm.get_all_algorithms()
            params = [HASHJOB_PARAMS[self.weights_version][algorithm] for algorithm in algorithms]
            hash_job = V2SyntheticJob.generate(algorithms, params, self.miner_hotkey)
        else:
            raise RuntimeError(f"No SyntheticJob for weights_version: {self.weights_version}")

        # precompute answer when already in thread
        return hash_job, hash_job.answer

    def timeout_seconds(self) -> int:
        return self.hash_job.timeout_seconds

    def base_docker_image_name(self) -> str:
        if self.weights_version == 0:
            return "backenddevelopersltd/compute-horde-job:v0-latest"
        elif self.weights_version in [1, 2, 3, 4]:
            return "backenddevelopersltd/compute-horde-job:v1-latest"
        else:
            raise RuntimeError(f"No base_docker_image for weights_version: {self.weights_version}")

    def docker_image_name(self) -> str:
        if self.weights_version in [0, 1, 2, 3, 4]:
            return self.base_docker_image_name()
        else:
            raise RuntimeError(f"No docker_image for weights_version: {self.weights_version}")

    def docker_run_options_preset(self) -> DockerRunOptionsPreset:
        return "nvidia_all"

    def docker_run_cmd(self) -> list[str]:
        return self.hash_job.docker_run_cmd()

    def raw_script(self) -> str | None:
        return self.hash_job.raw_script()

    @sync_to_async(thread_sensitive=False)
    def volume(self) -> Volume | None:
        return InlineVolume(contents=single_file_zip("payload.txt", self.hash_job.payload))

    def verify_time(self, time_took: float) -> bool | None:
        return time_took <= self.timeout_seconds()

    def verify_correctness(self, msg: V0JobFinishedRequest) -> tuple[bool, str]:
        if str(msg.docker_process_stdout).strip() != str(self.expected_answer):
            return (
                False,
                f"result does not match expected answer: {self.expected_answer}, msg: {msg.model_dump_json()}",
            )
        return True, ""

    def job_description(self) -> str:
        return f"Hashcat {self.hash_job}"

    def volume_in_initial_req(self) -> bool:
        if self.weights_version in [0, 1, 2, 3]:
            return False
        elif self.weights_version == 4:
            return True
        else:
            raise RuntimeError(f"No score function for weights_version: {self.weights_version}")
