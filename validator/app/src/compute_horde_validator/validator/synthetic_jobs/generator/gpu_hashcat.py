import math

import bittensor
from compute_horde.mv_protocol.miner_requests import V0JobFinishedRequest
from django.conf import settings

from compute_horde_validator.validator.synthetic_jobs.generator.base import (
    AbstractSyntheticJobGenerator,
)
from compute_horde_validator.validator.synthetic_jobs.synthetic_job import (
    HASHJOB_PARAMS,
    Algorithm,
)
from compute_horde_validator.validator.synthetic_jobs.v0_synthetic_job import V0SyntheticJob
from compute_horde_validator.validator.synthetic_jobs.v1_synthetic_job import V1SyntheticJob
from compute_horde_validator.validator.utils import single_file_zip

MAX_SCORE = 2


def get_subnet_weights_version():
    try:
        subtensor = bittensor.subtensor(network=settings.BITTENSOR_NETWORK)
        hyperparameters = subtensor.get_subnet_hyperparameters(netuid=settings.BITTENSOR_NETUID)
        return 1
        if hyperparameters is None:
            raise RuntimeError("Network hyperparameters are None")
        return hyperparameters.weights_version
    except Exception as e:
        raise RuntimeError("Failed to get subnet weight version") from e


class GPUHashcatSyntheticJobGenerator(AbstractSyntheticJobGenerator):
    def __init__(self):
        # set synthetic_jobs based on subnet weights_version
        self.weights_version = get_subnet_weights_version()
        if self.weights_version == 0:
            algorithm = Algorithm.get_random_algorithm()
            self.hash_job = V0SyntheticJob.generate(
                algorithm, HASHJOB_PARAMS[self.weights_version][algorithm]
            )
        elif self.weights_version == 1:
            algorithms = Algorithm.get_all_algorithms()
            params = [HASHJOB_PARAMS[self.weights_version][algorithm] for algorithm in algorithms]
            self.hash_job = V1SyntheticJob.generate(algorithms, params)
        else:
            raise RuntimeError(f"No SyntheticJob for weights_version: {self.weights_version}")

        self.expected_answer = self.hash_job.answer

    def timeout_seconds(self) -> int:
        return self.hash_job.timeout_seconds

    def base_docker_image_name(self) -> str:
        return f"backenddevelopersltd/compute-horde-job:v{self.weights_version}-latest"

    def docker_image_name(self) -> str:
        return f"backenddevelopersltd/compute-horde-job:v{self.weights_version}-latest"

    def docker_run_options_preset(self) -> str:
        return "nvidia_all"

    def docker_run_cmd(self) -> list[str]:
        return self.hash_job.docker_run_cmd()

    def raw_script(self) -> str | None:
        return self.hash_job.raw_script()

    def volume_contents(self) -> str:
        return single_file_zip("payload.txt", self.hash_job.payload)

    def score(self, time_took: float) -> float:
        if self.weights_version == 0:
            return MAX_SCORE * (1 - (time_took / (2 * self.timeout_seconds())))
        elif self.weights_version == 1:
            return math.e ** ((1 - (time_took / self.timeout_seconds())) * 1.5 )
        else:
            raise RuntimeError(f"No score function for weights_version: {self.weights_version}")

    def verify(self, msg: V0JobFinishedRequest, time_took: float) -> tuple[bool, str, float]:
        if str(msg.docker_process_stdout).strip() != str(self.expected_answer):
            return (
                False,
                f"result does not match expected answer: expected answer={self.expected_answer} msg={msg.json()}",
                0,
            )

        return True, "", self.score(time_took)

    def job_description(self) -> str:
        return f"Hashcat {self.hash_job}"
