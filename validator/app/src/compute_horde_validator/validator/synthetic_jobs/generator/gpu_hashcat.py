import time

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


class WeightVersionHolder:

    def __init__(self):
        self._time_set = 0
        self.value = None

    def get(self):
        if settings.DEBUG_WEIGHTS_VERSION is not None:
            return settings.DEBUG_WEIGHTS_VERSION
        if time.time() - self._time_set > 300:
            subtensor = bittensor.subtensor(network=settings.BITTENSOR_NETWORK)
            hyperparameters = subtensor.get_subnet_hyperparameters(netuid=settings.BITTENSOR_NETUID)
            if hyperparameters is None:
                raise RuntimeError("Network hyperparameters are None")
            self.value = hyperparameters.weights_version
            self._time_set = time.time()
        return self.value


weights_version_holder = WeightVersionHolder()


class GPUHashcatSyntheticJobGenerator(AbstractSyntheticJobGenerator):
    def __init__(self, *,
        weights_version: int | None = None,
        answer: str | None = None,
        contents: str | None = None,
    ):
        self._weights_version = weights_version
        self._answer = answer
        self._contents = contents

        if self._weights_version is None:
            # set synthetic_jobs based on subnet weights_version
            self._weights_version = weights_version_holder.get()
            self._pregenerated = False
        else:
            self._pregenerated = True

        if self._weights_version == 0:
            algorithm = Algorithm.get_random_algorithm()
            create_method = V0SyntheticJob.pregenerated if self._pregenerated else V0SyntheticJob.generate
            self.hash_job = create_method(
                algorithm, HASHJOB_PARAMS[self._weights_version][algorithm]
            )
        elif self._weights_version in (1, 2):
            algorithms = Algorithm.get_all_algorithms()
            params = [HASHJOB_PARAMS[self._weights_version][algorithm] for algorithm in algorithms]
            create_method = V1SyntheticJob.pregenerated if self._pregenerated else V1SyntheticJob.generate
            self.hash_job = V1SyntheticJob.generate(algorithms, params)
        else:
            raise RuntimeError(f"No SyntheticJob for weights_version: {self._weights_version}")

    def timeout_seconds(self) -> int:
        return self.hash_job.timeout_seconds

    def base_docker_image_name(self) -> str:
        return f"backenddevelopersltd/compute-horde-job:v{self._weights_version}-latest"

    def docker_image_name(self) -> str:
        return f"backenddevelopersltd/compute-horde-job:v{self._weights_version}-latest"

    def docker_run_options_preset(self) -> str:
        return "nvidia_all"

    def docker_run_cmd(self) -> list[str]:
        return self.hash_job.docker_run_cmd()

    def raw_script(self) -> str | None:
        return self.hash_job.raw_script()

    def weights_version(self) -> int:
        return self._weights_version

    def expected_answer(self) -> str:
        return self._answer if self._pregenerated else self.hash_job.answer

    def volume_contents(self) -> str:
        if self._pregenerated:
            return self._contents
        return single_file_zip("payload.txt", self.hash_job.payload)

    def score(self, time_took: float) -> float:
        if self._weights_version == 0:
            return MAX_SCORE * (1 - (time_took / (2 * self.timeout_seconds())))
        elif self._weights_version in (1, 2):
            return 1 / time_took
        else:
            raise RuntimeError(f"No score function for weights_version: {self._weights_version}")

    def verify(self, msg: V0JobFinishedRequest, time_took: float) -> tuple[bool, str, float]:
        if str(msg.docker_process_stdout).strip() != str(self.expected_answer()):
            return (
                False,
                f"result does not match expected answer: {self.expected_answer()}, msg: {msg.json()}",
                0,
            )

        return True, "", self.score(time_took)

    def job_description(self) -> str:
        return f"Hashcat {self.hash_job}"
