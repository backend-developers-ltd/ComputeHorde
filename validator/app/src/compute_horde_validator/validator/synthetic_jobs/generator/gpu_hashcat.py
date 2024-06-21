import time

import bittensor
from asgiref.sync import sync_to_async
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
        if settings.DEBUG_OVERRIDE_WEIGHTS_VERSION is not None:
            return settings.DEBUG_OVERRIDE_WEIGHTS_VERSION

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
    def __init__(self):
        # set synthetic_jobs based on subnet weights_version
        self.weights_version = weights_version_holder.get()
        self.hash_job = None
        self.expected_answer = None

    async def ainit(self):
        """Allow to initialize generator in asyncio and non blocking"""
        self.hash_job, self.expected_answer = await self._get_hash_job()

    @sync_to_async(thread_sensitive=False)
    def _get_hash_job(self):
        if self.weights_version == 0:
            algorithm = Algorithm.get_random_algorithm()
            hash_job = V0SyntheticJob.generate(
                algorithm, HASHJOB_PARAMS[self.weights_version][algorithm]
            )
        elif self.weights_version == 1:
            algorithms = Algorithm.get_all_algorithms()
            params = [HASHJOB_PARAMS[self.weights_version][algorithm] for algorithm in algorithms]
            hash_job = V1SyntheticJob.generate(algorithms, params)
        else:
            raise RuntimeError(f"No SyntheticJob for weights_version: {self.weights_version}")

        # precompute anwer when already in thread
        return hash_job, hash_job.answer

    def timeout_seconds(self) -> int:
        return 240

    def base_docker_image_name(self) -> str:
        return "ubuntu"

    def docker_image_name(self) -> str:
        return "ubuntu"

    def docker_run_options_preset(self) -> str:
        return "nvidia_all"

    def docker_run_cmd(self) -> list[str]:
        return [
            "bash",
            "-c",
            r"date -u +%s"
            " && "
            "cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w 16 | head -n 1"
            " && "
            "nvidia-smi --query-gpu=name,memory.total,compute_cap,driver_version,vbios_version,inforom.img,index,count,pci.bus_id,serial,uuid --format=csv"
        ]

    def raw_script(self) -> str | None:
        return None

    @sync_to_async(thread_sensitive=False)
    def volume_contents(self) -> str:
        return single_file_zip("empty.txt", "empty")

    def score(self, time_took: float) -> float:
        if self.weights_version == 0:
            return MAX_SCORE * (1 - (time_took / (2 * self.timeout_seconds())))
        elif self.weights_version == 1:
            return 1 / time_took
        else:
            raise RuntimeError(f"No score function for weights_version: {self.weights_version}")

    def verify(self, msg: V0JobFinishedRequest, time_took: float) -> tuple[bool, str, float]:
        return True, "", 0.0
        if str(msg.docker_process_stdout).strip() != str(self.expected_answer):
            return (
                False,
                f"result does not match expected answer: {self.expected_answer}, msg: {msg.model_dump_json()}",
                0,
            )

        return True, "", self.score(time_took)

    def job_description(self) -> str:
        return f"GPU Profile {self.hash_job}"
