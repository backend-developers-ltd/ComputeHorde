import random
import string

from asgiref.sync import sync_to_async
from compute_horde.mv_protocol.miner_requests import V0JobFinishedRequest

from compute_horde_validator.validator.synthetic_jobs.generator.base import (
    AbstractSyntheticJobGenerator,
)
from compute_horde_validator.validator.utils import single_file_zip


class EchoSyntheticJobGenerator(AbstractSyntheticJobGenerator):
    def __init__(self):
        self.payload = "".join(
            random.choice(string.ascii_uppercase + string.digits) for _ in range(32)
        )

    def timeout_seconds(self) -> int:
        return 3

    def base_docker_image_name(self) -> str:
        return "alpine"

    def docker_image_name(self) -> str:
        return "ghcr.io/reef-technologies/computehorde/echo:latest"

    def docker_run_options_preset(self) -> str:
        return "none"

    def docker_run_cmd(self) -> list[str]:
        return []

    @sync_to_async(thread_sensitive=False)
    def volume_contents(self) -> str:
        return single_file_zip("payload.txt", self.payload)

    def verify(self, msg: V0JobFinishedRequest, time_took: float) -> tuple[bool, str, float]:
        if msg.docker_process_stdout == self.payload:
            return True, "", 1
        return False, f"result does not match payload: payload={self.payload} msg={msg.model_dump_json()}", 0

    def job_description(self):
        return "echo"
