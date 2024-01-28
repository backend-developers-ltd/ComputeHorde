import base64
import io
import random
import string
import zipfile

from compute_horde.mv_protocol.miner_requests import V0JobFinishedRequest
from compute_horde_validator.validator.synthetic_jobs.generator.base import AbstractChallengeGenerator


class EchoChallengeGenerator(AbstractChallengeGenerator):
    def __init__(self):
        self.payload = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(32))

    def timeout_seconds(self) -> int:
        return 3

    def base_docker_image_name(self) -> str:
        return "alpine"

    def docker_image_name(self) -> str:
        return "ghcr.io/reef-technologies/computehorde/echo:latest"

    def volume_contents(self) -> str:
        in_memory_output = io.BytesIO()
        zipf = zipfile.ZipFile(in_memory_output, 'w')
        zipf.writestr('payload.txt', self.payload)
        zipf.close()
        in_memory_output.seek(0)
        zip_contents = in_memory_output.read()
        return base64.b64encode(zip_contents).decode()

    def verify(self, msg: V0JobFinishedRequest) -> tuple[bool, str]:
        if msg.docker_process_stdout == self.payload:
            return True, ''
        return False, f'result does not match payload: payload={self.payload} msg={msg.json()}'
