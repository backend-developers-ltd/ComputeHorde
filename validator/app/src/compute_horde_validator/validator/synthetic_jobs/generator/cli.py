import base64
import io
import random
import string
import zipfile

from compute_horde.mv_protocol.miner_requests import V0JobFinishedRequest

from compute_horde_validator.validator.synthetic_jobs.generator.base import (
    AbstractSyntheticJobGenerator,
)


class CLIJobGenerator(AbstractSyntheticJobGenerator):

    _timeout = None
    _base_docker_image_name = None
    _docker_image_name = None
    _docker_run_options = None
    _docker_run_cmd = None

    @classmethod
    def set_parameters(
        cls,
        timeout: int,
        base_docker_image_name: str,
        docker_image_name: str,
        docker_run_options: list[str],
        docker_run_cmd: list[str],
    ):
        cls._timeout = timeout
        cls._base_docker_image_name = base_docker_image_name
        cls._docker_image_name = docker_image_name
        cls._docker_run_options = docker_run_options
        cls._docker_run_cmd = docker_run_cmd

    def timeout_seconds(self) -> int:
        if self._timeout is None:
            raise RuntimeError('Call set_parameters() before delegating job execution')
        return self._timeout

    def base_docker_image_name(self) -> str:
        if self._base_docker_image_name is None:
            raise RuntimeError('Call set_parameters() before delegating job execution')
        return self._base_docker_image_name

    def docker_image_name(self) -> str:
        if self._docker_image_name is None:
            raise RuntimeError('Call set_parameters() before delegating job execution')
        return self._docker_image_name

    def docker_run_options(self) -> list[str]:
        if self._docker_run_options is None:
            raise RuntimeError('Call set_parameters() before delegating job execution')
        return self._docker_run_options

    def docker_run_cmd(self) -> list[str]:
        if self._docker_run_cmd is None:
            raise RuntimeError('Call set_parameters() before delegating job execution')
        return self._docker_run_cmd

    def volume_contents(self) -> str:
        in_memory_output = io.BytesIO()
        zipf = zipfile.ZipFile(in_memory_output, 'w')
        zipf.writestr('payload.txt', 'nothing')
        zipf.close()
        in_memory_output.seek(0)
        zip_contents = in_memory_output.read()
        return base64.b64encode(zip_contents).decode()

    def verify(self, msg: V0JobFinishedRequest, time_took: float) -> tuple[bool, str, float]:
        return True, '', 1
