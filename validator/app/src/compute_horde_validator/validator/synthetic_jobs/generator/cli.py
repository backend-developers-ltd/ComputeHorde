from compute_horde.mv_protocol.miner_requests import V0JobFinishedRequest

from compute_horde_validator.validator.synthetic_jobs.generator.base import (
    AbstractSyntheticJobGenerator,
)
from compute_horde_validator.validator.utils import single_file_zip


class CLIJobGenerator(AbstractSyntheticJobGenerator):

    _timeout = None
    _base_docker_image_name = None
    _docker_image_name = None
    _docker_run_options_preset = None
    _docker_run_cmd = None

    @classmethod
    def set_parameters(
        cls,
        timeout: int,
        base_docker_image_name: str,
        docker_image_name: str,
        docker_run_options_preset: str,
        docker_run_cmd: list[str],
    ):
        cls._timeout = timeout
        cls._base_docker_image_name = base_docker_image_name
        cls._docker_image_name = docker_image_name
        cls._docker_run_options_preset = docker_run_options_preset
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

    def docker_run_options_preset(self) -> str:
        if self._docker_run_options_preset is None:
            raise RuntimeError('Call set_parameters() before delegating job execution')
        return self._docker_run_options_preset

    def docker_run_cmd(self) -> list[str]:
        if self._docker_run_cmd is None:
            raise RuntimeError('Call set_parameters() before delegating job execution')
        return self._docker_run_cmd

    def volume_contents(self) -> str:
        return single_file_zip('payload.txt', 'nothing')

    def verify(self, msg: V0JobFinishedRequest, time_took: float) -> tuple[bool, str, float]:
        return True, '', 1

    def job_description(self):
        return 'custom'
