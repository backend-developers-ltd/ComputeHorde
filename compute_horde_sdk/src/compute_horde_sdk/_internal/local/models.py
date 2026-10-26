import math
import re
import subprocess
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path, PurePosixPath
from types import MappingProxyType

import pydantic
from docker.types import Mount


class LocalBindVolume(pydantic.BaseModel):
    """An existing host file or directory, mounted directly without transfers."""

    model_config = pydantic.ConfigDict(frozen=True, extra="forbid")

    source: Path
    destination: str
    read_only: bool = True

    @pydantic.field_validator("source")
    @classmethod
    def validate_source(cls, source: Path) -> Path:
        source = source.absolute()
        if not (source.is_file() or source.is_dir()):
            raise ValueError("Bind source must be an existing regular file or directory")
        return source

    @pydantic.field_validator("destination")
    @classmethod
    def validate_destination(cls, destination: str) -> str:
        if not PurePosixPath(destination).is_absolute() or "\x00" in destination:
            raise ValueError("Bind destination must be an absolute container path")
        return destination

    def to_docker_mount(self) -> Mount:
        return Mount(target=self.destination, source=str(self.source), type="bind", read_only=self.read_only)


def memory_bytes(value: int | str) -> int:
    if isinstance(value, int):
        result = value
    else:
        match = re.fullmatch(r"(\d+)([bkmg])?", value.lower())
        if match is None:
            raise ValueError("memory must be bytes or a Docker size such as '512m' or '4g'")
        result = int(match[1]) * 1024 ** {None: 0, "b": 0, "k": 1, "m": 2, "g": 3}[match[2]]
    if result <= 0:
        raise ValueError("memory must be positive")
    return result


@dataclass(frozen=True, kw_only=True)
class ComputeHordeJobSpec:
    """Version two specification; supported exclusively by LocalDockerClient."""

    docker_image: str
    args: Sequence[str] = ()
    env: Mapping[str, str] = field(default_factory=dict)
    volumes: Sequence[LocalBindVolume] = ()
    cpus: float | None = None
    memory: int | str | None = None
    user: str | None = None
    platform: str | None = None
    container_name: str | None = None
    execution_time_limit_sec: float | None = None
    text: bool = False
    encoding: str = "utf-8"
    errors: str = "strict"

    def __post_init__(self) -> None:
        if not self.docker_image or self.docker_image.startswith("-"):
            raise ValueError("docker_image must be an image reference")
        if isinstance(self.args, str) or any(not isinstance(arg, str) or "\x00" in arg for arg in self.args):
            raise ValueError("args must be a sequence of individual strings")
        if any(not isinstance(v, LocalBindVolume) for v in self.volumes):
            raise TypeError("v2 volumes must be LocalBindVolume objects")
        if any("=" in key or "\x00" in key + value for key, value in self.env.items()):
            raise ValueError("Invalid environment variable")
        if self.cpus is not None and (not math.isfinite(self.cpus) or self.cpus <= 0):
            raise ValueError("cpus must be finite and positive")
        if self.memory is not None:
            memory_bytes(self.memory)
        if self.execution_time_limit_sec is not None and (
            not math.isfinite(self.execution_time_limit_sec) or self.execution_time_limit_sec <= 0
        ):
            raise ValueError("execution_time_limit_sec must be finite and positive")
        if self.container_name is not None and not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_.-]+", self.container_name):
            raise ValueError("Invalid Docker container name")
        object.__setattr__(self, "args", tuple(self.args))
        object.__setattr__(self, "env", MappingProxyType(dict(self.env)))
        object.__setattr__(self, "volumes", tuple(self.volumes))


class LocalJobStatus(str, Enum):
    PREPARED = "prepared"
    STARTING = "starting"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    LAUNCH_FAILED = "launch_failed"
    CANCELLED = "cancelled"
    TIMED_OUT = "timed_out"


@dataclass(frozen=True)
class LocalJobResult:
    args: tuple[str, ...]
    returncode: int | None
    stdout: bytes | str | None
    stderr: bytes | str
    status: LocalJobStatus

    def to_completed_process(self) -> subprocess.CompletedProcess[bytes | str]:
        if self.returncode is None:
            raise ValueError("No container exit status is available")
        process: subprocess.CompletedProcess[bytes | str] = subprocess.CompletedProcess(
            self.args, self.returncode, self.stdout, self.stderr
        )
        return process


class LocalDockerError(Exception):
    """Local execution or Docker failure, with any available partial result."""

    def __init__(self, message: str, result: LocalJobResult | None = None):
        super().__init__(message)
        self.result = result


class LocalJobLaunchError(LocalDockerError):
    pass


class LocalJobExecutionError(LocalDockerError):
    pass


class LocalJobCancelledError(LocalDockerError):
    pass


class LocalJobTimeoutError(LocalDockerError, TimeoutError):
    pass


class LocalCleanupError(LocalDockerError):
    """Removal was not confirmed; retry stop while keeping the event loop alive."""


class LocalCleanupTimeoutError(LocalCleanupError, TimeoutError):
    pass
