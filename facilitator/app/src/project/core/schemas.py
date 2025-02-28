from decimal import Decimal
from typing import Annotated, Literal

from compute_horde.base.volume import (
    HuggingfaceVolume,
    InlineVolume,
    SingleFileVolume,
    ZipUrlVolume,
)
from pydantic import BaseModel, Extra, Field, field_validator

MuliVolumeAllowedVolume = Annotated[
    InlineVolume | ZipUrlVolume | SingleFileVolume | HuggingfaceVolume, Field(discriminator="volume_type")
]


class MinerResponse(BaseModel, extra=Extra.allow):
    job_uuid: str
    message_type: str | None
    docker_process_stderr: str
    docker_process_stdout: str
    artifacts: dict[str, str] | None = None


class JobStatusMetadata(BaseModel, extra=Extra.allow):
    comment: str
    miner_response: MinerResponse | None = None


class JobStatusUpdate(BaseModel, extra=Extra.forbid):
    """
    Message sent from validator to this app in response to NewJobRequest.
    """

    message_type: Literal["V0JobStatusUpdate"] = Field(default="V0JobStatusUpdate")
    uuid: str
    status: Literal["failed", "rejected", "accepted", "completed"]
    metadata: JobStatusMetadata | None = None


class ForceDisconnect(BaseModel, extra=Extra.forbid):
    """Message sent when validator is no longer valid and should be disconnected"""

    type: Literal["validator.disconnect"] = Field("validator.disconnect")


class CpuSpec(BaseModel, extra=Extra.forbid):
    model: str | None = None
    count: int
    frequency: Decimal | None = None
    clocks: list[float] | None = None


class GpuDetails(BaseModel, extra=Extra.forbid):
    name: str
    capacity: int | float | None = Field(default=None, description="in MB")
    cuda: str | None = None
    driver: str | None = None
    graphics_speed: int | None = Field(default=None, description="in MHz")
    memory_speed: int | None = Field(default=None, description="in MHz")
    power_limit: float | None = Field(default=None, description="in W")
    uuid: str | None = None
    serial: str | None = None

    @field_validator("power_limit", mode="before")
    @classmethod
    def parse_age(cls, v):
        try:
            return float(v)
        except Exception:
            return None


class GpuSpec(BaseModel, extra=Extra.forbid):
    capacity: int | float | None = None
    count: int | None = None
    details: list[GpuDetails] = []
    graphics_speed: int | None = Field(default=None, description="in MHz")
    memory_speed: int | None = Field(default=None, description="in MHz")


class HardDiskSpec(BaseModel, extra=Extra.forbid):
    total: int | float | None = Field(default=None, description="in kiB")
    free: int | float | None = Field(default=None, description="in kiB")
    used: int | float | None = Field(default=None, description="in kiB")
    read_speed: Decimal | None = None
    write_speed: Decimal | None = None

    @field_validator("*", mode="before")
    @classmethod
    def empty_str_to_none(cls, v):
        if v == "":
            return None
        return v

    def get_total_gb(self) -> float | None:
        if self.total is None:
            return None
        return self.total / 1024 / 1024


class RamSpec(BaseModel, extra=Extra.forbid):
    total: int | float | None = Field(default=None, description="in kiB")
    free: int | float | None = Field(default=None, description="in kiB")
    available: int | float | None = Field(default=None, description="in kiB")
    used: int | float | None = Field(default=None, description="in kiB")
    read_speed: Decimal | None = None
    write_speed: Decimal | None = None
    swap_free: int | None = Field(default=None, description="in kiB")
    swap_total: int | None = Field(default=None, description="in kiB")
    swap_used: int | None = Field(default=None, description="in kiB")

    @field_validator("*", mode="before")
    @classmethod
    def empty_str_to_none(cls, v):
        if v == "":
            return None
        return v

    def get_total_gb(self) -> float | None:
        if self.total is None:
            return None
        return self.total / 1024 / 1024


class HardwareSpec(BaseModel, extra=Extra.allow):
    cpu: CpuSpec
    gpu: GpuSpec | None = None
    hard_disk: HardDiskSpec
    has_docker: bool | None = None
    ram: RamSpec
    virtualization: str | None = None
    os: str | None = None
