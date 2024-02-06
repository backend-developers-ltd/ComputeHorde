import enum

import pydantic

from ..base_requests import BaseRequest, JobMixin


class RequestType(enum.Enum):
    V0PrepareJobRequest = 'V0PrepareJobRequest'
    V0RunJobRequest = 'V0RunJobRequest'
    GenericError = 'GenericError'


class BaseMinerRequest(BaseRequest):
    message_type: RequestType


class VolumeType(enum.Enum):
    inline = 'inline'


class V0InitialJobRequest(BaseMinerRequest, JobMixin):
    message_type: RequestType = RequestType.V0PrepareJobRequest
    base_docker_image_name: str | None
    timeout_seconds: int | None
    volume_type: VolumeType


class Volume(pydantic.BaseModel):
    volume_type: VolumeType
    contents: str  # TODO: this is only valid for volume_type = inline, some polymorphism like with BaseRequest is
    # required here


class V0JobRequest(BaseMinerRequest, JobMixin):
    message_type: RequestType = RequestType.V0RunJobRequest
    docker_image_name: str
    docker_run_options_preset: str
    docker_run_cmd: list[str]
    volume: Volume


class GenericError(BaseMinerRequest):
    message_type: RequestType = RequestType.GenericError
    details: str | None = None
