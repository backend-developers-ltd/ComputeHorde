import enum

import pydantic

from ..base_requests import BaseRequest, JobMixin


class RequestType(enum.Enum):
    V0InitialJobRequest = 'V0InitialJobRequest'
    V0JobRequest = 'V0JobRequest'
    GenericError = 'GenericError'


class BaseValidatorRequest(BaseRequest):
    message_type: RequestType

    @classmethod
    def type_to_model(cls, type_: RequestType) -> type['BaseValidatorRequest']:
        return {
            RequestType.V0InitialJobRequest: V0InitialJobRequest,
            RequestType.V0JobRequest: V0JobRequest,
        }[type_]


class VolumeType(enum.Enum):
    inline = 'inline'


class V0InitialJobRequest(BaseValidatorRequest, JobMixin):
    message_type: RequestType = RequestType.V0InitialJobRequest
    base_docker_image_name: str | None
    timeout_seconds: int | None
    volume_type: VolumeType


class Volume(pydantic.BaseModel):
    volume_type: VolumeType
    contents: str  # TODO: this is only valid for volume_type = inline, some polymorphism like with BaseRequest is
    # required here


class V0JobRequest(BaseValidatorRequest, JobMixin):
    message_type: RequestType = RequestType.V0JobRequest
    docker_image_name: str
    volume: Volume


class GenericError(BaseValidatorRequest):
    message_type: RequestType = RequestType.GenericError
    details: str | None = None
