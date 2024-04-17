import enum
from typing import Mapping, Any

import pydantic
from pydantic import Field, root_validator

from ..base_requests import BaseRequest, JobMixin


class RequestType(enum.Enum):
    V0PrepareJobRequest = 'V0PrepareJobRequest'
    V0RunJobRequest = 'V0RunJobRequest'
    GenericError = 'GenericError'


class BaseMinerRequest(BaseRequest):
    message_type: RequestType


class VolumeType(enum.Enum):
    inline = 'inline'
    zip_url = 'zip_url'


class V0InitialJobRequest(BaseMinerRequest, JobMixin):
    message_type: RequestType = RequestType.V0PrepareJobRequest
    base_docker_image_name: str | None
    timeout_seconds: int | None
    volume_type: VolumeType


class Volume(pydantic.BaseModel):
    volume_type: VolumeType
    contents: str  # TODO: this is only valid for volume_type = inline, some polymorphism like with BaseRequest is
    # required here


class OutputUploadType(enum.Enum):
    zip_and_http_post = 'zip_and_http_post'
    zip_and_http_put = 'zip_and_http_put'


class OutputUpload(pydantic.BaseModel):
    output_upload_type: OutputUploadType
    # TODO: the following are only valid for output_upload_type = zip_and_http_post, some polymorphism like with
    #  BaseRequest is required here
    url: str
    form_fields: Mapping[str, str] | None = Field(default=None)


class V0JobRequest(BaseMinerRequest, JobMixin):
    message_type: RequestType = RequestType.V0RunJobRequest
    docker_image_name: str | None = None
    raw_script: str | None = None
    docker_run_options_preset: str
    docker_run_cmd: list[str]
    volume: Volume
    output_upload: OutputUpload | None

    @root_validator()
    def validate(cls, values: dict[str, Any]) -> dict[str, Any]:
        if not (bool(values.get("docker_image_name")) or bool(values.get("raw_script"))):
            raise ValueError("Expected at least one of `docker_image_name` or `raw_script`")
        return values


class GenericError(BaseMinerRequest):
    message_type: RequestType = RequestType.GenericError
    details: str | None = None
