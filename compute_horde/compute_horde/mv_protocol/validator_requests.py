import enum
from typing import Mapping

import pydantic

from ..base_requests import BaseRequest, JobMixin


class RequestType(enum.Enum):
    V0AuthenticateRequest = 'V0AuthenticateRequest'
    V0InitialJobRequest = 'V0InitialJobRequest'
    V0JobRequest = 'V0JobRequest'
    GenericError = 'GenericError'


class BaseValidatorRequest(BaseRequest):
    message_type: RequestType


class VolumeType(enum.Enum):
    inline = 'inline'


class AuthenticationPayload(pydantic.BaseModel):
    validator_hotkey: str
    miner_hotkey: str
    timestamp: int

    def blob_for_signing(self):
        return self.json(sort_keys=True)


class V0AuthenticateRequest(BaseValidatorRequest):
    message_type: RequestType = RequestType.V0AuthenticateRequest
    payload: AuthenticationPayload
    signature: str

    def blob_for_signing(self):
        return self.payload.blob_for_signing()


class V0InitialJobRequest(BaseValidatorRequest, JobMixin):
    message_type: RequestType = RequestType.V0InitialJobRequest
    base_docker_image_name: str | None
    timeout_seconds: int | None
    volume_type: VolumeType


class Volume(pydantic.BaseModel):
    volume_type: VolumeType
    contents: str  # TODO: this is only valid for volume_type = inline, some polymorphism like with BaseRequest is
    # required here


class OutputUploadType(enum.Enum):
    zip_and_http_post = 'zip_and_http_post'


class OutputUpload(pydantic.BaseModel):
    output_upload_type: OutputUploadType
    # TODO: the following are only valid for output_upload_type = zip_and_http_post, some polymorphism like with
    #  BaseRequest is required here
    post_url: str
    post_form_fields: Mapping[str, str]


class V0JobRequest(BaseValidatorRequest, JobMixin):
    message_type: RequestType = RequestType.V0JobRequest
    docker_image_name: str
    docker_run_options_preset: str
    docker_run_cmd: list[str]
    volume: Volume
    output_upload: OutputUpload | None


class GenericError(BaseValidatorRequest):
    message_type: RequestType = RequestType.GenericError
    details: str | None = None
