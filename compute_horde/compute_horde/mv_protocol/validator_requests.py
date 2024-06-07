import datetime
import enum
import json
import re
from collections.abc import Mapping
from typing import Self
from urllib.parse import urlparse

import pydantic
from pydantic import model_validator, Field

from ..base_requests import BaseRequest, JobMixin
from ..utils import MachineSpecs

SAFE_DOMAIN_REGEX = re.compile(r".*")


class RequestType(enum.Enum):
    V0AuthenticateRequest = "V0AuthenticateRequest"
    V0InitialJobRequest = "V0InitialJobRequest"
    V0MachineSpecsRequest = "V0MachineSpecsRequest"
    V0JobRequest = "V0JobRequest"
    V0ReceiptRequest = "V0ReceiptRequest"
    GenericError = "GenericError"


class BaseValidatorRequest(BaseRequest):
    message_type: RequestType


class VolumeType(enum.Enum):
    inline = "inline"
    zip_url = "zip_url"


class AuthenticationPayload(pydantic.BaseModel):
    validator_hotkey: str
    miner_hotkey: str
    timestamp: int

    def blob_for_signing(self):
        # pydantic v2 does not support sort_keys anymore.
        # we are serializing into json twice here to use serialization of pydantic
        # instead of overriding json.JSONEncoder.default()
        return json.dumps(json.loads(self.model_dump_json()), sort_keys=True)


class V0AuthenticateRequest(BaseValidatorRequest):
    message_type: RequestType = RequestType.V0AuthenticateRequest
    payload: AuthenticationPayload
    signature: str

    def blob_for_signing(self):
        return self.payload.blob_for_signing()


class V0InitialJobRequest(BaseValidatorRequest, JobMixin):
    message_type: RequestType = RequestType.V0InitialJobRequest
    base_docker_image_name: str | None = None
    timeout_seconds: int | None = None
    volume_type: VolumeType


class Volume(pydantic.BaseModel):
    volume_type: VolumeType
    contents: str  # TODO: this is only valid for volume_type = inline, some polymorphism like with BaseRequest is
    # required here

    def is_safe(
        self,
    ) -> bool:
        if self.volume_type == VolumeType.zip_url:
            domain = urlparse(self.contents).netloc
            if SAFE_DOMAIN_REGEX.fullmatch(domain):
                return True
            return False
        else:
            return True


class OutputUploadType(enum.Enum):
    zip_and_http_post = "zip_and_http_post"
    zip_and_http_put = "zip_and_http_put"


class OutputUpload(pydantic.BaseModel):
    output_upload_type: OutputUploadType
    # TODO: the following are only valid for output_upload_type = zip_and_http_post, some polymorphism like with
    #  BaseRequest is required here
    url: str
    form_fields: Mapping[str, str] | None = None


class V0JobRequest(BaseValidatorRequest, JobMixin):
    message_type: RequestType = RequestType.V0JobRequest
    docker_image_name: str | None = None
    raw_script: str | None = None
    docker_run_options_preset: str
    docker_run_cmd: list[str]
    volume: Volume
    output_upload: OutputUpload | None = None

    @model_validator(mode="after")
    def validate_at_least_docker_image_or_raw_script(self) -> Self:
        if not (bool(self.docker_image_name) or bool(self.raw_script)):
            raise ValueError("Expected at least one of `docker_image_name` or `raw_script`")
        return self


class V0MachineSpecsRequest(BaseValidatorRequest, JobMixin):
    message_type: RequestType = RequestType.V0MachineSpecsRequest
    specs: MachineSpecs


class GenericError(BaseValidatorRequest):
    message_type: RequestType = RequestType.GenericError
    details: str | None = None


class ReceiptPayload(pydantic.BaseModel):
    job_uuid: str
    miner_hotkey: str
    validator_hotkey: str
    time_started: datetime.datetime
    time_took_us: int  # micro-seconds
    score_str: str

    def blob_for_signing(self):
        # pydantic v2 does not support sort_keys anymore.
        # we are serializing into json twice here to use serialization of pydantic
        # instead of overriding json.JSONEncoder.default()
        return json.dumps(json.loads(self.model_dump_json()), sort_keys=True)

    @property
    def time_took(self):
        return datetime.timedelta(microseconds=self.time_took_us)

    @property
    def score(self):
        return float(self.score_str)


class V0ReceiptRequest(BaseValidatorRequest):
    message_type: RequestType = RequestType.V0ReceiptRequest
    payload: ReceiptPayload
    signature: str

    def blob_for_signing(self):
        return self.payload.blob_for_signing()
