import enum
import json
import re
from typing import Self

import pydantic
from pydantic import model_validator

from ..base.docker import DockerRunOptionsPreset
from ..base.output_upload import OutputUpload  # noqa
from ..base.volume import Volume, VolumeType
from ..base_requests import BaseRequest, JobMixin
from ..executor_class import ExecutorClass
from ..receipts.schemas import (
    JobAcceptedReceiptPayload,
    JobFinishedReceiptPayload,
    JobStartedReceiptPayload,
)
from ..utils import MachineSpecs

SAFE_DOMAIN_REGEX = re.compile(r".*")


class RequestType(enum.Enum):
    V0AuthenticateRequest = "V0AuthenticateRequest"
    V0InitialJobRequest = "V0InitialJobRequest"
    V0MachineSpecsRequest = "V0MachineSpecsRequest"
    V0JobRequest = "V0JobRequest"
    V0JobAcceptedReceiptRequest = "V0JobAcceptedReceiptRequest"
    V0JobFinishedReceiptRequest = "V0JobFinishedReceiptRequest"
    GenericError = "GenericError"


class BaseValidatorRequest(BaseRequest):
    message_type: RequestType


class AuthenticationPayload(pydantic.BaseModel):
    validator_hotkey: str
    miner_hotkey: str
    timestamp: int

    def blob_for_signing(self):
        # pydantic v2 does not support sort_keys anymore.
        return json.dumps(self.model_dump(), sort_keys=True)


class V0AuthenticateRequest(BaseValidatorRequest):
    message_type: RequestType = RequestType.V0AuthenticateRequest
    payload: AuthenticationPayload
    signature: str

    def blob_for_signing(self):
        return self.payload.blob_for_signing()


class V0InitialJobRequest(BaseValidatorRequest, JobMixin):
    message_type: RequestType = RequestType.V0InitialJobRequest
    executor_class: ExecutorClass | None = None
    base_docker_image_name: str | None = None
    timeout_seconds: int | None = None
    volume: Volume | None = None
    volume_type: VolumeType | None = None
    job_started_receipt_payload: JobStartedReceiptPayload
    job_started_receipt_signature: str

    @model_validator(mode="after")
    def validate_volume_or_volume_type(self) -> Self:
        if bool(self.volume) and bool(self.volume_type):
            raise ValueError("Expected either `volume` or `volume_type`, got both")
        return self


class V0JobRequest(BaseValidatorRequest, JobMixin):
    message_type: RequestType = RequestType.V0JobRequest
    executor_class: ExecutorClass | None = None
    docker_image_name: str | None = None
    raw_script: str | None = None
    docker_run_options_preset: DockerRunOptionsPreset
    docker_run_cmd: list[str]
    volume: Volume | None = None
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


class V0JobFinishedReceiptRequest(BaseValidatorRequest):
    message_type: RequestType = RequestType.V0JobFinishedReceiptRequest
    payload: JobFinishedReceiptPayload
    signature: str

    def blob_for_signing(self):
        return self.payload.blob_for_signing()


class V0JobAcceptedReceiptRequest(BaseValidatorRequest):
    message_type: RequestType = RequestType.V0JobAcceptedReceiptRequest
    payload: JobAcceptedReceiptPayload
    signature: str

    def blob_for_signing(self):
        return self.payload.blob_for_signing()
