from typing import Annotated, Literal, Self

import pydantic
from pydantic import BaseModel, JsonValue, model_validator

from compute_horde.base.output_upload import OutputUpload, ZipAndHttpPutUpload
from compute_horde.base.volume import Volume, ZipUrlVolume
from compute_horde.executor_class import ExecutorClass


class Error(BaseModel, extra="allow"):
    msg: str
    type: str
    help: str = ""


class Response(BaseModel, extra="forbid"):
    """Message sent from facilitator to validator in response to AuthenticationRequest & JobStatusUpdate"""

    status: Literal["error", "success"]
    errors: list[Error] = []


class Signature(BaseModel, extra="forbid"):
    # has defaults to allow easy instantiation
    signature_type: str = ""
    signatory: str = ""
    timestamp_ns: int = 0
    signature: str = ""


class V0JobRequest(BaseModel, extra="forbid"):
    """Message sent from facilitator to validator to request a job execution"""

    # this points to a `ValidatorConsumer.job_new` handler (fuck you django-channels!)
    type: Literal["job.new"] = "job.new"
    message_type: Literal["V0JobRequest"] = "V0JobRequest"

    uuid: str
    miner_hotkey: str
    executor_class: ExecutorClass
    docker_image: str
    raw_script: str
    args: list[str]
    env: dict[str, str]
    use_gpu: bool
    input_url: str
    output_url: str

    def get_args(self):
        return self.args

    @model_validator(mode="after")
    def validate_at_least_docker_image_or_raw_script(self) -> Self:
        if not (bool(self.docker_image) or bool(self.raw_script)):
            raise ValueError("Expected at least one of `docker_image` or `raw_script`")
        return self

    @property
    def volume(self) -> Volume | None:
        if self.input_url:
            return ZipUrlVolume(contents=self.input_url)
        return None

    @property
    def output_upload(self) -> OutputUpload | None:
        if self.output_url:
            return ZipAndHttpPutUpload(url=self.output_url)
        return None


class V1JobRequest(BaseModel, extra="forbid"):
    """Message sent from facilitator to validator to request a job execution"""

    # this points to a `ValidatorConsumer.job_new` handler (fuck you django-channels!)
    type: Literal["job.new"] = "job.new"
    message_type: Literal["V1JobRequest"] = "V1JobRequest"
    uuid: str
    miner_hotkey: str
    executor_class: ExecutorClass
    docker_image: str
    raw_script: str
    args: list[str]
    env: dict[str, str]
    use_gpu: bool
    volume: Volume | None = None
    output_upload: OutputUpload | None = None

    def get_args(self):
        return self.args

    @model_validator(mode="after")
    def validate_at_least_docker_image_or_raw_script(self) -> Self:
        if not (bool(self.docker_image) or bool(self.raw_script)):
            raise ValueError("Expected at least one of `docker_image` or `raw_script`")
        return self


class V2JobRequest(BaseModel, extra="forbid"):
    """Message sent from facilitator to validator to request a job execution"""

    # this points to a `ValidatorConsumer.job_new` handler (fuck you django-channels!)
    type: Literal["job.new"] = "job.new"
    message_type: Literal["V2JobRequest"] = "V2JobRequest"
    signature: Signature | None = None

    # !!! all fields below are included in the signed json payload
    uuid: str
    executor_class: ExecutorClass
    docker_image: str
    raw_script: str
    args: list[str]
    env: dict[str, str]
    use_gpu: bool
    volume: Volume | None = None
    output_upload: OutputUpload | None = None
    # !!! all fields above are included in the signed json payload

    def get_args(self):
        return self.args

    def json_for_signing(self) -> JsonValue:
        payload = self.model_dump(mode="json")
        del payload["type"]
        del payload["message_type"]
        del payload["signature"]
        return payload

    @model_validator(mode="after")
    def validate_at_least_docker_image_or_raw_script(self) -> Self:
        if not (bool(self.docker_image) or bool(self.raw_script)):
            raise ValueError("Expected at least one of `docker_image` or `raw_script`")
        return self


JobRequest = Annotated[
    V0JobRequest | V1JobRequest | V2JobRequest,
    pydantic.Field(discriminator="message_type"),
]
