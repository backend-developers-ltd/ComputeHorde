from typing import Annotated, Literal, Self

import pydantic
from pydantic import BaseModel, model_validator

from compute_horde.base.output_upload import OutputUpload, ZipAndHttpPutUpload
from compute_horde.base.volume import Volume, ZipUrlVolume
from compute_horde.executor_class import DEFAULT_EXECUTOR_CLASS, ExecutorClass


class Error(BaseModel, extra="allow"):
    msg: str
    type: str
    help: str = ""


class Response(BaseModel, extra="forbid"):
    """Message sent from facilitator to validator in response to AuthenticationRequest & JobStatusUpdate"""

    status: Literal["error", "success"]
    errors: list[Error] = []


class V0FacilitatorJobRequest(BaseModel, extra="forbid"):
    """Message sent from facilitator to validator to request a job execution"""

    # this points to a `ValidatorConsumer.job_new` handler (fuck you django-channels!)
    type: Literal["job.new"] = "job.new"
    message_type: Literal["V0JobRequest"] = "V0JobRequest"

    uuid: str
    miner_hotkey: str
    # TODO: remove default after we add executor class support to facilitator
    executor_class: ExecutorClass = DEFAULT_EXECUTOR_CLASS
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


class V1FacilitatorJobRequest(BaseModel, extra="forbid"):
    """Message sent from facilitator to validator to request a job execution"""

    # this points to a `ValidatorConsumer.job_new` handler (fuck you django-channels!)
    type: Literal["job.new"] = "job.new"
    message_type: Literal["V1JobRequest"] = "V1JobRequest"
    uuid: str
    miner_hotkey: str
    # TODO: remove default after we add executor class support to facilitator
    executor_class: ExecutorClass = DEFAULT_EXECUTOR_CLASS
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


JobRequest = Annotated[
    V0FacilitatorJobRequest | V1FacilitatorJobRequest,
    pydantic.Field(discriminator="message_type"),
]
