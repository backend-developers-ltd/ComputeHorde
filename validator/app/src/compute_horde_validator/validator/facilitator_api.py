from typing import Annotated, Any, Literal, Self

import bittensor
import pydantic
from compute_horde.base.output_upload import OutputUpload
from compute_horde.base.volume import Volume
from compute_horde.executor_class import DEFAULT_EXECUTOR_CLASS
from pydantic import BaseModel, model_validator


class Error(BaseModel, extra="allow"):
    msg: str
    type: str
    help: str = ""


class Response(BaseModel, extra="forbid"):
    """Message sent from facilitator to validator in response to AuthenticationRequest & JobStatusUpdate"""

    status: Literal["error", "success"]
    errors: list[Error] = []


class AuthenticationRequest(BaseModel, extra="forbid"):
    """Message sent from validator to facilitator to authenticate itself"""

    message_type: str = "V0AuthenticationRequest"
    public_key: str
    signature: str

    @classmethod
    def from_keypair(cls, keypair: bittensor.Keypair) -> Self:
        return cls(
            public_key=keypair.public_key.hex(),
            signature=f"0x{keypair.sign(keypair.public_key).hex()}",
        )


class V0FacilitatorJobRequest(BaseModel, extra="forbid"):
    """Message sent from facilitator to validator to request a job execution"""

    # this points to a `ValidatorConsumer.job_new` handler (fuck you django-channels!)
    type: Literal["job.new"] = "job.new"
    message_type: Literal["V0JobRequest"] = "V0JobRequest"

    uuid: str
    miner_hotkey: str
    # TODO: remove default after we add executor class support to facilitator
    executor_class: str = DEFAULT_EXECUTOR_CLASS
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


class V1FacilitatorJobRequest(BaseModel, extra="forbid"):
    """Message sent from facilitator to validator to request a job execution"""

    # this points to a `ValidatorConsumer.job_new` handler (fuck you django-channels!)
    type: Literal["job.new"] = "job.new"
    message_type: Literal["V1JobRequest"] = "V1JobRequest"
    uuid: str
    miner_hotkey: str
    # TODO: remove default after we add executor class support to facilitator
    executor_class: str = DEFAULT_EXECUTOR_CLASS
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


class Heartbeat(BaseModel, extra="forbid"):
    message_type: str = "V0Heartbeat"


class MachineSpecsUpdate(BaseModel, extra="forbid"):
    message_type: str = "V0MachineSpecsUpdate"
    miner_hotkey: str
    validator_hotkey: str
    specs: dict[str, Any]
    batch_id: str
