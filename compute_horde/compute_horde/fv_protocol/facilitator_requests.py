import base64
import typing
from typing import Annotated, Literal, Self

import pydantic
from pydantic import (
    BaseModel,
    JsonValue,
    field_serializer,
    field_validator,
    model_validator,
)

from compute_horde.base.output_upload import MultiUpload, OutputUpload, ZipAndHttpPutUpload
from compute_horde.base.volume import MultiVolume, Volume, ZipUrlVolume
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
    signatory: str = (
        ""  # identity of the signer (e.g. sa58 address if signature_type == "bittensor")
    )
    timestamp_ns: int = 0  # UNIX timestamp in nanoseconds
    signature: bytes

    @field_validator("signature")
    @classmethod
    def validate_signature(cls, signature: str) -> bytes:
        return base64.b64decode(signature)

    @field_serializer("signature")
    def serialize_signature(self, signature: bytes) -> str:
        return base64.b64encode(signature).decode("utf-8")


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


class SignedFields(BaseModel):
    executor_class: str
    docker_image: str
    raw_script: str
    args: str
    env: dict[str, str]
    use_gpu: bool

    volumes: list[JsonValue]
    uploads: list[JsonValue]

    @staticmethod
    def from_facilitator_sdk_json(data: JsonValue):
        data = typing.cast(dict[str, JsonValue], data)

        signed_fields = SignedFields(
            executor_class=str(data.get("executor_class")),
            docker_image=str(data.get("docker_image", "")),
            raw_script=str(data.get("raw_script", "")),
            args=str(data.get("args", "")),
            env=typing.cast(dict[str, str], data.get("env", None)),
            use_gpu=typing.cast(bool, data.get("use_gpu")),
            volumes=typing.cast(list[JsonValue], data.get("volumes", [])),
            uploads=typing.cast(list[JsonValue], data.get("uploads", [])),
        )
        return signed_fields


def to_json_array(data) -> list[JsonValue]:
    return typing.cast(list[JsonValue], [x.model_dump() for x in data])


class V2JobRequest(BaseModel, extra="forbid"):
    """Message sent from facilitator to validator to request a job execution"""

    # this points to a `ValidatorConsumer.job_new` handler (fuck you django-channels!)
    type: Literal["job.new"] = "job.new"
    message_type: Literal["V2JobRequest"] = "V2JobRequest"
    signature: Signature | None = None

    uuid: str

    # !!! all fields below are included in the signed json payload
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

    def get_signed_fields(self) -> SignedFields:
        volumes = (
            to_json_array(
                self.volume.volumes if isinstance(self.volume, MultiVolume) else [self.volume]
            )
            if self.volume
            else []
        )

        uploads = (
            to_json_array(
                self.output_upload.uploads
                if isinstance(self.output_upload, MultiUpload)
                # TODO: fix consolidate faci output_upload types
                else [self.output_upload]  # type: ignore
            )
            if self.output_upload
            else []
        )

        signed_fields = SignedFields(
            executor_class=self.executor_class,
            docker_image=self.docker_image,
            raw_script=self.raw_script,
            args=" ".join(self.args),
            env=self.env,
            use_gpu=self.use_gpu,
            volumes=volumes,
            uploads=uploads,
        )
        return signed_fields

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
