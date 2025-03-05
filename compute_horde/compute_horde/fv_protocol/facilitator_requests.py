import typing
from typing import Annotated, Literal

import pydantic
from compute_horde_core.executor_class import ExecutorClass
from compute_horde_core.output_upload import MultiUpload, OutputUpload, ZipAndHttpPutUpload
from compute_horde_core.signature import Signature, SignedFields
from compute_horde_core.volume import MultiVolume, Volume, ZipUrlVolume
from pydantic import BaseModel, JsonValue


class Error(BaseModel, extra="allow"):
    msg: str
    type: str
    help: str = ""


class Response(BaseModel, extra="forbid"):
    """Message sent from facilitator to validator in response to AuthenticationRequest & JobStatusUpdate"""

    status: Literal["error", "success"]
    errors: list[Error] = []


class V0JobCheated(BaseModel, extra="forbid"):
    """Message sent from facilitator to report cheated job"""

    # this points to a `ValidatorConsumer.job_cheated` handler (fuck you django-channels!)
    type: Literal["job.cheated"] = "job.cheated"
    message_type: Literal["V0JobCheated"] = "V0JobCheated"

    job_uuid: str


class V0JobRequest(BaseModel, extra="forbid"):
    """Message sent from facilitator to validator to request a job execution"""

    # this points to a `ValidatorConsumer.job_new` handler (fuck you django-channels!)
    type: Literal["job.new"] = "job.new"
    message_type: Literal["V0JobRequest"] = "V0JobRequest"

    uuid: str
    miner_hotkey: str
    executor_class: ExecutorClass
    docker_image: str
    args: list[str]
    env: dict[str, str]
    use_gpu: bool
    input_url: str
    output_url: str

    def get_args(self):
        return self.args

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
    args: list[str]
    env: dict[str, str]
    use_gpu: bool
    volume: Volume | None = None
    output_upload: OutputUpload | None = None

    def get_args(self):
        return self.args


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
    args: list[str]
    env: dict[str, str]
    use_gpu: bool
    volume: Volume | None = None
    output_upload: OutputUpload | None = None
    artifacts_dir: str | None = None
    on_trusted_miner: bool = False
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
                else [self.output_upload]
            )
            if self.output_upload
            else []
        )

        signed_fields = SignedFields(
            executor_class=self.executor_class,
            docker_image=self.docker_image,
            args=self.args,
            env=self.env,
            use_gpu=self.use_gpu,
            artifacts_dir=self.artifacts_dir or "",
            on_trusted_miner=self.on_trusted_miner,
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


JobRequest = Annotated[
    V0JobRequest | V1JobRequest | V2JobRequest,
    pydantic.Field(discriminator="message_type"),
]
