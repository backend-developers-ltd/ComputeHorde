import enum
import json
from typing import Annotated, Literal, Self

from compute_horde_core.executor_class import ExecutorClass
from compute_horde_core.output_upload import OutputUpload
from compute_horde_core.volume import Volume, VolumeType
from pydantic import BaseModel, Field, model_validator

from compute_horde.base.docker import DockerRunOptionsPreset
from compute_horde.receipts.schemas import (
    JobStartedReceiptPayload,
    Receipt,
    JobAcceptedReceiptPayload,
    JobFinishedReceiptPayload,
)
from compute_horde.utils import MachineSpecs

# NOTE:
# miner.ec - The executor consumer of a Miner.
# miner.vc - The validator consumer of a Miner.

# TODO: remove `Request` suffix from all the models.


# executor -> miner.ec -> miner.vc -> validator
class GenericError(BaseModel):
    message_type: Literal["GenericError"] = "GenericError"
    details: str | None = None
    # TODO: add field to indicate at which layer the issue occurred?


class AuthenticationPayload(BaseModel):
    validator_hotkey: str
    miner_hotkey: str
    timestamp: int

    def blob_for_signing(self):
        # pydantic v2 does not support sort_keys anymore.
        return json.dumps(self.model_dump(), sort_keys=True)


# validator -> miner.vc
class V0AuthenticateRequest(BaseModel):
    message_type: Literal["V0AuthenticateRequest"] = "V0AuthenticateRequest"
    payload: AuthenticationPayload
    signature: str
    # TODO: consolidate `AuthenticationPayload` here
    #       and sign the concat of `{validator_hotkey}:{miner_hotkey}:{timestamp}`?

    def blob_for_signing(self):
        return self.payload.blob_for_signing()


class UnauthorizedErrorType(enum.Enum):
    TOKEN_TOO_OLD = "TOKEN_TOO_OLD"
    UNKNOWN_VALIDATOR = "UNKNOWN_VALIDATOR"
    VALIDATOR_INACTIVE = "VALIDATOR_INACTIVE"


# miner.vc -> validator
class UnauthorizedError(BaseModel):
    # TODO: move `UnauthorizedErrorType` under this class?
    message_type: Literal["UnauthorizedError"] = "UnauthorizedError"
    code: UnauthorizedErrorType
    details: str | None = None


class ExecutorClassManifest(BaseModel):
    executor_class: ExecutorClass | int
    count: int


class ExecutorManifest(BaseModel):
    executor_classes: list[ExecutorClassManifest]

    @property
    def total_count(self) -> int:
        return sum([x.count for x in self.executor_classes])


# miner.vc -> validator
class V0ExecutorManifestRequest(BaseModel):
    message_type: Literal["V0ExecutorManifestRequest"] = "V0ExecutorManifestRequest"
    manifest: ExecutorManifest
    # TODO: `manifest: dict[ExecutorClass, int]` ?


# validator -> miner.vc -> miner.ec -> executor
class V0InitialJobRequest(BaseModel):
    message_type: Literal["V0InitialJobRequest"] = "V0InitialJobRequest"
    job_uuid: str
    executor_class: ExecutorClass  # NOT on miner.ec -> executor
    base_docker_image_name: str | None = None
    timeout_seconds: int
    volume: Volume | None = None
    volume_type: VolumeType | None = None
    job_started_receipt_payload: JobStartedReceiptPayload  # NOT on miner.ec -> executor
    job_started_receipt_signature: str  # NOT on miner.ec -> executor

    @model_validator(mode="after")
    def validate_volume_or_volume_type(self) -> Self:
        if bool(self.volume) and bool(self.volume_type):
            raise ValueError("Expected either `volume` or `volume_type`, got both")
        return self


# validator -> miner.vc -> miner.ec -> executor
class V1InitialJobRequest(V0InitialJobRequest):
    message_type: Literal["V1InitialJobRequest"] = "V1InitialJobRequest"
    public_key: str
    executor_ip: str  # ONLY on miner.ec -> executor


# miner.vc -> validator
class V0DeclineJobRequest(BaseModel):
    class Reason(enum.Enum):
        NOT_SPECIFIED = "not_specified"
        BUSY = "busy"
        EXECUTOR_RESERVATION_FAILURE = "executor_reservation_failure"
        EXECUTOR_FAILURE = "executor_failure"
        VALIDATOR_BLACKLISTED = "validator_blacklisted"

    message_type: Literal["V0DeclineJobRequest"] = "V0DeclineJobRequest"
    job_uuid: str
    reason: Reason = Reason.NOT_SPECIFIED
    receipts: list[Receipt] = Field(default_factory=list)


# miner.vc -> validator
class V0AcceptJobRequest(BaseModel):
    message_type: Literal["V0AcceptJobRequest"] = "V0AcceptJobRequest"
    job_uuid: str


# executor -> miner.ec -> miner.vc -> validator
class V0ExecutorFailedRequest(BaseModel):
    message_type: Literal["V0ExecutorFailedRequest"] = "V0ExecutorFailedRequest"
    job_uuid: str  # NOT on miner.ec -> miner.vc
    executor_token: str  # ONLY on miner.ec -> miner.vc


# executor -> miner.ec -> miner.vc -> validator
class V0StreamingJobNotReadyRequest(BaseModel):
    message_type: Literal["V0StreamingJobNotReadyRequest"] = "V0StreamingJobNotReadyRequest"
    job_uuid: str  # NOT on miner.ec -> miner.vc
    executor_token: str  # ONLY on miner.ec -> miner.vc


# executor -> miner.ec -> miner.vc -> validator
class V0ExecutorReadyRequest(BaseModel):
    message_type: Literal["V0ExecutorReadyRequest"] = "V0ExecutorReadyRequest"
    job_uuid: str  # NOT on miner.ec -> miner.vc
    executor_token: str  # ONLY on miner.ec -> miner.vc


# signaling the streaming job is ready to accept connections from user
# executor -> miner.ec -> miner.vc -> validator
class V0StreamingJobReadyRequest(BaseModel):
    message_type: Literal["V0StreamingJobReadyRequest"] = "V0StreamingJobReadyRequest"
    job_uuid: str  # NOT on miner.ec -> miner.vc
    executor_token: str  # ONLY on miner.ec -> miner.vc
    public_key: str
    ip: str  # NOT on executor -> miner.ec
    port: int


# validator -> miner.vc -> miner.ec -> executor
class V0JobRequest(BaseModel):
    message_type: Literal["V0JobRequest"] = "V0JobRequest"
    job_uuid: str
    executor_class: ExecutorClass | None = None  # ONLY on validator -> miner.vc
    docker_image_name: str | None = None
    raw_script: str | None = None
    docker_run_options_preset: DockerRunOptionsPreset
    docker_run_cmd: list[str]
    volume: Volume | None = None
    output_upload: OutputUpload | None = None
    artifacts_dir: str | None = None

    @model_validator(mode="after")
    def validate_at_least_docker_image_or_raw_script(self) -> Self:
        if not (bool(self.docker_image_name) or bool(self.raw_script)):
            raise ValueError("Expected at least one of `docker_image_name` or `raw_script`")
        return self


class JobErrorType(enum.StrEnum):
    HUGGINGFACE_DOWNLOAD = "HUGGINGFACE_DOWNLOAD"


# executor -> miner.ec -> miner.vc -> validator
class V0JobFailedRequest(BaseModel):
    message_type: Literal["V0JobFailedRequest"] = "V0JobFailedRequest"
    job_uuid: str
    docker_process_exit_status: int | None = None
    docker_process_stdout: str
    docker_process_stderr: str
    error_type: JobErrorType | None = None
    error_detail: str | None = None
    timeout: bool  # ONLY on executor -> miner.ec


# executor -> miner.ec -> miner.vc -> validator
class V0JobFinishedRequest(BaseModel):
    message_type: Literal["V0JobFinishedRequest"] = "V0JobFinishedRequest"
    job_uuid: str
    docker_process_stdout: str
    docker_process_stderr: str
    artifacts: dict[str, str] | None = None


# validator -> miner.vc
class V0JobAcceptedReceiptRequest(BaseModel):
    message_type: Literal["V0JobAcceptedReceiptRequest"] = "V0JobAcceptedReceiptRequest"
    payload: JobAcceptedReceiptPayload
    signature: str

    def blob_for_signing(self):
        return self.payload.blob_for_signing()


# validator -> miner.vc
class V0JobFinishedReceiptRequest(BaseModel):
    message_type: Literal["V0JobFinishedReceiptRequest"] = "V0JobFinishedReceiptRequest"
    payload: JobFinishedReceiptPayload
    signature: str

    def blob_for_signing(self):
        return self.payload.blob_for_signing()


# executor -> miner.ec -> miner.vc -> validator
class V0MachineSpecsRequest(BaseModel):
    message_type: Literal["V0MachineSpecsRequest"] = "V0MachineSpecsRequest"
    job_uuid: str
    specs: MachineSpecs


ValidatorToMinerMessage = Annotated[
    V0AuthenticateRequest
    | V0InitialJobRequest
    | V1InitialJobRequest
    | V0JobRequest
    | V0JobAcceptedReceiptRequest
    | V0JobFinishedReceiptRequest,
    Field(discriminator="message_type"),
]

MinerToExecutorMessage = Annotated[
    V0InitialJobRequest | V1InitialJobRequest | V0JobRequest,
    Field(discriminator="message_type"),
]

ExecutorToMinerMessage = Annotated[
    GenericError
    | V0ExecutorFailedRequest
    | V0StreamingJobNotReadyRequest
    | V0ExecutorReadyRequest
    | V0StreamingJobReadyRequest
    | V0JobFailedRequest
    | V0JobFinishedRequest
    | V0MachineSpecsRequest,
    Field(discriminator="message_type"),
]

MinerToValidatorMessage = Annotated[
    GenericError
    | UnauthorizedError
    | V0ExecutorManifestRequest
    | V0DeclineJobRequest
    | V0AcceptJobRequest
    | V0ExecutorFailedRequest
    | V0StreamingJobNotReadyRequest
    | V0ExecutorReadyRequest
    | V0StreamingJobReadyRequest
    | V0JobFailedRequest
    | V0JobFinishedRequest
    | V0MachineSpecsRequest,
    Field(discriminator="message_type"),
]
