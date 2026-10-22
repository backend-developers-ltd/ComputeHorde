import enum
import json
from typing import Annotated, Any, Literal, Self, TypeAlias

import bittensor
from compute_horde_core.executor_class import ExecutorClass
from compute_horde_core.output_upload import OutputUpload
from compute_horde_core.streaming import StreamingDetails
from compute_horde_core.volume import Volume
from pydantic import AliasChoices, BaseModel, Field, JsonValue, PrivateAttr, TypeAdapter
from typing_extensions import deprecated

from compute_horde.base.docker import DockerRunOptionsPreset
from compute_horde.protocol_consts import (
    HordeFailureReason,
    JobFailureReason,
    JobParticipantType,
    JobRejectionReason,
    JobStage,
)
from compute_horde.receipts.schemas import (
    JobAcceptedReceiptPayload,
    JobFinishedReceiptPayload,
    JobStartedReceiptPayload,
    Receipt,
)
from compute_horde.utils import MachineSpecs, canonical_json, sign_blob, verify_blob_signature

# NOTE:
# miner.ec = Miner's executor consumer
# miner.vc = Miner's validator consumer


FailureContext: TypeAlias = dict[str, JsonValue]


# executor <-> miner.ec <-> miner.vc <-> validator
class GenericError(BaseModel):
    message_type: Literal["GenericError"] = "GenericError"
    details: str | None = None


# validator -> miner.vc
class ValidatorAuthForMiner(BaseModel):
    message_type: Literal["ValidatorAuthForMiner"] = "ValidatorAuthForMiner"
    validator_hotkey: str
    miner_hotkey: str
    timestamp: int
    signature: str

    def blob_for_signing(self) -> str:
        return f"{self.validator_hotkey}:{self.miner_hotkey}:{self.timestamp}"


# miner.vc -> validator
class UnauthorizedError(BaseModel):
    class Code(enum.Enum):
        TOKEN_TOO_OLD = "TOKEN_TOO_OLD"
        UNKNOWN_VALIDATOR = "UNKNOWN_VALIDATOR"
        VALIDATOR_INACTIVE = "VALIDATOR_INACTIVE"

    message_type: Literal["UnauthorizedError"] = "UnauthorizedError"
    code: Code
    details: str | None = None


# miner.vc -> validator
class V0ExecutorManifestRequest(BaseModel):
    message_type: Literal["V0ExecutorManifestRequest"] = "V0ExecutorManifestRequest"
    manifest: dict[ExecutorClass, int]

    @property
    def total_count(self) -> int:
        return sum(self.manifest.values())


# validator -> miner.vc and miner.vc -> validator
class V0MainHotkeyMessage(BaseModel):
    message_type: Literal["V0MainHotkeyMessage"] = "V0MainHotkeyMessage"
    main_hotkey: str | None = None


# validator -> miner.vc
class ValidatorSignedMessage(BaseModel):
    """
    Message whose whole content is signed with the validator hotkey, so the miner can tell a
    request authored by the authenticated validator from one injected into its socket.

    The signature covers the message's JSON object without validator_signature, in canonical
    form. A message from parse_validator_to_miner_message is verified against the object as it
    arrived rather than against its own fields, so a field known to only one side keeps the
    signature valid. Copies keep that object too, so verify a received message before changing
    it.
    """

    validator_signature: str | None = None
    _received_content: dict[str, Any] | None = PrivateAttr(default=None)

    def blob_for_signing(self) -> str:
        content = self._received_content
        if content is None:
            content = json.loads(self.model_dump_json(exclude={"validator_signature"}))
        return canonical_json(content)

    def signed(self, keypair: bittensor.Keypair) -> Self:
        return self.model_copy(
            update={"validator_signature": sign_blob(keypair, self.blob_for_signing())}
        )

    def is_signed_by(self, validator_hotkey: str) -> bool:
        if self.validator_signature is None:
            return False
        return verify_blob_signature(
            validator_hotkey, self.blob_for_signing(), self.validator_signature
        )


# validator -> miner.vc -> miner.ec -> executor
class V0InitialJobRequest(ValidatorSignedMessage):
    class ExecutorTimingDetails(BaseModel):
        allowed_leeway: int
        download_time_limit: int
        execution_time_limit: int
        upload_time_limit: int
        streaming_start_time_limit: int

    message_type: Literal["V0InitialJobRequest"] = "V0InitialJobRequest"
    job_uuid: str
    executor_class: ExecutorClass
    docker_image: str | None = None
    timeout_seconds: int | None = None  # Deprecated - use executor_timing instead
    volume: Volume | None = None
    job_started_receipt_payload: JobStartedReceiptPayload
    job_started_receipt_signature: str

    # this field should be set if the job is a streaming job
    streaming_details: StreamingDetails | None = None

    # This field should be set if the job should use fine-grained timing.
    # Otherwise, the executor will use `timeout_seconds` as the total time limit.
    executor_timing: ExecutorTimingDetails | None = None


# miner.vc -> validator
class V0DeclineJobRequest(BaseModel):
    # TODO(post error propagation): message and reason should not be optional
    message_type: Literal["V0DeclineJobRequest"] = "V0DeclineJobRequest"
    job_uuid: str
    message: str = ""
    reason: JobRejectionReason = JobRejectionReason.UNKNOWN
    receipts: list[Receipt] = Field(default_factory=list)
    context: FailureContext | None = None


# miner.vc -> validator
class V0AcceptJobRequest(BaseModel):
    message_type: Literal["V0AcceptJobRequest"] = "V0AcceptJobRequest"
    job_uuid: str


# executor -> miner.ec -> miner.vc -> validator
@deprecated("Use V0HordeFailedRequest instead")
class V0ExecutorFailedRequest(BaseModel):
    message_type: Literal["V0ExecutorFailedRequest"] = "V0ExecutorFailedRequest"
    job_uuid: str
    executor_token: str | None = None  # SET ONLY on miner.ec -> miner.vc


# executor -> miner.ec -> miner.vc -> validator
@deprecated("Use V0HordeFailedRequest / V0JobFailedRequest instead")
class V0StreamingJobNotReadyRequest(BaseModel):
    message_type: Literal["V0StreamingJobNotReadyRequest"] = "V0StreamingJobNotReadyRequest"
    job_uuid: str
    executor_token: str | None = None  # SET ONLY on miner.ec -> miner.vc


# executor -> miner.ec -> miner.vc -> validator
class V0ExecutorReadyRequest(BaseModel):
    message_type: Literal["V0ExecutorReadyRequest"] = "V0ExecutorReadyRequest"
    job_uuid: str
    executor_token: str | None = None  # SET ONLY on miner.ec -> miner.vc


# signaling the streaming job is ready to accept connections from user
# executor -> miner.ec -> miner.vc -> validator
class V0StreamingJobReadyRequest(BaseModel):
    message_type: Literal["V0StreamingJobReadyRequest"] = "V0StreamingJobReadyRequest"
    job_uuid: str
    executor_token: str | None = None  # SET ONLY on miner.ec -> miner.vc
    public_key: str
    ip: str | None = (
        None  # set by miner after it receives streaming job ready message from executor
    )
    port: int
    miner_signature: str | None = None

    def blob_for_signing(self) -> str:
        return f"{self.job_uuid}:{self.ip}:{self.port}:{self.public_key}"


class V0VolumesReadyRequest(BaseModel):
    message_type: Literal["V0VolumesReadyRequest"] = "V0VolumesReadyRequest"
    job_uuid: str


class V0ExecutionDoneRequest(BaseModel):
    message_type: Literal["V0ExecutionDoneRequest"] = "V0ExecutionDoneRequest"
    job_uuid: str


# validator -> miner.vc -> miner.ec -> executor
class V0JobRequest(ValidatorSignedMessage):
    message_type: Literal["V0JobRequest"] = "V0JobRequest"
    job_uuid: str
    executor_class: ExecutorClass
    docker_image: str
    raw_script: str | None = None
    docker_run_options_preset: DockerRunOptionsPreset
    docker_run_cmd: list[str]
    env: dict[str, str] = Field(default_factory=dict)
    volume: Volume | None = None
    output_upload: OutputUpload | None = None
    artifacts_dir: str | None = None


# executor -> miner.ec -> miner.vc -> validator
class V0JobFailedRequest(BaseModel):
    # TODO(post error propagation): make message, stage and failure reason non-optional
    # TODO(post error propagation): remove aliases after all participants are updated
    message_type: Literal["V0JobFailedRequest"] = "V0JobFailedRequest"
    job_uuid: str
    message: str = Field(default="", validation_alias=AliasChoices("message", "error_detail"))
    reason: JobFailureReason = Field(
        default=JobFailureReason.UNKNOWN,
        validation_alias=AliasChoices("reason", "error_type"),
    )
    stage: JobStage = JobStage.UNKNOWN
    docker_process_exit_status: int | None = None
    docker_process_stdout: str | None = None
    docker_process_stderr: str | None = None
    context: FailureContext | None = None


# executor -> miner.ec -> miner.vc -> validator
class V0JobFinishedRequest(BaseModel):
    message_type: Literal["V0JobFinishedRequest"] = "V0JobFinishedRequest"
    job_uuid: str
    docker_process_stdout: str
    docker_process_stderr: str
    artifacts: dict[str, str] | None = None
    upload_results: dict[str, str] | None = (
        None  # Contains serialized HTTP upload results (if available)
    )


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


# executor -> miner.ec -> miner.vc -> validator
class V0HordeFailedRequest(BaseModel):
    message_type: Literal["V0HordeFailedRequest"] = "V0HordeFailedRequest"
    job_uuid: str
    reported_by: JobParticipantType
    message: str
    reason: HordeFailureReason
    context: FailureContext | None = None


ValidatorToMinerMessage = Annotated[
    GenericError
    | ValidatorAuthForMiner
    | V0InitialJobRequest
    | V0JobRequest
    | V0JobAcceptedReceiptRequest
    | V0JobFinishedReceiptRequest
    | V0MainHotkeyMessage,
    Field(discriminator="message_type"),
]

_validator_to_miner_message: TypeAdapter[ValidatorToMinerMessage] = TypeAdapter(
    ValidatorToMinerMessage
)


def parse_validator_to_miner_message(raw: str | bytes) -> ValidatorToMinerMessage:
    """
    Parses a frame sent by a validator. A signed message is validated from the same JSON object
    that its signature is checked against, so with duplicate keys, too, the miner acts only on
    content the validator signed.

    Raises ValueError, such as json.JSONDecodeError or pydantic.ValidationError, for a malformed
    frame.
    """
    try:
        content = json.loads(raw)
        message = _validator_to_miner_message.validate_python(content)
    except RecursionError as ex:
        raise ValueError("JSON is nested too deeply") from ex
    if isinstance(message, ValidatorSignedMessage):
        message._received_content = {
            key: value for key, value in content.items() if key != "validator_signature"
        }
    return message


MinerToExecutorMessage = Annotated[
    GenericError | V0InitialJobRequest | V0JobRequest,
    Field(discriminator="message_type"),
]

ExecutorToMinerMessage = Annotated[
    GenericError
    | V0ExecutorFailedRequest
    | V0StreamingJobNotReadyRequest
    | V0ExecutorReadyRequest
    | V0StreamingJobReadyRequest
    | V0VolumesReadyRequest
    | V0ExecutionDoneRequest
    | V0JobFailedRequest
    | V0JobFinishedRequest
    | V0MachineSpecsRequest
    | V0HordeFailedRequest,
    Field(discriminator="message_type"),
]

MinerToValidatorMessage = Annotated[
    GenericError
    | UnauthorizedError
    | V0ExecutorManifestRequest
    | V0MainHotkeyMessage
    | V0DeclineJobRequest
    | V0AcceptJobRequest
    | V0ExecutorFailedRequest
    | V0StreamingJobNotReadyRequest
    | V0ExecutorReadyRequest
    | V0StreamingJobReadyRequest
    | V0JobFailedRequest
    | V0VolumesReadyRequest
    | V0ExecutionDoneRequest
    | V0JobFinishedRequest
    | V0MachineSpecsRequest
    | V0HordeFailedRequest,
    Field(discriminator="message_type"),
]
