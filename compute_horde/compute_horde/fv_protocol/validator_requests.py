from typing import Any, Literal, Self

import bittensor
from pydantic import BaseModel, JsonValue

from compute_horde.protocol_consts import (
    HordeFailureReason,
    JobFailureReason,
    JobFailureStage,
    JobParticipantType,
    JobRejectionReason,
    JobStatus,
)


class V0Heartbeat(BaseModel, extra="forbid"):
    """Message sent from validator to facilitator to keep connection alive"""

    message_type: Literal["V0Heartbeat"] = "V0Heartbeat"


class V0AuthenticationRequest(BaseModel, extra="forbid"):
    """Message sent from validator to facilitator to authenticate itself"""

    message_type: Literal["V0AuthenticationRequest"] = "V0AuthenticationRequest"
    public_key: str
    signature: str

    @classmethod
    def from_keypair(cls, keypair: bittensor.Keypair) -> Self:
        return cls(
            public_key=keypair.public_key.hex(),
            signature=f"0x{keypair.sign(keypair.public_key).hex()}",
        )

    def verify_signature(self) -> bool:
        public_key_bytes = bytes.fromhex(self.public_key)
        keypair = bittensor.Keypair(public_key=self.public_key, ss58_format=42)
        # make mypy happy
        valid: bool = keypair.verify(public_key_bytes, self.signature)
        return valid

    @property
    def ss58_address(self) -> str:
        # make mypy happy
        address: str = bittensor.Keypair(public_key=self.public_key, ss58_format=42).ss58_address
        return address


class JobResultDetails(BaseModel, extra="allow"):
    docker_process_stderr: str
    docker_process_stdout: str
    artifacts: dict[str, str] | None = None
    upload_results: dict[str, str] | None = None


class JobRejectionDetails(BaseModel):
    rejected_by: JobParticipantType
    reason: JobRejectionReason
    message: str
    context: JsonValue = None


class JobFailureDetails(BaseModel):
    reason: JobFailureReason
    stage: JobFailureStage
    message: str
    context: JsonValue = None
    docker_process_exit_status: int | None = None
    docker_process_stdout: str | None = None
    docker_process_stderr: str | None = None


class HordeFailureDetails(BaseModel):
    reported_by: JobParticipantType
    reason: HordeFailureReason
    message: str
    context: JsonValue = None


class StreamingServerDetails(BaseModel, extra="forbid"):
    streaming_server_cert: str | None = None
    streaming_server_address: str | None = None
    streaming_server_port: int | None = None


class JobStatusMetadata(BaseModel, extra="allow"):
    miner_response: JobResultDetails | None = None
    job_rejection_details: JobRejectionDetails | None = None
    job_failure_details: JobFailureDetails | None = None
    horde_failure_details: HordeFailureDetails | None = None
    streaming_details: StreamingServerDetails | None = None

    @classmethod
    def from_uncaught_exception(cls, reported_by: JobParticipantType, exception: Exception) -> Self:
        return cls(
            horde_failure_details=HordeFailureDetails(
                reported_by=reported_by,
                reason=HordeFailureReason.UNCAUGHT_EXCEPTION,
                message="Uncaught exception",
                context={"exception_type": type(exception).__qualname__},
            ),
        )


class JobStatusUpdate(BaseModel, extra="forbid"):
    # TODO(post error propagation): remove "extra"
    """
    Message sent from validator to facilitator when the job's state changes.
    """

    message_type: Literal["V0JobStatusUpdate"] = "V0JobStatusUpdate"
    uuid: str
    status: JobStatus
    metadata: JobStatusMetadata | None = None


class V0MachineSpecsUpdate(BaseModel, extra="forbid"):
    """Message sent from validator to facilitator to update miner specs"""

    message_type: Literal["V0MachineSpecsUpdate"] = "V0MachineSpecsUpdate"
    miner_hotkey: str
    validator_hotkey: str
    specs: dict[str, Any]
    batch_id: str
