from enum import StrEnum
from typing import Any, Literal, Self

import bittensor
from pydantic import BaseModel, JsonValue

from compute_horde import protocol_consts


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
    # TODO(error propagation): this payload is an amalgam of a success and failure: don't use this for errors.
    # TODO(post error propagation): job_uuid is redundant,
    job_uuid: str
    docker_process_stderr: str
    docker_process_stdout: str
    artifacts: dict[str, str] | None = None
    upload_results: dict[str, str] | None = None


class JobRejectionDetails(BaseModel):
    rejected_by: protocol_consts.JobParticipantType
    reason: protocol_consts.JobRejectionReason
    message: str | None = None
    context: JsonValue = None


class JobFailureDetails(BaseModel):
    reason: protocol_consts.JobFailureReason
    message: str | None = None
    context: JsonValue = None
    docker_process_exit_status: int | None = None
    docker_process_stdout: str | None = None
    docker_process_stderr: str | None = None


class HordeFailureDetails(BaseModel):
    reported_by: protocol_consts.JobParticipantType
    reason: protocol_consts.HordeFailureReason
    message: str | None = None
    context: JsonValue = None


class StreamingServerDetails(BaseModel, extra="forbid"):
    streaming_server_cert: str | None = None
    streaming_server_address: str | None = None
    streaming_server_port: int | None = None


class JobStatusUpdateMetadata(BaseModel, extra="allow"):
    """This is really a "payload" attached to a status update."""

    # TODO: "comment" is probably unnecessary? payloads below should contain details if they need to
    comment: str
    miner_response: JobResultDetails | None = None
    job_rejection_details: JobRejectionDetails | None = None
    job_failure_details: JobFailureDetails | None = None
    horde_failure_details: HordeFailureDetails | None = None
    streaming_details: StreamingServerDetails | None = None

    @classmethod
    def from_uncaught_exception(
        cls, reported_by: protocol_consts.JobParticipantType, exception: Exception
    ) -> Self:
        return cls(
            comment="Uncaught exception",
            horde_failure_details=HordeFailureDetails(
                reported_by=reported_by,
                reason=protocol_consts.HordeFailureReason.UNCAUGHT_EXCEPTION,
                message="Uncaught exception",
                context={"exception_type": type(exception).__qualname__},
            ),
        )


class JobStatusUpdate(BaseModel, extra="forbid"):
    # TODO(post error propagation): remove "extra"
    """
    Message sent from validator to facilitator when the job's state changes.
    """

    class Status(StrEnum):
        """
        Job status common between the validator, facilitator, and the SDK.
        """

        SENT = "sent"
        RECEIVED = "received"
        ACCEPTED = "accepted"

        EXECUTOR_READY = "executor_ready"
        STREAMING_READY = "streaming_ready"
        VOLUMES_READY = "volumes_ready"
        EXECUTION_DONE = "execution_done"

        COMPLETED = "completed"
        REJECTED = "rejected"
        FAILED = "failed"
        HORDE_FAILED = "horde_failed"

        @classmethod
        def choices(cls):
            """Return Django-compatible choices tuple for model fields."""
            return [(status.value, status.value) for status in cls]

        @classmethod
        def end_states(cls) -> set["JobStatusUpdate.Status"]:
            return {cls.COMPLETED, cls.REJECTED, cls.FAILED}

        def is_in_progress(self) -> bool:
            return self not in self.end_states()

        def is_successful(self) -> bool:
            return self == self.COMPLETED

        def is_failed(self) -> bool:
            return self in {self.REJECTED, self.FAILED}

    message_type: Literal["V0JobStatusUpdate"] = "V0JobStatusUpdate"
    uuid: str
    status: Status
    # TODO(post error propagation): no "None" default
    stage: protocol_consts.JobStage = protocol_consts.JobStage.UNKNOWN
    metadata: JobStatusUpdateMetadata | None = None


class V0MachineSpecsUpdate(BaseModel, extra="forbid"):
    """Message sent from validator to facilitator to update miner specs"""

    message_type: Literal["V0MachineSpecsUpdate"] = "V0MachineSpecsUpdate"
    miner_hotkey: str
    validator_hotkey: str
    specs: dict[str, Any]
    batch_id: str
