from typing import Any, Literal, Self, TypeAlias

import bittensor
from pydantic import BaseModel


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


class MinerResponse(BaseModel, extra="allow"):
    job_uuid: str
    message_type: str | None
    docker_process_stderr: str
    docker_process_stdout: str
    artifacts: dict[str, str] | None = None
    upload_results: dict[str, str] | None = None


class StreamingServerDetails(BaseModel, extra="forbid"):
    streaming_server_cert: str | None = None
    streaming_server_address: str | None = None
    streaming_server_port: int | None = None


class JobStatusMetadata(BaseModel, extra="allow"):
    comment: str
    miner_response: MinerResponse | None = None
    streaming_details: StreamingServerDetails | None = None


JobStatusType: TypeAlias = Literal["failed", "rejected", "accepted", "completed", "streaming_ready"]
# JobStatusType = Literal["failed", "rejected", "accepted", "completed"]

class JobStatusUpdate(BaseModel, extra="forbid"):
    """
    Message sent from validator to facilitator in response to NewJobRequest.
    """

    message_type: Literal["V0JobStatusUpdate"] = "V0JobStatusUpdate"
    uuid: str
    status: JobStatusType
    metadata: JobStatusMetadata | None = None


class V0MachineSpecsUpdate(BaseModel, extra="forbid"):
    """Message sent from validator to facilitator to update miner specs"""

    message_type: Literal["V0MachineSpecsUpdate"] = "V0MachineSpecsUpdate"
    miner_hotkey: str
    validator_hotkey: str
    specs: dict[str, Any]
    batch_id: str
