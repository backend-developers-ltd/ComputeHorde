from typing import Any, Self

import bittensor
from pydantic import BaseModel


class V0Heartbeat(BaseModel, extra="forbid"):
    """Message sent from validator to facilitator to keep connection alive"""

    message_type: str = "V0Heartbeat"


class V0AuthenticationRequest(BaseModel, extra="forbid"):
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


class V0MachineSpecsUpdate(BaseModel, extra="forbid"):
    """Message sent from validator to facilitator to update miner specs"""

    message_type: str = "V0MachineSpecsUpdate"
    miner_hotkey: str
    validator_hotkey: str
    specs: dict[str, Any]
    batch_id: str
