import enum
import logging

import bittensor
import pydantic

from compute_horde.mv_protocol.validator_requests import (
    JobFinishedReceiptPayload,
    JobStartedReceiptPayload,
    JobStillRunningReceiptPayload,
)

logger = logging.getLogger(__name__)


class ReceiptType(enum.Enum):
    JobStartedReceipt = "JobStartedReceipt"
    JobFinishedReceipt = "JobFinishedReceipt"
    JobStillRunningReceipt = "JobStillRunningReceipt"


class Receipt(pydantic.BaseModel):
    payload: JobStartedReceiptPayload | JobFinishedReceiptPayload | JobStillRunningReceiptPayload
    validator_signature: str
    miner_signature: str

    def verify_miner_signature(self):
        miner_keypair = bittensor.Keypair(ss58_address=self.payload.miner_hotkey)
        return miner_keypair.verify(self.payload.blob_for_signing(), self.miner_signature)

    def verify_validator_signature(self):
        validator_keypair = bittensor.Keypair(ss58_address=self.payload.validator_hotkey)
        return validator_keypair.verify(self.payload.blob_for_signing(), self.validator_signature)
