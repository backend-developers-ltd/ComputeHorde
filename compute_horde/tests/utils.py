import random
from uuid import uuid4

from bittensor import Keypair
from compute_horde_core.executor_class import ExecutorClass
from django.utils import timezone

from compute_horde.receipts.schemas import JobStartedReceiptPayload, Receipt, ReceiptType
from compute_horde.utils import sign_blob

_USE_RANDOM = object()


def random_receipt(
    miner_keypair: Keypair = _USE_RANDOM,
    validator_keypair: Keypair = _USE_RANDOM,
):
    if miner_keypair is _USE_RANDOM:
        miner_keypair = random_keypair()

    if validator_keypair is _USE_RANDOM:
        validator_keypair = random_keypair()

    payload = JobStartedReceiptPayload(
        job_uuid=str(uuid4()),
        miner_hotkey=miner_keypair.ss58_address,
        validator_hotkey=validator_keypair.ss58_address,
        timestamp=timezone.now(),
        receipt_type=ReceiptType.JobStartedReceipt,
        executor_class=ExecutorClass.always_on__gpu_24gb,
        ttl=100,
        is_organic=False,
    )
    blob = payload.blob_for_signing()
    return Receipt(
        payload=payload,
        miner_signature=sign_blob(miner_keypair, blob),
        validator_signature=sign_blob(validator_keypair, blob),
    )


def random_keypair() -> Keypair:
    return Keypair.create_from_seed(random.randbytes(32).hex())
