from compute_horde_miner.miner.models import JobReceipt
from compute_horde_miner.miner.receipt_store.base import Receipt
from compute_horde_miner.miner.receipt_store.current import receipts_store


async def prepare_receipts() -> None:
    receipts = [Receipt.from_job_receipt(jr) async for jr in JobReceipt.objects.all()]
    await receipts_store.store(receipts)
