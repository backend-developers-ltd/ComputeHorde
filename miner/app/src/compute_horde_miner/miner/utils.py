import datetime

from django.utils.timezone import now

from compute_horde_miner.miner.models import JobReceipt
from compute_horde_miner.miner.receipt_store.base import Receipt
from compute_horde_miner.miner.receipt_store.current import receipts_store

RECEIPTS_MAX_RETENTION_PERIOD = datetime.timedelta(days=7)


async def prepare_receipts() -> None:
    # TODO: use a lock to prevent simultaneous write to zip file
    await JobReceipt.objects.filter(time_started__lt=now()-RECEIPTS_MAX_RETENTION_PERIOD).adelete()
    receipts = [Receipt.from_job_receipt(jr) async for jr in JobReceipt.objects.all()]
    await receipts_store.store(receipts)
