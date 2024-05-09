import datetime

from django.utils.timezone import now

from compute_horde_miner.miner.models import JobReceipt
from compute_horde_miner.miner.receipt_store.base import Receipt
from compute_horde_miner.miner.receipt_store.current import receipts_store


async def prepare_receipts(date: datetime.date | None = None) -> None:
    if date is None:
        date = now().date()

    job_receipts = JobReceipt.objects.filter(time_started__date=date)
    receipts = [Receipt.from_job_receipt(jr) async for jr in job_receipts]
    await receipts_store.store(date, receipts)
