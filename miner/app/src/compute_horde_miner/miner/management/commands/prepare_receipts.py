from compute_horde.receipts.models import (
    JobAcceptedReceipt,
    JobFinishedReceipt,
    JobStartedReceipt,
    ReceiptModel,
)
from django.core.management import BaseCommand

from compute_horde_miner.miner.receipts import current_store


class Command(BaseCommand):
    def handle(self, *args, **options):
        receipt_types: list[type[ReceiptModel]] = [
            JobStartedReceipt,
            JobAcceptedReceipt,
            JobFinishedReceipt,
        ]
        for receipt_cls in receipt_types:
            receipts = receipt_cls.objects.all()
            current_store().store([r.to_receipt() for r in receipts])
