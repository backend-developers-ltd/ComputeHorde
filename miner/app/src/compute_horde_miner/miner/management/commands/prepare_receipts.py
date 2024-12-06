from compute_horde.receipts.models import (
    JobAcceptedReceipt,
    JobFinishedReceipt,
    JobStartedReceipt,
    ReceiptModel,
)
from compute_horde.receipts.store.current import receipts_store
from django.core.management import BaseCommand


class Command(BaseCommand):
    def handle(self, *args, **options):
        receipt_types: list[type[ReceiptModel]] = [
            JobStartedReceipt,
            JobAcceptedReceipt,
            JobFinishedReceipt,
        ]
        for receipt_cls in receipt_types:
            receipts = receipt_cls.objects.all()
            receipts_store().store([r.to_receipt() for r in receipts])
