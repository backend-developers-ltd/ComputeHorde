from compute_horde.receipts.models import JobStartedReceipt, JobAcceptedReceipt, JobFinishedReceipt
from django.core.management import BaseCommand

from compute_horde_miner.miner.receipt_store.current import receipts_store


class Command(BaseCommand):
    def handle(self, *args, **options):
        for receipt_cls in [JobStartedReceipt, JobAcceptedReceipt, JobFinishedReceipt]:
            # TODO order by timestamp may improve writing receipt pages
            receipts_store.store([
                r.to_receipt() for r in receipt_cls.objects.all()
            ])
