from compute_horde.receipts.models import JobAcceptedReceipt, JobFinishedReceipt, JobStartedReceipt
from compute_horde.receipts.store.current import receipts_store
from django.core.management import BaseCommand


class Command(BaseCommand):
    def handle(self, *args, **options):
        for receipt_cls in [JobStartedReceipt, JobAcceptedReceipt, JobFinishedReceipt]:
            # TODO order by timestamp may improve writing receipt pages
            receipts_store.store([r.to_receipt() for r in receipt_cls.objects.all()])
