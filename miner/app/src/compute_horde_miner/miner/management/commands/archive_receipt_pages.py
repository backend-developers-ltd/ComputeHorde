from django.core.management import BaseCommand

from compute_horde_miner.miner.tasks import archive_receipt_pages


class Command(BaseCommand):
    def handle(self, *args, **options):
        archive_receipt_pages()