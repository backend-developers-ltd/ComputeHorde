from django.core.management import BaseCommand

from compute_horde_validator.validator.allowance.tasks import (
    scan_archive_blocks_and_calculate_allowance,
)


class Command(BaseCommand):
    def handle(self, *args, **options):
        scan_archive_blocks_and_calculate_allowance(keep_running=False)
