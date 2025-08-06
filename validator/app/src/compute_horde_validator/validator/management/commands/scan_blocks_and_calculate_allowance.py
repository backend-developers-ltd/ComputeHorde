
from django.core.management import BaseCommand

from compute_horde_validator.validator.allowance.tasks import report_allowance_to_system_events
from compute_horde_validator.validator.allowance.utils.blocks import (
    scan_blocks_and_calculate_allowance,
)


class Command(BaseCommand):
    def handle(self, *args, **options):
        scan_blocks_and_calculate_allowance(report_allowance_to_system_events.delay)
