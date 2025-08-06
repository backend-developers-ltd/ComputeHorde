from django.core.management import BaseCommand

from compute_horde_validator.validator.allowance.tasks import report_allowance_checkpoint
from compute_horde_validator.validator.allowance.utils.blocks import (
    scan_blocks_and_calculate_allowance,
)
from compute_horde_validator.validator.allowance.utils.supertensor import PrecachingSuperTensor
from compute_horde_validator.validator.allowance.utils.supertensor_django_cache import DjangoCache


class Command(BaseCommand):
    def handle(self, *args, **options):
        scan_blocks_and_calculate_allowance(
            report_allowance_checkpoint.delay,
            PrecachingSuperTensor(cache=DjangoCache()),
        )
