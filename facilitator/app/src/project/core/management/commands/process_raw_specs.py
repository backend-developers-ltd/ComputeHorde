import structlog
from asgiref.sync import async_to_sync
from django.core.management import BaseCommand

from ...models import ParsedSpecsData, RawSpecsData
from ...specs import process_raw_specs_data

log = structlog.get_logger(__name__)


class Command(BaseCommand):
    help = "Process RawSpecsData jsons to regenerate MinerSpecs and MinerGpuSpecs tables"

    def add_arguments(self, parser):
        pass

    def handle(self, *args, **options):
        n = RawSpecsData.objects.count()
        log.info(f"Processing {n} raw specs")

        for i, raw_specs in enumerate(RawSpecsData.objects.all()):
            if not ParsedSpecsData.objects.filter(pk=raw_specs.pk).exists():
                async_to_sync(process_raw_specs_data)(raw_specs)
                log.info(f"processed {i}/{n} raw specs {raw_specs.pk}")
            else:
                log.warning(f"skipping {i}/{n} raw specs {raw_specs.pk}")
