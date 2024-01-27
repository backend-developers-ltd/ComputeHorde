import asyncio

from django.core.management.base import BaseCommand

from validator.app.src.compute_horde_validator.validator.synthetic_jobs.utils import debug_run_synthetic_jobs


class Command(BaseCommand):
    def handle(self, *args, **options):
        asyncio.run(debug_run_synthetic_jobs())
