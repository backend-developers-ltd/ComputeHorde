import asyncio
import datetime

from django.conf import settings
from django.core.management.base import BaseCommand
from django.utils.timezone import now

from compute_horde_validator.validator.synthetic_jobs.utils import execute_jobs, JOB_LENGTH
from compute_horde_validator.validator.models import Miner, SyntheticJob, SyntheticJobBatch


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument('--miner_hotkey', type=str, help='Miner hotkey', required=True)
        parser.add_argument('--miner_address', type=str, help='Miner IPv4 address', default='127.0.0.1')
        parser.add_argument('--miner_port', type=int, help='Miner port', default=8000)

    def handle(self, *args, **options):
        miner_hotkey = options['miner_hotkey']
        miner_address = options['miner_address']
        miner_port = options['miner_port']
        keypair = settings.BITTENSOR_WALLET().get_hotkey()
        batch = SyntheticJobBatch.objects.create(
            accepting_results_until=now() + datetime.timedelta(seconds=JOB_LENGTH)
        )
        jobs = [SyntheticJob.objects.create(
            batch=batch,
            miner=Miner.objects.get_or_create(hotkey=miner_hotkey)[0],
            miner_address=miner_address,
            miner_address_ip_version=4,
            miner_port=miner_port,
            status=SyntheticJob.Status.PENDING,
        )]
        asyncio.run(execute_jobs(jobs))
