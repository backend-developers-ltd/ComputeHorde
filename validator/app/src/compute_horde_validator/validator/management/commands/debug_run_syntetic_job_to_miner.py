import asyncio
import datetime
from collections.abc import Iterable

from compute_horde.executor_class import DEFAULT_EXECUTOR_CLASS
from django.conf import settings
from django.core.management.base import BaseCommand
from django.utils.timezone import now

from compute_horde_validator.validator.models import Miner, SyntheticJob, SyntheticJobBatch
from compute_horde_validator.validator.synthetic_jobs.utils import (
    JOB_LENGTH,
    MinerClient,
    execute_synthetic_jobs,
)


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument("--miner_hotkey", type=str, help="Miner hotkey", required=True)
        parser.add_argument(
            "--miner_address", type=str, help="Miner IPv4 address", default="127.0.0.1"
        )
        parser.add_argument("--miner_port", type=int, help="Miner port", default=8000)
        parser.add_argument(
            "--executor_class", type=str, help="Executor class", default=DEFAULT_EXECUTOR_CLASS
        )

    def handle(self, *args, **options):
        miner_hotkey = options["miner_hotkey"]
        miner_address = options["miner_address"]
        miner_port = options["miner_port"]
        executor_class = options["executor_class"]
        batch = SyntheticJobBatch.objects.create(
            accepting_results_until=now() + datetime.timedelta(seconds=JOB_LENGTH)
        )
        jobs = [
            SyntheticJob.objects.create(
                batch=batch,
                miner=Miner.objects.get_or_create(hotkey=miner_hotkey)[0],
                miner_address=miner_address,
                miner_address_ip_version=4,
                miner_port=miner_port,
                executor_class=executor_class,
                status=SyntheticJob.Status.PENDING,
            )
        ]

        loop = asyncio.get_event_loop()
        key = settings.BITTENSOR_WALLET().get_hotkey()
        client = MinerClient(
            loop=loop,
            miner_address=miner_address,
            miner_port=miner_port,
            miner_hotkey=miner_hotkey,
            my_hotkey=key.ss58_address,
            job_uuid=None,
            keypair=key,
        )
        asyncio.run(_execute_jobs(client, jobs))


async def _execute_jobs(client: MinerClient, synthetic_jobs: Iterable[SyntheticJob]):
    async with client:
        for job in synthetic_jobs:
            client.add_job(str(job.job_uuid))
        await execute_synthetic_jobs(client, synthetic_jobs)
