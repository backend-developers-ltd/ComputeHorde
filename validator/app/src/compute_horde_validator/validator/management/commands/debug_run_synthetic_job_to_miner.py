import contextlib
import datetime
import logging
import sys
from collections.abc import Iterable

from asgiref.sync import async_to_sync
from compute_horde.executor_class import DEFAULT_EXECUTOR_CLASS
from compute_horde.miner_client.base import MinerConnectionError
from django.conf import settings
from django.core.management.base import BaseCommand
from django.utils.timezone import now

from compute_horde_validator.validator.models import Miner, SyntheticJob, SyntheticJobBatch
from compute_horde_validator.validator.synthetic_jobs.utils import (
    JOB_LENGTH,
    MinerClient,
    execute_synthetic_jobs,
)

logger = logging.getLogger(__name__)


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
        try:
            _execute_jobs(miner_address, miner_port, miner_hotkey, jobs)
        except KeyboardInterrupt:
            print("Interrupted by user")
            sys.exit(1)
        except Exception:
            logger.warning("command failed with exception", exc_info=True)
            sys.exit(1)

        print(f"synthetic_job_uuid={jobs[0].job_uuid}")


@async_to_sync
async def _execute_jobs(
    miner_address: str, miner_port: int, miner_hotkey: str, synthetic_jobs: Iterable[SyntheticJob]
):
    async with contextlib.AsyncExitStack() as exit_stack:
        key = settings.BITTENSOR_WALLET().get_hotkey()
        miner_client = MinerClient(
            miner_address=miner_address,
            miner_port=miner_port,
            miner_hotkey=miner_hotkey,
            my_hotkey=key.ss58_address,
            job_uuid=None,
            batch_id=None,
            keypair=key,
        )
        try:
            await exit_stack.enter_async_context(miner_client)
        except MinerConnectionError as exc:
            print(f"Miner connection error: {exc}")
            return
        for job in synthetic_jobs:
            miner_client.add_job(str(job.job_uuid))
        await execute_synthetic_jobs(miner_client, synthetic_jobs, 0)
