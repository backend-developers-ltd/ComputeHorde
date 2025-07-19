from typing import cast

from asgiref.sync import async_to_sync
from django.conf import settings
from django.core.management.base import BaseCommand
from django.utils.module_loading import import_string

from compute_horde_executor.executor.job_driver import JobDriver
from compute_horde_executor.executor.job_runner import BaseJobRunner, DefaultJobRunner
from compute_horde_executor.executor.miner_client import (
    MinerClient,
)


class Command(BaseCommand):
    help = "Run the executor, query the miner for job details, and run the job docker"

    MINER_CLIENT_CLASS = MinerClient

    runner: BaseJobRunner
    miner_client: MinerClient

    def add_arguments(self, parser):
        parser.add_argument(
            "--startup-time-limit",
            type=int,
            help="Time limit in seconds for startup stage.",
            required=True,
        )
        parser.add_argument(
            "--job-runner-class",
            type=import_string,
            help="Job runner class to use.",
            default=DefaultJobRunner,
        )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @async_to_sync
    async def handle(self, *args, **options):
        self.runner = options["job_runner_class"]()
        self.miner_client = self.MINER_CLIENT_CLASS(settings.MINER_ADDRESS, settings.EXECUTOR_TOKEN)

        driver = JobDriver(
            runner=self.runner,
            miner_client=self.miner_client,
            startup_time_limit=cast(int, options.get("startup_time_limit")),
        )

        await driver.execute()
