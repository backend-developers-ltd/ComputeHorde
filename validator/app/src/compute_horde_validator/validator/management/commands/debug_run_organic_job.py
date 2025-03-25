import sys

from asgiref.sync import async_to_sync
from compute_horde.executor_class import DEFAULT_EXECUTOR_CLASS
from django.core.management.base import BaseCommand
from django.utils import timezone

from compute_horde_validator.validator.models import (
    AdminJobRequest,
    Miner,
    MinerBlacklist,
    OrganicJob,
)
from compute_horde_validator.validator.organic_jobs.miner_driver import JobStatusUpdate
from compute_horde_validator.validator.tasks import run_admin_job_request


async def notify_job_status_update(msg: JobStatusUpdate):
    comment = msg.metadata.comment if msg.metadata else ""
    print(f"\njob status: {msg.status} {comment}")
    if (
        msg.metadata
        and msg.metadata.miner_response
        and (
            msg.metadata.miner_response.docker_process_stderr != ""
            or msg.metadata.miner_response.docker_process_stdout != ""
        )
    ):
        print(f"stderr: {msg.metadata.miner_response.docker_process_stderr}")
        print(f"stdout: {msg.metadata.miner_response.docker_process_stdout}")


class Command(BaseCommand):
    """
    For running in dev environment, not in production
    """

    def add_arguments(self, parser):
        parser.add_argument("--miner_hotkey", default=None, type=str, help="Miner Hotkey")
        # TODO: mock miner with address, port, ip_type
        # parser.add_argument("--miner_address", default=None, type=str, help="Miner IPv4 address")
        # parser.add_argument("--miner_port", default=None, type=int, help="Miner port")
        parser.add_argument(
            "--executor_class", type=str, help="Executor class", default=DEFAULT_EXECUTOR_CLASS
        )
        parser.add_argument("--timeout", type=int, help="Timeout value", required=True)
        parser.add_argument(
            "--docker_image", type=str, help="docker image for job execution", required=True
        )
        parser.add_argument(
            "--cmd_args",
            type=str,
            default="",
            help="arguments passed to the script or docker image",
        )
        parser.add_argument(
            "--use_gpu",
            action="store_true",
            help="use gpu for job execution",
        )
        parser.add_argument(
            "--input_url",
            type=str,
            default="",
            help="input url for job execution",
        )
        parser.add_argument(
            "--output_url",
            type=str,
            default="",
            help="output url for job execution",
        )
        parser.add_argument(
            "--nonzero_if_not_complete",
            action="store_true",
            help="if job completes with PENDING or FAILED state, exit with non-zero status code",
        )

    def handle(self, *args, **options):
        hotkey = options["miner_hotkey"]

        miner = None
        if hotkey:
            try:
                miner = Miner.objects.get(hotkey=hotkey)
            except Miner.DoesNotExist:
                print(f"Miner with hotkey {hotkey} not found")
                sys.exit(1)
            miner_blacklisted = MinerBlacklist.objects.filter(miner=miner).exists()
            if miner_blacklisted:
                raise ValueError(f"miner with hotkey {hotkey} is blacklisted")
        else:
            miner = Miner.objects.exclude(minerblacklist__isnull=False).first()

        if miner is None:
            print("No miners found")
            sys.exit(1)

        print(f"\nPicked miner: {miner} to run the job")

        job_request = AdminJobRequest.objects.create(
            miner=miner,
            timeout=options["timeout"],
            executor_class=options["executor_class"],
            docker_image=options["docker_image"],
            args=options["cmd_args"],
            use_gpu=options["use_gpu"],
            input_url=options["input_url"],
            output_url=options["output_url"],
            created_at=timezone.now(),
        )

        try:
            async_to_sync(run_admin_job_request)(job_request.pk, callback=notify_job_status_update)
        except KeyboardInterrupt:
            print("Interrupted by user")
            sys.exit(1)

        try:
            job_request.refresh_from_db()
            job = OrganicJob.objects.get(job_uuid=job_request.uuid)
            print(f"\nJob {job.job_uuid} done processing")
        except OrganicJob.DoesNotExist:
            print(f"\nJob {job_request.uuid} not found")
            sys.exit(1)

        if options["nonzero_if_not_complete"] and job.status != OrganicJob.Status.COMPLETED:
            print(f"\nJob {job_request.uuid} was unsuccessful, status = {job.status}")
            sys.exit(1)
