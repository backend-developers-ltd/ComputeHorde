import shlex
import sys
import uuid

from asgiref.sync import async_to_sync
from compute_horde.executor_class import DEFAULT_EXECUTOR_CLASS
from compute_horde.fv_protocol.facilitator_requests import V2JobRequest
from compute_horde.fv_protocol.validator_requests import JobStatusUpdate
from compute_horde_core.executor_class import ExecutorClass
from django.conf import settings
from django.core.management.base import BaseCommand

from compute_horde_validator.validator.allowance.default import allowance
from compute_horde_validator.validator.models import (
    Miner,
    MinerBlacklist,
    OrganicJob,
)
from compute_horde_validator.validator.organic_jobs.miner_client import MinerClient
from compute_horde_validator.validator.organic_jobs.miner_driver import (
    drive_organic_job,
)


def get_keypair():
    return settings.BITTENSOR_WALLET().get_hotkey()


async def notify_job_status_update(msg: JobStatusUpdate):
    print(f"\njob status: {msg.status}")
    if msg.metadata:
        if details := (
            msg.metadata.job_rejection_details
            or msg.metadata.job_failure_details
            or msg.metadata.horde_failure_details
        ):
            print(f"reason: {details.reason}")
            print(f"message: {details.message}")

        if msg.metadata.miner_response and (
            msg.metadata.miner_response.docker_process_stderr != ""
            or msg.metadata.miner_response.docker_process_stdout != ""
        ):
            print(f"stderr: {msg.metadata.miner_response.docker_process_stderr}")
            print(f"stdout: {msg.metadata.miner_response.docker_process_stdout}")


class Command(BaseCommand):
    """
    For running in dev environment, not in production
    """

    def add_arguments(self, parser):
        parser.add_argument("--miner_hotkey", default=None, help="Miner Hotkey")
        parser.add_argument("--miner_address", default=None, help="Miner IP address")
        parser.add_argument("--miner_ip_version", default=4, help="Miner IP version")
        parser.add_argument("--miner_port", default=None, type=int, help="Miner port")
        parser.add_argument(
            "--executor_class",
            type=ExecutorClass,
            help="Executor class",
            default=DEFAULT_EXECUTOR_CLASS,
        )
        parser.add_argument("--docker_image", help="docker image for job execution", required=True)
        parser.add_argument(
            "--cmd_args", default="", help="arguments passed to the script or docker image"
        )

        parser.add_argument(
            "--download_time_limit", default=10, type=int, help="download time limit in seconds"
        )
        parser.add_argument(
            "--execution_time_limit", default=100, type=int, help="execution time limit in seconds"
        )
        parser.add_argument(
            "--upload_time_limit", default=10, type=int, help="upload time limit in seconds"
        )
        parser.add_argument(
            "--streaming_start_time_limit",
            default=10,
            type=int,
            help="streaming start time limit in seconds",
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

        miner_address = miner.address
        miner_ip_version = miner.ip_version
        miner_port = miner.port
        if options["miner_address"]:
            miner_address = options["miner_address"]
            miner_ip_version = options["miner_ip_version"]
        if options["miner_port"]:
            miner_port = options["miner_port"]

        job_request = V2JobRequest(
            uuid=str(uuid.uuid4()),
            executor_class=options["executor_class"],
            docker_image=options["docker_image"],
            args=shlex.split(options["cmd_args"]),
            env={},
            download_time_limit=options["download_time_limit"],
            execution_time_limit=options["execution_time_limit"],
            upload_time_limit=options["upload_time_limit"],
            streaming_start_time_limit=options["streaming_start_time_limit"],
        )

        job = OrganicJob.objects.create(
            job_uuid=str(job_request.uuid),
            miner=miner,
            miner_address=miner_address,
            miner_address_ip_version=miner_ip_version,
            miner_port=miner_port,
            namespace=job_request.job_namespace or job_request.docker_image or None,
            executor_class=job_request.executor_class,
            job_description="User job from facilitator",
            block=allowance().get_current_block(),
        )

        async def _run_job():
            keypair = get_keypair()
            miner_client = MinerClient(
                miner_hotkey=miner.hotkey,
                miner_address=miner_address,
                miner_port=miner_port,
                job_uuid=str(job.job_uuid),
                my_keypair=keypair,
            )
            await drive_organic_job(
                miner_client,
                job,
                job_request,
                notify_callback=notify_job_status_update,
            )

        try:
            async_to_sync(_run_job)()
        except Exception as e:
            print(f"Failed to run job {job.job_uuid}: {e}")
            sys.exit(1)
        except KeyboardInterrupt:
            print("Interrupted by user")
            sys.exit(1)

        job.refresh_from_db()
        print(f"\nJob {job.job_uuid} done processing with status: {job.status}")
