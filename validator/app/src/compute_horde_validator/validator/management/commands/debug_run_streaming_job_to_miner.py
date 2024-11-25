import asyncio
import contextlib
import logging
import pathlib
import sys
import tempfile
import time
import uuid

import uvloop
from asgiref.sync import async_to_sync
from compute_horde.certificate import generate_certificate_at, read_certificate
from compute_horde.executor_class import DEFAULT_EXECUTOR_CLASS, EXECUTOR_CLASS
from compute_horde.miner_client.organic import (
    FailureReason,
    OrganicJobDetails,
    OrganicJobError,
    OrganicMinerClient,
)
from compute_horde.mv_protocol.miner_requests import (
    V0DeclineJobRequest,
    V0ExecutorFailedRequest,
    V0JobFailedRequest,
)
from compute_horde.mv_protocol.validator_requests import (
    V0JobRequest,
    V1InitialJobRequest,
)
from compute_horde.transport import TransportConnectionError
from compute_horde.utils import Timer
from django.conf import settings
from django.core.management.base import BaseCommand

logger = logging.getLogger(__name__)

JOB_STARTED_RECEIPT_MIN_TTL = 30


def get_mock_job_details():
    return OrganicJobDetails(
        job_uuid=str(uuid.uuid4()),
        docker_image="python:3.11-slim",
        docker_run_options_preset="none",
        raw_script="print('hello world')",
        #         raw_script="""
        # import time
        # time.sleep(1000)
        # """
    )


# TODO: implement
class StreamingMinerClient(OrganicMinerClient):
    """
    Miner client to run streaming job on a miner.
    """

    pass


async def run_streaming_job(options, wait_timeout: int = 300):
    miner_hotkey = options["miner_hotkey"]
    miner_address = options["miner_address"]
    miner_port = options["miner_port"]

    job_details = get_mock_job_details()

    client = StreamingMinerClient(
        miner_hotkey=miner_hotkey,
        miner_address=miner_address,
        miner_port=miner_port,
        job_uuid=job_details.job_uuid,
        my_keypair=settings.BITTENSOR_WALLET().get_hotkey(),
    )

    assert client.job_uuid == job_details.job_uuid

    async with contextlib.AsyncExitStack() as exit_stack:
        try:
            await exit_stack.enter_async_context(client)
        except TransportConnectionError as exc:
            raise OrganicJobError(FailureReason.MINER_CONNECTION_FAILED) from exc

        job_timer = Timer(timeout=job_details.total_job_timeout)

        receipt_payload, receipt_signature = client.generate_job_started_receipt_message(
            executor_class=job_details.executor_class,
            max_timeout=int(job_timer.time_left()),
            ttl=max(
                JOB_STARTED_RECEIPT_MIN_TTL,
                EXECUTOR_CLASS[job_details.executor_class].spin_up_time or 0,
            ),
        )

        # Generate validator certificate
        certs_dir = pathlib.Path(tempfile.mkdtemp())
        generate_certificate_at(certs_dir, "127.0.0.1")
        certificate = read_certificate(certs_dir)
        if certificate is None:
            raise Exception("Failed to read certificate")

        await client.send_model(
            V1InitialJobRequest(
                job_uuid=job_details.job_uuid,
                executor_class=job_details.executor_class,
                base_docker_image_name=job_details.docker_image,
                timeout_seconds=job_details.total_job_timeout,
                volume_type=job_details.volume.volume_type if job_details.volume else None,
                job_started_receipt_payload=receipt_payload,
                job_started_receipt_signature=receipt_signature,
                public_key=certificate,
            ),
        )

        try:
            try:
                initial_response = await asyncio.wait_for(
                    client.miner_accepting_or_declining_future,
                    timeout=min(job_timer.time_left(), wait_timeout),
                )
            except TimeoutError as exc:
                raise OrganicJobError(FailureReason.INITIAL_RESPONSE_TIMED_OUT) from exc
            if isinstance(initial_response, V0DeclineJobRequest):
                raise OrganicJobError(FailureReason.JOB_DECLINED, initial_response)

            await client.notify_job_accepted(initial_response)

            await client.send_job_accepted_receipt_message(
                accepted_timestamp=time.time(),
                ttl=int(job_timer.time_left()),
            )

            try:
                executor_readiness_response = await asyncio.wait_for(
                    client.executor_ready_or_failed_future,
                    timeout=min(job_timer.time_left(), wait_timeout),
                )
            except TimeoutError as exc:
                raise OrganicJobError(FailureReason.EXECUTOR_READINESS_RESPONSE_TIMED_OUT) from exc

            if isinstance(executor_readiness_response, V0ExecutorFailedRequest):
                raise OrganicJobError(FailureReason.EXECUTOR_FAILED, executor_readiness_response)

            logger.error(f"!!! Executor is ready: {type(executor_readiness_response)}")
            logger.error(f"!!! Executor is ready: {executor_readiness_response}")

            await client.notify_executor_ready(executor_readiness_response)

            await client.send_model(
                V0JobRequest(
                    job_uuid=job_details.job_uuid,
                    executor_class=job_details.executor_class,
                    docker_image_name=job_details.docker_image,
                    raw_script=job_details.raw_script,
                    docker_run_options_preset=job_details.docker_run_options_preset,
                    docker_run_cmd=job_details.docker_run_cmd,
                    volume=job_details.volume,
                    output_upload=job_details.output,
                )
            )

            try:
                final_response = await asyncio.wait_for(
                    client.miner_finished_or_failed_future,
                    timeout=job_timer.time_left(),
                )
                if isinstance(final_response, V0JobFailedRequest):
                    raise OrganicJobError(FailureReason.JOB_FAILED, final_response)

                await client.send_job_finished_receipt_message(
                    started_timestamp=job_timer.start_time.timestamp(),
                    time_took_seconds=job_timer.passed_time(),
                    score=0,
                )

                return final_response.docker_process_stdout, final_response.docker_process_stderr
            except TimeoutError as exc:
                raise OrganicJobError(FailureReason.FINAL_RESPONSE_TIMED_OUT) from exc
        except Exception:
            await client.send_job_finished_receipt_message(
                started_timestamp=job_timer.start_time.timestamp(),
                time_took_seconds=job_timer.passed_time(),
                score=0,
            )
            raise


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
        uvloop.install()

        try:
            async_to_sync(run_streaming_job)(options)
        except KeyboardInterrupt:
            print("Interrupted by user")
            sys.exit(1)
        except Exception:
            logger.warning("command failed with exception", exc_info=True)
            sys.exit(1)
