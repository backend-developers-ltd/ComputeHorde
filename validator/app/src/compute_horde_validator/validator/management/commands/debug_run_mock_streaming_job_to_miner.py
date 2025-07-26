import asyncio
import contextlib
import logging
import sys
import uuid

import requests
import uvloop
from asgiref.sync import async_to_sync
from compute_horde.miner_client.organic import (
    OrganicJobDetails,
    OrganicJobError,
    OrganicMinerClient,
)
from compute_horde.protocol_consts import MinerFailureReason
from compute_horde.protocol_messages import (
    V0DeclineJobRequest,
    V0InitialJobRequest,
    V0JobFailedRequest,
    V0JobRequest,
    V0StreamingJobReadyRequest,
)
from compute_horde.transport import TransportConnectionError
from compute_horde.utils import Timer
from compute_horde_core.certificate import generate_certificate_at
from compute_horde_core.executor_class import ExecutorClass
from compute_horde_core.streaming import StreamingDetails
from django.conf import settings
from django.core.management.base import BaseCommand

logger = logging.getLogger(__name__)


def get_mock_job_details():
    return OrganicJobDetails(
        job_uuid=str(uuid.uuid4()),
        executor_class=ExecutorClass.always_on__llm__a6000,
        docker_image="backenddevelopersltd/compute-horde-streaming-job-test:v0-latest",
        docker_run_options_preset="none",
        docker_run_cmd=["python", "./mock_streaming_job.py"],
    )


async def run_streaming_job(options, wait_timeout: int = 300):
    miner_hotkey = options["miner_hotkey"]
    miner_address = options["miner_address"]
    miner_port = options["miner_port"]

    job_details = get_mock_job_details()

    client = OrganicMinerClient(
        miner_hotkey=miner_hotkey,
        miner_address=miner_address,
        miner_port=miner_port,
        job_uuid=job_details.job_uuid,
        my_keypair=settings.BITTENSOR_WALLET().get_hotkey(),
    )

    assert client.job_uuid == job_details.job_uuid

    # Generate validator certificate
    dir_path, public_key, cert = generate_certificate_at()

    async with contextlib.AsyncExitStack() as exit_stack:
        try:
            await exit_stack.enter_async_context(client)
        except TransportConnectionError as exc:
            raise OrganicJobError(MinerFailureReason.MINER_CONNECTION_FAILED) from exc

        job_timer = Timer(timeout=job_details.total_job_timeout)

        receipt_payload, receipt_signature = client.generate_job_started_receipt_message(
            executor_class=job_details.executor_class,
            ttl=30,
        )

        # Send streaming job initial request with validator public key
        await client.send_model(
            V0InitialJobRequest(
                job_uuid=job_details.job_uuid,
                executor_class=job_details.executor_class,
                docker_image=job_details.docker_image,
                timeout_seconds=job_details.total_job_timeout,
                volume=job_details.volume,
                job_started_receipt_payload=receipt_payload,
                job_started_receipt_signature=receipt_signature,
                streaming_details=StreamingDetails(public_key=public_key),
            ),
        )

        try:
            try:
                initial_response = await asyncio.wait_for(
                    client.miner_accepting_or_declining_future,
                    timeout=min(job_timer.time_left(), wait_timeout),
                )
            except TimeoutError as exc:
                raise OrganicJobError(MinerFailureReason.INITIAL_RESPONSE_TIMED_OUT) from exc
            if isinstance(initial_response, V0DeclineJobRequest):
                raise OrganicJobError(MinerFailureReason.JOB_DECLINED, initial_response)

            await client.notify_job_accepted(initial_response)

            try:
                exec_ready_response = await asyncio.wait_for(
                    client.executor_ready_or_failed_future,
                    timeout=min(job_timer.time_left(), wait_timeout),
                )
            except TimeoutError as exc:
                raise OrganicJobError(
                    MinerFailureReason.EXECUTOR_READINESS_RESPONSE_TIMED_OUT
                ) from exc

            await client.notify_executor_ready(exec_ready_response)

            await client.send_model(
                V0JobRequest(
                    job_uuid=job_details.job_uuid,
                    executor_class=job_details.executor_class,
                    docker_image=job_details.docker_image,
                    docker_run_options_preset=job_details.docker_run_options_preset,
                    docker_run_cmd=job_details.docker_run_cmd,
                    volume=job_details.volume,
                    output_upload=job_details.output,
                    artifacts_dir=job_details.artifacts_dir,
                )
            )

            try:
                streaming_job_ready_response = await asyncio.wait_for(
                    client.streaming_job_ready_or_not_future,
                    timeout=min(job_timer.time_left(), wait_timeout),
                )
            except TimeoutError as exc:
                raise OrganicJobError(MinerFailureReason.STREAMING_JOB_READY_TIMED_OUT) from exc

            # Check received streaming job executor ready response
            assert isinstance(streaming_job_ready_response, V0StreamingJobReadyRequest)

            # Save job certificate received from executor
            executor_cert_path = dir_path / "ssl" / "executor_certificate.pem"
            executor_cert_path.write_text(streaming_job_ready_response.public_key)

            base_url = (
                f"https://{streaming_job_ready_response.ip}:{streaming_job_ready_response.port}"
            )

            # Check you can connect to the job container and trigger job execution
            url = f"{base_url}/execute-job"
            logger.info(f"Triggering streaming job execution on: {url}")
            response = requests.get(
                url,
                verify=str(executor_cert_path),
                cert=cert,
                headers={"Host": streaming_job_ready_response.ip},
            )
            logger.info(f"Response {response.status_code}: {response.text}")
            assert response.text == "OK", "Failed to trigger job execution"

            url = f"{base_url}/terminate"
            logger.info(f"Terminating streaming job on: {url}")
            response = requests.get(
                url,
                verify=str(executor_cert_path),
                cert=cert,
                headers={"Host": streaming_job_ready_response.ip},
            )
            logger.info(f"Response {response.status_code}: {response.text}")
            assert response.text == "OK", "Failed to terminate job"

            try:
                final_response = await asyncio.wait_for(
                    client.miner_finished_or_failed_future,
                    timeout=job_timer.time_left(),
                )
                if isinstance(final_response, V0JobFailedRequest):
                    raise OrganicJobError(MinerFailureReason.JOB_FAILED, final_response)

                logger.info(f"Job finished: {final_response}")

            except TimeoutError as exc:
                raise OrganicJobError(MinerFailureReason.FINAL_RESPONSE_TIMED_OUT) from exc
        except Exception:
            raise


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument("--miner_hotkey", type=str, help="Miner hotkey", required=True)
        parser.add_argument(
            "--miner_address", type=str, help="Miner IPv4 address", default="127.0.0.1"
        )
        parser.add_argument("--miner_port", type=int, help="Miner port", default=8000)

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
