import asyncio
import contextlib
import logging
import sys
import uuid
from typing import cast

import requests
import uvloop
from asgiref.sync import async_to_sync
from compute_horde.certificate import generate_certificate_at
from compute_horde.executor_class import ExecutorClass
from compute_horde.miner_client.organic import (
    FailureReason,
    OrganicJobDetails,
    OrganicJobError,
    OrganicMinerClient,
)
from compute_horde.mv_protocol.miner_requests import (
    RequestType,
    V0DeclineJobRequest,
    V0JobFailedRequest,
    V1ExecutorReadyRequest,
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


class StreamingMinerClient(OrganicMinerClient):
    """
    Miner client to run streaming job on a miner.
    """

    pass


def get_mock_job_details():
    return OrganicJobDetails(
        job_uuid=str(uuid.uuid4()),
        executor_class  = ExecutorClass.always_on__llm__a6000,
        docker_image="python:3.11-slim",
        docker_run_options_preset="none",
        raw_script="""
import os
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

class SimpleHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/execute-job":
            self.send_response(200)
            self.send_header("Content-type", "text/plain")
            self.end_headers()
            self.wfile.write(b"OK")
            time.sleep(2)
            os._exit(0)  # Mock job finish after endpoint was hit
        else:
            self.send_response(404)
            self.send_header("Content-type", "text/plain")
            self.end_headers()
            self.wfile.write(b"Not Found")

if __name__ == '__main__':
    httpd = HTTPServer(("0.0.0.0", 8000), SimpleHandler)
    httpd.serve_forever()
""",
    )


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

    # Generate validator certificate
    dir_path, public_key, cert = generate_certificate_at()

    async with contextlib.AsyncExitStack() as exit_stack:
        try:
            await exit_stack.enter_async_context(client)
        except TransportConnectionError as exc:
            raise OrganicJobError(FailureReason.MINER_CONNECTION_FAILED) from exc

        job_timer = Timer(timeout=job_details.total_job_timeout)

        receipt_payload, receipt_signature = client.generate_job_started_receipt_message(
            executor_class=job_details.executor_class,
            max_timeout=int(job_timer.time_left()),
            ttl=30,
        )

        # Send streaming job initial request with validator public key
        await client.send_model(
            V1InitialJobRequest(
                job_uuid=job_details.job_uuid,
                executor_class=job_details.executor_class,
                base_docker_image_name=job_details.docker_image,
                timeout_seconds=job_details.total_job_timeout,
                volume_type=job_details.volume.volume_type if job_details.volume else None,
                job_started_receipt_payload=receipt_payload,
                job_started_receipt_signature=receipt_signature,
                public_key=public_key,
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

            try:
                exec_ready_response = await asyncio.wait_for(
                    client.executor_ready_or_failed_future,
                    timeout=min(job_timer.time_left(), wait_timeout),
                )
            except TimeoutError as exc:
                raise OrganicJobError(FailureReason.EXECUTOR_READINESS_RESPONSE_TIMED_OUT) from exc

            # Check received streaming job executor ready response
            assert exec_ready_response.message_type == RequestType.V1ExecutorReadyRequest
            exec_ready_response = cast(V1ExecutorReadyRequest, exec_ready_response)

            # Save job certificate received from executor
            executor_cert_path = dir_path / "ssl" / "executor_certificate.pem"
            executor_cert_path.write_text(exec_ready_response.public_key)

            await client.notify_executor_ready(exec_ready_response)

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

            # TODO: handle waiting for job and nginx containers to spin up
            await asyncio.sleep(5)

            # Check you can connect to the job container and trigger job execution
            url = f"https://{exec_ready_response.ip}:{exec_ready_response.port}/execute-job"
            logger.info(f"Making request to job container: {url}")
            response = requests.get(
                url,
                verify=str(executor_cert_path),
                cert=cert,
                headers={"Host": exec_ready_response.ip},
            )
            logger.info(f"Response {response.status_code}: {response.text}")
            assert response.text == "OK"

            try:
                final_response = await asyncio.wait_for(
                    client.miner_finished_or_failed_future,
                    timeout=job_timer.time_left(),
                )
                if isinstance(final_response, V0JobFailedRequest):
                    raise OrganicJobError(FailureReason.JOB_FAILED, final_response)

                logger.info(f"Job finished: {final_response}")

            except TimeoutError as exc:
                raise OrganicJobError(FailureReason.FINAL_RESPONSE_TIMED_OUT) from exc
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