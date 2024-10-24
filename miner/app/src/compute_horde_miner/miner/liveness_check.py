import asyncio
import base64
import datetime
import io
import json
import os
import sys
import time
import uuid
import zipfile

import bittensor
import websockets
from channels.layers import get_channel_layer
from compute_horde.base.volume import InlineVolume, VolumeType
from compute_horde.executor_class import DEFAULT_EXECUTOR_CLASS
from compute_horde.mv_protocol import validator_requests
from compute_horde.receipts.schemas import JobStartedReceiptPayload
from django.conf import settings

from compute_horde_miner.channel_layer.channel_layer import ECRedisChannelLayer
from compute_horde_miner.miner.miner_consumer.layer_utils import (
    ExecutorInterfaceMixin,
    JobRequest,
    ValidatorInterfaceMixin,
)
from compute_horde_miner.miner.models import AcceptedJob, Validator

# PYTHONPATH is adjusted in entrypoint, so management commands do not get it by default
if vendor_dir := os.getenv("VENDOR_DIRECTORY"):
    sys.path.insert(0, vendor_dir)

from compute_horde_miner.miner.executor_manager import current  # noqa

JOB_TIMEOUT_SECONDS = 60
PREPARATION_TIMEOUT_SECONDS = 60
JOB_IMAGE_NAME = "backenddevelopersltd/compute-horde-job:v0-latest"
EXPECTED_JOB_RESULT = "mQNJTt"
MAX_SCORE = 2
JOB_RUN_CMD = [
    "--runtime",
    "600",
    "--restore-disable",
    "--attack-mode",
    "3",
    "--workload-profile",
    "3",
    "--optimized-kernel-enable",
    "--hash-type",
    "1410",
    "--hex-salt",
    "-1",
    "?l?d?u",
    "--outfile-format",
    "2",
    "--quiet",
    "5726c17704f709432e2c7d816b8b3fc3236263c4cf7383267bf13bea22e91a85:55507f1971ff79d5",
    "?1?1?1?1?1?1",
]


class CheckError(Exception):
    pass


async def check_all():
    neuron = check_registered()
    await check_publicly_accessible(neuron.axon_info)
    await run_dummy_synthetic_jobs()


def check_registered() -> bittensor.NeuronInfo:
    my_hotkey = settings.BITTENSOR_WALLET().get_hotkey().ss58_address
    metagraph = bittensor.metagraph(
        netuid=settings.BITTENSOR_NETUID, network=settings.BITTENSOR_NETWORK
    )
    neurons = [n for n in metagraph.neurons if n.hotkey == my_hotkey]
    if not neurons:
        raise CheckError("Your hotkey is not in the metagraph. Have you registered?")

    return neurons[0]


async def check_publicly_accessible(axon_info: bittensor.AxonInfo):
    if not axon_info.is_serving:
        raise CheckError("Your miner is not announcing axon info (ip and port) to the metagraph!")

    url = f"ws://{axon_info.ip}:{axon_info.port}/v0.1/validator_interface/fake-hotkey"
    try:
        ws = await websockets.connect(url)
        msg = await ws.recv()
        assert json.loads(msg) == {
            "message_type": "GenericError",
            "details": "Unknown validator: fake-hotkey",
        }
    except (
        AssertionError,
        ConnectionError,
        TimeoutError,
        websockets.WebSocketException,
        json.JSONDecodeError,
    ) as exc:
        raise CheckError(
            "Your miner is not accessible publicly. Check if your port is open in the firewall."
        ) from exc


async def run_dummy_synthetic_jobs():
    # send jobs to executors
    manifest = await current.executor_manager.get_manifest()
    if DEFAULT_EXECUTOR_CLASS not in manifest:
        raise CheckError(
            f"Default executor class {DEFAULT_EXECUTOR_CLASS} not present in manifest."
        )
    num_executors = manifest[DEFAULT_EXECUTOR_CLASS]
    print(f"Number of executor of class {DEFAULT_EXECUTOR_CLASS}: {num_executors}")

    # evaluate the results and print scores
    results = await asyncio.gather(
        *[drive_executor() for _ in range(num_executors)],
        return_exceptions=True,
    )

    print("\n\n")
    failed_count = 0
    for result in results:
        if isinstance(result, Exception):
            print(f"Executor failed with error: {result!r}")
            failed_count += 1
        else:
            print(f"Executor succeeded with score: {result}")
    print(f"Out of {num_executors} executors, {failed_count} failed.")


def get_dummy_inline_zip_volume() -> str:
    in_memory_output = io.BytesIO()
    with zipfile.ZipFile(in_memory_output, "w"):
        pass
    in_memory_output.seek(0)
    zip_contents = in_memory_output.read()
    base64_zip_contents = base64.b64encode(zip_contents)
    return base64_zip_contents.decode()


class JobError(Exception):
    pass


async def drive_executor() -> float:
    job_uuid = str(uuid.uuid4())
    executor_token = f"{job_uuid}-{uuid.uuid4()}"

    keypair = settings.BITTENSOR_WALLET().get_hotkey()
    receipt_payload = JobStartedReceiptPayload(
        job_uuid=job_uuid,
        miner_hotkey=keypair.ss58_address,
        validator_hotkey=keypair.ss58_address,
        timestamp=datetime.datetime.now(datetime.UTC),
        executor_class=DEFAULT_EXECUTOR_CLASS,
        max_timeout=JOB_TIMEOUT_SECONDS,
        is_organic=False,
        ttl=30,
    )
    receipt_signature = f"0x{keypair.sign(receipt_payload.blob_for_signing()).hex()}"
    initial_job_request = validator_requests.V0InitialJobRequest(
        job_uuid=job_uuid,
        executor_class=DEFAULT_EXECUTOR_CLASS,
        base_docker_image_name=JOB_IMAGE_NAME,
        timeout_seconds=JOB_TIMEOUT_SECONDS,
        volume_type=VolumeType.inline,
        job_started_receipt_payload=receipt_payload,
        job_started_receipt_signature=receipt_signature,
    )
    job_request = validator_requests.V0JobRequest(
        job_uuid=job_uuid,
        executor_class=DEFAULT_EXECUTOR_CLASS,
        docker_image_name=JOB_IMAGE_NAME,
        docker_run_options_preset="nvidia_all",
        docker_run_cmd=JOB_RUN_CMD,
        raw_script=None,
        volume=InlineVolume(
            contents=get_dummy_inline_zip_volume(),
        ),
        output_upload=None,
    )

    validator, _ = await Validator.objects.aget_or_create(
        public_key="fake", defaults={"active": False}
    )

    channel_layer: ECRedisChannelLayer = get_channel_layer()
    assert channel_layer is not None
    channel_name = await channel_layer.new_channel()
    group_name = ValidatorInterfaceMixin.group_name(executor_token)
    await channel_layer.group_add(group_name, channel_name)

    # miner consumer will send initial job request for this job after executor connects
    job = AcceptedJob(
        validator=validator,
        job_uuid=job_uuid,
        executor_token=executor_token,
        initial_job_details=initial_job_request.model_dump(),
        status=AcceptedJob.Status.WAITING_FOR_EXECUTOR,
    )
    await job.asave()
    await current.executor_manager._reserve_executor(executor_token)

    # wait for executor to be ready
    msg = await asyncio.wait_for(channel_layer.receive(channel_name), PREPARATION_TIMEOUT_SECONDS)
    if msg["type"] == "executor.ready":
        pass
    elif msg["type"] == "executor.failed_to_prepare":
        raise JobError("Executor failed to prepare")
    else:
        raise JobError("Executor sent unknown message", msg)

    # send full job payload
    await channel_layer.group_send(
        ExecutorInterfaceMixin.group_name(executor_token),
        {
            "type": "miner.job_request",
            **JobRequest(
                job_uuid=job_request.job_uuid,
                docker_image_name=job_request.docker_image_name,
                raw_script=job_request.raw_script,
                docker_run_options_preset=job_request.docker_run_options_preset,
                docker_run_cmd=job_request.docker_run_cmd,
                volume=job_request.volume,
                output_upload=job_request.output_upload,
            ).model_dump(),
        },
    )
    full_job_sent = time.time()

    # wait for job to complete
    while True:
        msg = await asyncio.wait_for(channel_layer.receive(channel_name), JOB_TIMEOUT_SECONDS + 10)
        if msg["type"] == "executor.specs":
            pass
        if msg["type"] == "executor.finished":
            finished_msg = msg
            time_took = time.time() - full_job_sent
            break
        if msg["type"] == _:
            raise JobError("Executor sent unknown message", msg)

    if finished_msg["docker_process_stdout"].strip() != EXPECTED_JOB_RESULT:
        raise JobError("Executor sent wrong result", msg)
    return MAX_SCORE * (1 - (time_took / (2 * JOB_TIMEOUT_SECONDS)))
