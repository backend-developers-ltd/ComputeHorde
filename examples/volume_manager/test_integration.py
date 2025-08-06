import asyncio
import pathlib
import os
import logging
import requests
import bittensor

from compute_horde_sdk._internal.sdk import ComputeHordeJobSpec
from compute_horde_sdk.v1 import (
    ComputeHordeClient,
    ExecutorClass,
    HuggingfaceInputVolume,
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize wallet and Compute Horde client.
wallet = bittensor.wallet(
    name="validator",
    hotkey="default",
    path=(
        pathlib.Path(__file__).parent.parent.parent / "local_stack" / "wallets"
    ).as_posix(),
)
compute_horde_client = ComputeHordeClient(
    hotkey=wallet.hotkey,
    compute_horde_validator_hotkey=wallet.hotkey.ss58_address,
    facilitator_url="http://localhost:9000",
)


async def run_volume_test():
    """Run a test job that downloads a HuggingFace model."""
    logger.info("Running volume download test...")

    # Pull required Docker images
    os.system("docker pull python:3.11-slim")
    os.system(
        "docker pull us-central1-docker.pkg.dev/twistlock-secresearch/public/can-ctr-escape-cve-2022-0492:latest"
    )

    # Create a job spec that downloads a small HuggingFace model
    compute_horde_job_spec = ComputeHordeJobSpec(
        executor_class=ExecutorClass.always_on__llm__a6000,
        job_namespace="SN123.0",
        docker_image="python:3.11-slim",
        args=[
            "bash",
            "-c",
            "echo 'Running volume test...' > /artifacts/stuff",
        ],
        artifacts_dir="/artifacts",
        input_volumes={
            "/volume/model": HuggingfaceInputVolume(
                repo_id="prajjwal1/bert-tiny",
                repo_type="model",
            ),
        },
        download_time_limit_sec=60,
        execution_time_limit_sec=30,
        upload_time_limit_sec=10,
    )

    # Create and submit the job.
    job = await compute_horde_client.create_job(compute_horde_job_spec)
    await job.wait(timeout=60)

    # Validate job completion and output.
    expected_artifacts = {"/artifacts/stuff": b"Running volume test...\n"}
    if job.status != "Completed" or job.result.artifacts != expected_artifacts:
        raise RuntimeError(
            f"Job failed: status={job.status}, artifacts={job.result.artifacts}"
        )

    # Verify volume manager cache
    try:
        response = requests.get("http://localhost:8001/cache/status")
        if response.status_code == 200:
            cache_data = response.json()
            logger.info(f"Volume manager cache status: {cache_data}")
            if "hf-prajjwal1_bert-tiny" in cache_data.get("cached_volumes", []):
                logger.info("✓ Volume manager successfully cached the model")
            else:
                logger.warning("Volume manager cache is empty")
    except Exception:
        logger.warning("Could not check volume manager status")

    logger.info("Volume test finished!")


if __name__ == "__main__":
    asyncio.run(run_volume_test())
