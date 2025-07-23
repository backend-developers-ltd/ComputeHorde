import asyncio
import pathlib
import os
import logging

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
    path=(pathlib.Path(__file__).parent / "wallets").as_posix(),
)
compute_horde_client = ComputeHordeClient(
    hotkey=wallet.hotkey,
    compute_horde_validator_hotkey=wallet.hotkey.ss58_address,
    facilitator_url="http://localhost:9000",
)


async def main() -> None:
    os.system("docker pull python:3.11-slim")

    # Create a job spec that downloads a small Huggingface model
    compute_horde_job_spec = ComputeHordeJobSpec(
        executor_class=ExecutorClass.always_on__llm__a6000,
        job_namespace="SN123.0",
        docker_image="python:3.11-slim",
        args=[
            "bash",
            "-c",
            "echo 'Hello, world!' && echo 'Job completed successfully' > /output/result.txt",
        ],
        artifacts_dir="/artifacts",
        input_volumes={
            "/volume/model": HuggingfaceInputVolume(
                repo_id="prajjwal1/bert-tiny",
                repo_type="model",
                # usage_type="reusable",
            ),
        },
        download_time_limit_sec=60,
        execution_time_limit_sec=30,
        upload_time_limit_sec=10,
    )

    # Create and submit the job
    logger.info("Creating job...")
    job = await compute_horde_client.create_job(compute_horde_job_spec)
    logger.info("Waiting for job completion...")
    await job.wait(timeout=10 * 60)

    # Validate job completion and output
    if job.status != "Completed":
        logger.error(f"Job failed with status: {job.status}")
        if hasattr(job.result, "stderr"):
            logger.error(f"Job stderr: {job.result.stderr}")
        raise RuntimeError(f"Job failed: status={job.status}")

    logger.info(f"Job completed successfully! Artifacts: {job.result.artifacts}")


if __name__ == "__main__":
    asyncio.run(main())
