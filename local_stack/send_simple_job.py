import asyncio
import pathlib
import logging

import bittensor

from compute_horde_sdk._internal.sdk import ComputeHordeJobSpec
from compute_horde_sdk.v1 import ComputeHordeClient, ExecutorClass

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
    logger.info("Sending job")
    logger.info(f"hotkey={wallet.hotkey.ss58_address}")

    compute_horde_job_spec = ComputeHordeJobSpec(
        executor_class=ExecutorClass.always_on__llm__a6000,
        job_namespace="SN123.0",
        docker_image="alpine",
        args=["sh", "-c", "echo 'Hello, World!' > /artifacts/stuff"],
        artifacts_dir="/artifacts",
        download_time_limit_sec=5,
        execution_time_limit_sec=10,
        upload_time_limit_sec=5,
    )

    # Create and submit the job.
    job = await compute_horde_client.create_job(compute_horde_job_spec)
    await job.wait(timeout=10 * 60)

    # Validate job completion and output.
    expected_artifacts = {"/artifacts/stuff": b"Hello, World!\n"}
    if job.status != "Completed" or job.result.artifacts != expected_artifacts:
        raise RuntimeError(
            f"Job failed: status={job.status}, artifacts={job.result.artifacts}"
        )

    logger.info("Success!")


if __name__ == "__main__":
    asyncio.run(main())
