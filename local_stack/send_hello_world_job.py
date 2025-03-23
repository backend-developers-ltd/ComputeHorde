import asyncio
import pathlib

import bittensor

from compute_horde_sdk._internal.sdk import ComputeHordeJobSpec
from compute_horde_sdk.v1 import ComputeHordeClient, ExecutorClass

wallet = bittensor.wallet(name="validator", hotkey="default",
                          path=(pathlib.Path(__file__).parent / "wallets").as_posix())

compute_horde_client = ComputeHordeClient(
    hotkey=wallet.hotkey,
    compute_horde_validator_hotkey=wallet.hotkey.ss58_address,
    facilitator_url="http://localhost:9000",
)


async def main():
    # Create a job to run on the Compute Horde.
    job = await compute_horde_client.create_job(
        ComputeHordeJobSpec(
            executor_class=ExecutorClass.always_on__llm__a6000,
            job_namespace="SN123.0",
            docker_image="alpine",
            args=["sh", "-c", "echo 'Hello, World!' > /artifacts/stuff"],
            artifacts_dir="/artifacts",
        )
    )

    await job.wait(timeout=10 * 60)

    assert (job.status, job.result.artifacts) == ("Completed", {'/artifacts/stuff': b'Hello, World!\n'}), \
        f"{job.status=}, {job.result.artifacts=}"

    print("Success!")


asyncio.run(main())
