import asyncio
import pathlib

import bittensor
from compute_horde_sdk.v1 import ComputeHordeClient, ExecutorClass

wallet = bittensor.wallet(name="validator", hotkey="default", path=(pathlib.Path(__file__).parent / "wallets").as_posix())

compute_horde_client = ComputeHordeClient(
    hotkey=wallet.hotkey,
    compute_horde_validator_hotkey=wallet.hotkey.ss58_address,
    facilitator_url="http://localhost:9000",
)

async def main():
    # Create a job to run on the Compute Horde.
    job = await compute_horde_client.create_job(
        executor_class=ExecutorClass.always_on__llm__a6000,
        job_namespace="SN123.0",
        docker_image="alpine",
        # args=["sh", "-c", "sleep 60; echo 'Hello, World!' > /artifacts/stuff"],
        args=["sh", "-c", "while true; do echo -e 'HTTP/1.1 200 OK\r\nContent-Length: 13\r\n\r\nHello, World!' | nc -l -p 8000; done & sleep 150"],
        artifacts_dir="/artifacts",
        streaming=True,
    )

    await job.wait(timeout=10 * 60)

    assert (job.status, job.result.artifacts) == ("Completed", {'/artifacts/stuff': b'Hello, World!\n'}),\
        f"{job.status=}, {job.result.artifacts=}"

    print("Success!")

asyncio.run(main())