import asyncio
import pathlib
import random
import string
import os
import logging

import boto3
import bittensor

from compute_horde_sdk._internal.sdk import ComputeHordeJobSpec
from compute_horde_sdk.v1 import ComputeHordeClient, ExecutorClass, HTTPOutputVolume

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize wallet and Compute Horde client.
wallet = bittensor.wallet(
    name="validator",
    hotkey="default",
    path=(pathlib.Path(__file__).parent / "wallets").as_posix()
)
compute_horde_client = ComputeHordeClient(
    hotkey=wallet.hotkey,
    compute_horde_validator_hotkey=wallet.hotkey.ss58_address,
    facilitator_url="http://localhost:9000",
)


def get_presigned_urls(bucket: str, post_object_key: str, put_object_key: str, expires_in: int = 3600):
    """
    Generate presigned POST and PUT URLs for the given S3 bucket and object keys.
    """
    s3_client = boto3.client('s3')
    presigned_post = s3_client.generate_presigned_post(Bucket=bucket, Key=post_object_key, ExpiresIn=expires_in)
    presigned_put = s3_client.generate_presigned_url(
        "put_object",
        Params={"Bucket": bucket, "Key": put_object_key},
        ExpiresIn=expires_in
    )
    if not presigned_post:
        raise RuntimeError("Failed to generate presigned POST URL")
    return presigned_post, presigned_put


async def main() -> None:
    ci_run = os.environ.get("GITHUB_ACTIONS") == "true"

    # Set up the default job specification.
    compute_horde_job_spec = ComputeHordeJobSpec(
        executor_class=ExecutorClass.always_on__llm__a6000,
        job_namespace="SN123.0",
        docker_image="alpine",
        args=["sh", "-c", "echo 'Hello, World!' > /artifacts/stuff"],
        artifacts_dir="/artifacts",
    )

    if ci_run:
        bucket_name = "compute-horde-integration-tests"
        available_characters = string.ascii_letters + string.digits
        filename = "".join(random.choices(available_characters, k=32))
        post_object_key = f"{filename}_post.txt"
        put_object_key = f"{filename}_put.txt"
        presigned_post, presigned_put = get_presigned_urls(bucket_name, post_object_key, put_object_key)

        # Update job spec for CI to include S3 uploads.
        compute_horde_job_spec.args = [
            "sh", "-c",
            f"echo 'Hello, World!' | tee -a /artifacts/stuff /output/{post_object_key} /output/{put_object_key}"
        ]
        compute_horde_job_spec.output_volumes = {
            f"/output/{post_object_key}": HTTPOutputVolume(
                http_method="POST",
                url=presigned_post["url"],
                form_fields=presigned_post["fields"],
            ),
            f"/output/{put_object_key}": HTTPOutputVolume(
                http_method="PUT",
                url=presigned_put,
            ),
        }

    # Create and submit the job.
    job = await compute_horde_client.create_job(compute_horde_job_spec)
    await job.wait(timeout=10 * 60)

    # Validate job completion and output.
    expected_artifacts = {'/artifacts/stuff': b'Hello, World!\n'}
    if job.status != "Completed" or job.result.artifacts != expected_artifacts:
        raise RuntimeError(f"Job failed: status={job.status}, artifacts={job.result.artifacts}")

    post_object_path = f"/output/{post_object_key}"
    put_object_path = f"/output/{put_object_key}"
    if ci_run:
        if len(job.result.upload_results) != 2:
            raise RuntimeError(f"Expected 2 keys in upload results, found {len(job.result.upload_results)}")
        if post_object_path not in job.result.upload_results:
            raise RuntimeError(f"Missing key: {post_object_path}")
        if put_object_path not in job.result.upload_results:
            raise RuntimeError(f"Missing key: {put_object_path}")
        if not job.result.upload_results[post_object_path].headers:
            raise RuntimeError("No headers for POST upload")
        if not job.result.upload_results[put_object_path].headers:
            raise RuntimeError("No headers for PUT upload")

    logger.info("Success!")


if __name__ == "__main__":
    asyncio.run(main())
