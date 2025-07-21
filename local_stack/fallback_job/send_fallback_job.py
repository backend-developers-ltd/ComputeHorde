import logging
import os
import asyncio
import base64
import boto3
import uuid
from compute_horde_sdk._internal.fallback.client import FallbackClient
from compute_horde_sdk._internal.fallback.job import FallbackJobSpec
from compute_horde_sdk._internal.sdk import ComputeHordeJobSpec
from compute_horde_sdk.v1 import ExecutorClass
from compute_horde_sdk._internal.models import (
    InlineInputVolume,
    HTTPOutputVolume,
    HTTPInputVolume,
)


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

INPUT_FILE_CONTENT = b"This is the input file content.\n"


def get_presigned_urls(
    bucket: str, post_object_key: str, put_object_key: str, expires_in: int = 3600
):
    """
    Generate presigned POST and PUT URLs for the given S3 bucket and object keys.
    """
    s3_client = boto3.client("s3")
    presigned_post = s3_client.generate_presigned_post(
        Bucket=bucket, Key=post_object_key, ExpiresIn=expires_in
    )
    presigned_put = s3_client.generate_presigned_url(
        "put_object",
        Params={"Bucket": bucket, "Key": put_object_key},
        ExpiresIn=expires_in,
    )
    if not presigned_post:
        raise RuntimeError("Failed to generate presigned POST URL")
    return presigned_post, presigned_put


async def main():
    bucket_name = os.environ.get("S3_BUCKET_NAME", "compute-horde-integration-tests")
    post_object_key = f"{uuid.uuid4().hex}_output_post.txt"
    put_object_key = f"{uuid.uuid4().hex}_output_put.txt"
    presigned_post, presigned_put = get_presigned_urls(
        bucket_name, post_object_key, put_object_key
    )

    logger.info(f"Presigned POST: {presigned_post}")
    logger.info(f"Presigned PUT URL: {presigned_put}")

    compute_horde_job_spec = ComputeHordeJobSpec(
        executor_class=ExecutorClass.spin_up_4min__gpu_24gb,
        job_namespace="SN123.0",
        docker_image="python:3.11-slim",
        artifacts_dir="/output",
        args=[
            "python3 -c \"with open('/volume/input.txt') as fin, "
            f"open('/output/{put_object_key}', 'w') as fout1, "
            f"open('/output/{post_object_key}', 'w') as fout2: "
            "data = fin.read(); "
            "fout1.write('Read from input: ' + data); "
            "fout2.write('Read from input: ' + data)\""
        ],
        input_volumes={
            "/volume/": InlineInputVolume.from_file_contents(
                filename="input.txt", contents=INPUT_FILE_CONTENT
            ),
            "/volume/data/dataset.json": HTTPInputVolume(
                url="https://jsonplaceholder.typicode.com/todos/1",
            ),
        },
        output_volumes={
            f"/output/{post_object_key}": HTTPOutputVolume(
                http_method="POST",
                url=presigned_post["url"],
                form_fields=presigned_post["fields"],
            ),
            f"/output/{put_object_key}": HTTPOutputVolume(
                http_method="PUT",
                url=presigned_put,
            ),
        },
        download_time_limit_sec=5,
        execution_time_limit_sec=10,
        upload_time_limit_sec=5,
        streaming_start_time_limit_sec=5,
    )

    fallback_job_spec = FallbackJobSpec.from_job_spec(
        compute_horde_job_spec, work_dir="/output"
    )

    with FallbackClient(cloud="runpod", idle_minutes=1) as fallback_client:
        job = await fallback_client.create_job(fallback_job_spec)
        await job.wait(timeout=120)
        print(f"[Fallback] Job status: {job.status}")
        print(f"[Fallback] Job output:\n{job.result.stdout}")
        print(f"[Fallback] Artifacts: {job.result.artifacts}")
        # Verification step
        expected_content = b"Read from input: " + INPUT_FILE_CONTENT
        if not len(job.result.artifacts) == 2:
            raise RuntimeError(
                "Expected to find 2 artifacts, but found %d", len(job.result.artifacts)
            )
        for _, content in job.result.artifacts.items():
            try:
                decoded = base64.b64decode(content)
            except Exception:
                decoded = content
            if decoded != expected_content:
                raise RuntimeError(
                    "Artifact verification failed: one or more artifacts do not match the expected content."
                )
    logger.info("Success!")


if __name__ == "__main__":
    asyncio.run(main())
