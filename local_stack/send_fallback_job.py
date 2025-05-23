import logging
logging.basicConfig(level=logging.DEBUG)

import asyncio
import os
from compute_horde_sdk._internal.fallback.client import FallbackClient
from compute_horde_sdk._internal.fallback.job import FallbackJobSpec
from compute_horde_sdk._internal.sdk import ComputeHordeJobSpec
from compute_horde_sdk.v1 import ExecutorClass
from compute_horde_sdk._internal.models import InlineInputVolume, HTTPOutputVolume
import boto3
logger = logging.getLogger(__name__)

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

async def main():
    bucket_name = "compute-horde-integration-tests"
    post_object_key = "output_post.txt"
    put_object_key = "output_put.txt"
    presigned_post, presigned_put = get_presigned_urls(bucket_name, post_object_key, put_object_key)

    logger.info(f"Presigned POST: {presigned_post}")
    logger.info(f"Presigned PUT URL: {presigned_put}")  

    compute_horde_job_spec = ComputeHordeJobSpec(
        executor_class=ExecutorClass.always_on__llm__a6000,
        job_namespace="SN123.0",
        docker_image="python:3.11-slim",
        artifacts_dir="/output",
        args=[
            f"python -c \"with open('/volume/input.txt') as fin, open('/output/{put_object_key}', 'w') as fout: data = fin.read(); fout.write('Read from input: ' + data)\""
        ],
        input_volumes={
            "/volume/": InlineInputVolume.from_file_contents(
                filename="input.txt",
                contents=b"This is the input file content.\n"
            )
        },
        output_volumes={
            # f"/output/{post_object_key}": HTTPOutputVolume(
            #     http_method="POST",
            #     url=presigned_post["url"],
            #     form_fields=presigned_post["fields"],
            # ),
            f"/output/{put_object_key}": HTTPOutputVolume(
                http_method="PUT",
                url=presigned_put,
            ),
        },
        download_time_limit_sec=5,
        execution_time_limit_sec=10,
        upload_time_limit_sec=5,
    )

    fallback_job_spec = FallbackJobSpec.from_job_spec(
        compute_horde_job_spec, 
        work_dir="/output", 
        region="us")

    client = FallbackClient(cloud="runpod")
    job = await client.create_job(fallback_job_spec)
    await job.wait(timeout=120)
    print(f"[Fallback] Job status: {job.status}")
    print(f"[Fallback] Job output:\n{job.result.stdout}")
    if job.result.artifacts:
        print(f"[Fallback] Artifacts: {job.result.artifacts}")

if __name__ == "__main__":
    asyncio.run(main()) 