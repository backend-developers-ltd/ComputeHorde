import asyncio
import pathlib
import random
import string
import os
import logging
import tempfile
import time
import ssl

import boto3
import bittensor
import httpx
from cryptography.hazmat.primitives import serialization
from cryptography.x509 import Certificate
from cryptography.hazmat.primitives.asymmetric import rsa

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


def create_mt_client_with_tempfiles(
    client_cert: Certificate,
    client_key: rsa.RSAPrivateKey,
    server_cert_pem: str
) -> httpx.Client:
    # Serialize cert and key to PEM strings
    cert_pem = client_cert.public_bytes(
        encoding=serialization.Encoding.PEM
    ).decode("utf-8")

    key_pem = client_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption()
    ).decode("utf-8")

    client = None
    with tempfile.NamedTemporaryFile("w+", delete=False) as cert_file, \
            tempfile.NamedTemporaryFile("w+", delete=False) as key_file, \
            tempfile.NamedTemporaryFile("w+", delete=False) as ca_file:
        cert_file.write(cert_pem)
        cert_file.flush()
        key_file.write(key_pem)
        key_file.flush()
        ca_file.write(server_cert_pem)
        ca_file.flush()

        ctx = ssl.create_default_context(
            cafile=ca_file.name,
        )
        client = httpx.Client(
            cert=(cert_file.name, key_file.name),
            verify=ctx
        )
    return client


async def main() -> None:
    compute_horde_streaming_job_spec = ComputeHordeJobSpec(
        executor_class=ExecutorClass.always_on__llm__a6000,
        job_namespace="SN123.0",
        docker_image="backenddevelopersltd/compute-horde-streaming-job-test:v0-latest",
        args=["python", "./mock_streaming_job.py"],
        artifacts_dir="/artifacts",
        streaming=True,
        download_time_limit_sec=5,
        execution_time_limit_sec=15,
        streaming_start_time_limit_sec=5,
        upload_time_limit_sec=5,
    )
    streaming_job = await compute_horde_client.create_job(compute_horde_streaming_job_spec)
    await streaming_job.wait_for_streaming(timeout=60)
    logger.info(f"Streaming server: {streaming_job.streaming_server_address}:{streaming_job.streaming_server_port}")

    client = create_mt_client_with_tempfiles(
        streaming_job.streaming_public_cert,
        streaming_job.streaming_private_key,
        streaming_job.streaming_server_cert
    )
    
    max_terminate_retries = 3
    for attempt in range(1, max_terminate_retries + 1):
        try:
            response = client.get(f"https://{streaming_job.streaming_server_address}:{streaming_job.streaming_server_port}/terminate")
            if 200 <= response.status_code < 300:
                logger.info(f"Successfully terminated streaming server: {response.text}")
                break
            else:
                logger.warning(f"Attempt {attempt}: Non-2xx response when terminating streaming server: {response.status_code} - {response.text}")
        except Exception as e:
            logger.warning(f"Attempt {attempt}: Exception during streaming server termination: {e}")
        if attempt == max_terminate_retries:
            raise RuntimeError(f"Failed to terminate streaming server after {max_terminate_retries} attempts")
        backoff = 2 ** (attempt - 1)
        logger.info(f"Retrying streaming server termination in {backoff} seconds...")
        time.sleep(backoff)
    await streaming_job.wait(timeout=60)
    logger.info("Streaming job success!")

    verify_http_output_volumes = all(
        var in os.environ for var in ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"]
    )

    # TODO: There should not be a need to sleep here. 
    # Figure out why the job fails to start immediately after the 
    # previous job is finished.
    await asyncio.sleep(15)

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

    if verify_http_output_volumes:
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

    if verify_http_output_volumes:
        post_object_path = f"/output/{post_object_key}"
        put_object_path = f"/output/{put_object_key}"
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
