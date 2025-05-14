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

async def test_buffered_job():
    """
    Test flow from master branch: buffered job, S3 upload, etc.
    """
    verify_http_output_volumes = all(
        var in os.environ for var in ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"]
    )

    # Set up the default job specification.
    compute_horde_job_spec = ComputeHordeJobSpec(
        executor_class=ExecutorClass.always_on__llm__a6000,
        job_namespace="SN123.0",
        docker_image="alpine",
        args=["sh", "-c", "echo 'Hello, World!' > /artifacts/stuff"],
        artifacts_dir="/artifacts",
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

    logger.info("Buffered job test: Success!")

async def test_streaming_job():
    """
    Test flow for streaming job: streaming, mTLS, direct HTTP server.
    """
    job = await compute_horde_client.create_job(
        ComputeHordeJobSpec(
            executor_class=ExecutorClass.always_on__llm__a6000,
            job_namespace="SN123.0",
            docker_image="alpine",
            args=["sh", "-c",
                  "while true; do echo -e 'HTTP/1.1 200 OK\r\nContent-Length: 13\r\n\r\nHello, World!' | nc -l -p 8000; done & sleep 150"],
            artifacts_dir="/artifacts",
            streaming=True,
        )
    )

    await job.wait_for_streaming(timeout=10 * 60)

    print(job.streaming_public_cert, job.streaming_private_key, job.streaming_server_address,
          job.streaming_server_port, job.streaming_server_cert)

    print(1)

    import httpx
    import ssl
    from cryptography.hazmat.primitives.serialization import Encoding, PrivateFormat, NoEncryption
    from cryptography.hazmat.primitives.serialization import pkcs12
    from cryptography.x509 import Certificate
    from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey
    from cryptography.hazmat.primitives.serialization import load_pem_private_key
    from cryptography.x509 import load_pem_x509_certificate
    from io import BytesIO

    def create_mt_client_in_memory(
            client_cert: Certificate,
            client_private_key: RSAPrivateKey,
            server_cert_str: str,
    ) -> httpx.Client:
        """
        Create an HTTPX client with mTLS using the client certificates and key without writing to disk.

        :param client_cert: A `cryptography.x509.Certificate` object representing the client certificate.
        :param client_private_key: A `cryptography.hazmat.primitives.asymmetric.rsa.RSAPrivateKey` object for the client private key.
        :param server_cert_str: Server certificate (CA certificate) as a string.
        :return: An instance of `httpx.Client` configured for mutual TLS.
        """
        # Combine client certificate and key into a PKCS12 binary object (in-memory)
        p12_data = pkcs12.serialize_key_and_certificates(
            name=b"mtls-client",
            key=client_private_key,
            cert=client_cert,
            cas=None,  # Additional chain certificates can go here
            encryption_algorithm=NoEncryption(),  # No password protection
        )

        # Create an in-memory buffer for the client certificate and private key from PKCS12
        p12_buffer = BytesIO(p12_data)

        # Create an in-memory buffer for the server CA certificate
        server_cert_buffer = BytesIO(server_cert_str.encode("utf-8"))

        # Configure SSL context for mTLS
        ssl_context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
        ssl_context.load_cert_chain(certfile=p12_buffer, password=None)
        ssl_context.load_verify_locations(cadata=server_cert_buffer.read().decode("utf-8"))
        ssl_context.verify_mode = ssl.CERT_REQUIRED  # Enforce server certificate validation

        # Create and return the HTTPX client
        client = httpx.Client(transport=httpx.HTTPTransport(ssl_context=ssl_context))
        return client

    logger.info("Streaming job test: Success!")

async def main():
    import sys
    test_type = os.environ.get("HORDE_TEST_TYPE")
    if len(sys.argv) > 1:
        test_type = sys.argv[1]
    if test_type == "streaming":
        await test_streaming_job()
    else:
        await test_buffered_job()

if __name__ == "__main__":
    asyncio.run(main())
