import logging
logging.basicConfig(level=logging.DEBUG)

import asyncio
import os
import tempfile
from cryptography.hazmat.primitives import serialization
import base64
import requests
import time
from compute_horde_sdk._internal.fallback.client import FallbackClient
from compute_horde_sdk._internal.fallback.job import FallbackJobSpec
from compute_horde_sdk._internal.models import InlineInputVolume
from cryptography import x509
from cryptography.x509.oid import NameOID
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from compute_horde_sdk._internal.sdk import ComputeHordeJobSpec
from compute_horde_sdk.v1 import ExecutorClass
logger = logging.getLogger(__name__)

def create_streaming_https_client(job, client):
    """
    Prepares a requests.Session configured for HTTPS with client cert, key, and server cert verification.
    Returns (session, cleanup_fn).
    """
    # Write client cert
    temp_client_cert = tempfile.NamedTemporaryFile(delete=False, suffix='.crt')
    if isinstance(job.client_cert, bytes):
        temp_client_cert.write(job.client_cert)
    else:
        temp_client_cert.write(job.client_cert.public_bytes(serialization.Encoding.PEM))
    temp_client_cert.close()
    logger.warning(f"Client certificate written to: {temp_client_cert.name}")
    with open(temp_client_cert.name, 'rb') as f:
        logger.warning(f"Client certificate contents:\n{f.read().decode(errors='replace')}")

    # Write client key
    temp_client_key = tempfile.NamedTemporaryFile(delete=False, suffix='.key')
    if isinstance(job.private_key, bytes):
        temp_client_key.write(job.private_key)
    else:
        temp_client_key.write(
            job.private_key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.TraditionalOpenSSL,
                encryption_algorithm=serialization.NoEncryption()
            )
        )
    temp_client_key.close()
    logger.warning(f"Client private key written to: {temp_client_key.name}")
    with open(temp_client_key.name, 'rb') as f:
        logger.warning(f"Client private key contents:\n{f.read().decode(errors='replace')}")

    # Download server cert
    print(f"[Fallback] Streaming server certificate: {job.streaming_server_certificate}")
    temp_server_cert = tempfile.NamedTemporaryFile(delete=False, suffix='.crt')
    temp_server_cert.write(job.streaming_server_certificate)
    temp_server_cert.close()
    logger.warning(f"Server certificate written to: {temp_server_cert.name}")
    with open(temp_server_cert.name, 'rb') as f:
        logger.warning(f"Server certificate contents:\n{f.read().decode(errors='replace')}")

    session = requests.Session()
    session.cert = (temp_client_cert.name, temp_client_key.name)
    session.verify = temp_server_cert.name
    logger.warning(f"Session cert: {session.cert}, verify: {session.verify}")

    def cleanup():
        os.unlink(temp_client_cert.name)
        os.unlink(temp_client_key.name)
        os.unlink(temp_server_cert.name)

    return session, cleanup

async def main():
    # Build ComputeHordeJobSpec first
    compute_horde_job_spec = ComputeHordeJobSpec(
        executor_class=ExecutorClass.always_on__llm__a6000,
        job_namespace="SN123.0",
        docker_image="python:3.11-slim",
        args=[
            # Write the FastAPI server to app.py and run it
            (
                "pip install --no-cache-dir fastapi uvicorn && "
                "echo 'from fastapi import FastAPI\n"
                "import os, signal, threading\n"
                "app = FastAPI()\n"
                "@app.get(\"/\")\n"
                "def root(): return {\"message\": \"Server is running.\"}\n"
                "@app.post(\"/terminate\")\n"
                "def terminate():\n"
                "    def shutdown(): os.kill(os.getpid(), signal.SIGINT)\n"
                "    threading.Thread(target=shutdown).start()\n"
                "    return {\"message\": \"Server is shutting down.\"}' > app.py && "
                "uvicorn app:app --host 127.0.0.1 --port 80"
            )
        ],
        output_volumes=None,
        artifacts_dir="/output",
        download_time_limit_sec=5,
        execution_time_limit_sec=10,
        upload_time_limit_sec=5,
        streaming=True,
        streaming_start_time_limit_sec=10,
    )

    # Use from_job_spec to create the fallback job spec
    fallback_job_spec = FallbackJobSpec.from_job_spec(
        compute_horde_job_spec, work_dir="/")

    cloud = "runpod"
    print(f"[Fallback] Submitting job to fallback cloud: {cloud}")
    client = FallbackClient(cloud=cloud)
    job = await client.create_job(fallback_job_spec)
    await job.wait_for_streaming(timeout=120)

    session, cleanup = create_streaming_https_client(job, client)
    ip = job.streaming_server_ip
    port = job.streaming_server_port
    url = f"https://{ip}:{port}/terminate"
    print(f"[Fallback] Attempting to terminate server at {url}")

    max_retries = 5
    for attempt in range(1, max_retries + 1):
        try:
            resp = session.post(url, timeout=5)
            print(f"[Fallback] Terminate response: {resp.status_code} {resp.text}")
            if resp.status_code == 200:
                break
        except Exception as e:
            print(f"[Fallback] Error calling /terminate (attempt {attempt}): {e}")
        backoff = 2 ** attempt
        print(f"[Fallback] Retrying in {backoff} seconds...")
        time.sleep(backoff)
    else:
        print(f"[Fallback] Failed to terminate server after {max_retries} attempts.")
    cleanup()

    await job.wait(timeout=60)
    print(f"[Fallback] Job status: {job.status}")
    print(f"[Fallback] Job output:\n{job.result.stdout}")
    if job.result.artifacts:
        print(f"[Fallback] Artifacts: {job.result.artifacts}")

if __name__ == "__main__":
    asyncio.run(main()) 