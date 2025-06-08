import asyncio
import os
import tempfile
import requests
import time
from cryptography.hazmat.primitives import serialization
from compute_horde_sdk._internal.fallback.client import FallbackClient
from compute_horde_sdk._internal.fallback.job import FallbackJobSpec
from compute_horde_sdk._internal.sdk import ComputeHordeJobSpec
from compute_horde_sdk.v1 import ExecutorClass
from cryptography import x509
from cryptography.x509.oid import NameOID
from cryptography.hazmat.primitives import serialization
import logging
from typing import Tuple, Callable


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_streaming_https_client(job: 'FallbackJob') -> Tuple[requests.Session, Callable[[], None]]:
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

    temp_server_cert = tempfile.NamedTemporaryFile(delete=False, suffix='.crt')
    temp_server_cert.write(job.streaming_server_certificate)
    temp_server_cert.close()

    session = requests.Session()
    session.cert = (temp_client_cert.name, temp_client_key.name)
    session.verify = temp_server_cert.name

    def cleanup():
        os.unlink(temp_client_cert.name)
        os.unlink(temp_client_key.name)
        os.unlink(temp_server_cert.name)

    return session, cleanup

async def main():
    compute_horde_job_spec = ComputeHordeJobSpec(
        executor_class=ExecutorClass.spin_up_4min__gpu_24gb,
        job_namespace="SN123.0",
        docker_image="python:3.11-slim",
        args=[
            # Write the FastAPI server to app.py and run it
            (
                "env &&"
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
                "uvicorn app:app --host 127.0.0.1 --port 8081"
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
        compute_horde_job_spec, 
        work_dir="/",
        proxy_pass_port=8081
    )

    client = FallbackClient(cloud="runpod", idle_minutes=1)
    job = await client.create_job(fallback_job_spec)
    await job.wait_for_streaming(timeout=120)

    session, cleanup = create_streaming_https_client(job)
    ip = job.streaming_server_ip
    port = job.streaming_server_port
    url = f"https://{ip}:{port}/terminate"
    logger.info(f"[Fallback] Attempting to terminate server at {url}")

    max_retries = 5
    for attempt in range(1, max_retries + 1):
        try:
            resp = session.post(url, timeout=5)
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
    logger.info("Success!")

if __name__ == "__main__":
    asyncio.run(main())
