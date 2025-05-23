import logging
logging.basicConfig(level=logging.DEBUG)

import asyncio
import os
from compute_horde_sdk._internal.fallback.client import FallbackClient
from compute_horde_sdk._internal.fallback.job import FallbackJobSpec
from compute_horde_sdk._internal.models import InlineInputVolume
from cryptography import x509
from cryptography.x509.oid import NameOID
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
import datetime
from compute_horde_sdk._internal.sdk import ComputeHordeJobSpec
from compute_horde_sdk.v1 import ExecutorClass
logger = logging.getLogger(__name__)

def generate_self_signed_cert(cert_path="certificate.crt", key_path="private.key"):
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    with open(key_path, "wb") as f:
        f.write(key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption()
        ))
    subject = issuer = x509.Name([
        x509.NameAttribute(NameOID.COUNTRY_NAME, u"US"),
        x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, u"State"),
        x509.NameAttribute(NameOID.LOCALITY_NAME, u"City"),
        x509.NameAttribute(NameOID.ORGANIZATION_NAME, u"Organization"),
        x509.NameAttribute(NameOID.COMMON_NAME, u"localhost"),
    ])
    cert = x509.CertificateBuilder().subject_name(
        subject
    ).issuer_name(
        issuer
    ).public_key(
        key.public_key()
    ).serial_number(
        x509.random_serial_number()
    ).not_valid_before(
        datetime.datetime.utcnow()
    ).not_valid_after(
        datetime.datetime.utcnow() + datetime.timedelta(days=365)
    ).add_extension(
        x509.SubjectAlternativeName([x509.DNSName(u"localhost")]),
        critical=False,
    ).sign(key, hashes.SHA256())
    with open(cert_path, "wb") as f:
        f.write(cert.public_bytes(serialization.Encoding.PEM))
    logger.info(f"Generated {cert_path} and {key_path}")
    return cert_path, key_path

async def main():
    cert_path, key_path = generate_self_signed_cert()
    with open(cert_path, "rb") as f:
        cert_bytes = f.read()
    with open(key_path, "rb") as f:
        key_bytes = f.read()

    input_volumes = {
        "/volume/cert/": InlineInputVolume.from_file_contents(
            filename="certificate.crt",
            contents=cert_bytes
        ),
        "/volume/key": InlineInputVolume.from_file_contents(
            filename="private.key",
            contents=key_bytes
        ),
    }

    # Build ComputeHordeJobSpec first
    compute_horde_job_spec = ComputeHordeJobSpec(
        executor_class=ExecutorClass.always_on__llm__a6000,
        job_namespace="SN123.0",
        docker_image="python:3.11-slim",
        args=[
            (
                "mkdir -p /app && "
                "apt-get update && "
                "apt-get install -y nginx && "
                "rm -rf /var/lib/apt/lists/* && "
                "sed -i 's/listen 80 default_server;/listen 8443 default_server;/' /etc/nginx/sites-available/default && "
                "cp /volume/cert/certificate.crt /app/certificate.crt && "
                "cp /volume/key/private.key /app/private.key && "
                "pip install --no-cache-dir fastapi uvicorn && "
                "cd /app && "
                "nginx -g 'daemon off;'"
            )
        ],
        input_volumes=input_volumes,
        output_volumes=None,
        artifacts_dir="/output",
        download_time_limit_sec=5,
        execution_time_limit_sec=10,
        upload_time_limit_sec=5,
    )

    # Use from_job_spec to create the fallback job spec
    fallback_job_spec = FallbackJobSpec.from_job_spec(
        compute_horde_job_spec, ports=[8443], work_dir="/")

    cloud = "runpod"
    print(f"[Fallback] Submitting job to fallback cloud: {cloud}")
    client = FallbackClient(cloud=cloud)
    job = await client.create_job(fallback_job_spec)
    await job.wait(timeout=120)
    print(f"[Fallback] Job status: {job.status}")
    print(f"[Fallback] Job output:\n{job.result.stdout}")
    if job.result.artifacts:
        print(f"[Fallback] Artifacts: {job.result.artifacts}")

if __name__ == "__main__":
    asyncio.run(main()) 