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
            #args=["sh", "-c", "echo 'Hello, World!' > /artifacts/stuff"],
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

    assert (job.status, job.result.artifacts) == ("Completed", {'/artifacts/stuff': b'Hello, World!\n'}), \
        f"{job.status=}, {job.result.artifacts=}"

    print("Success!")


asyncio.run(main())
