import asyncio
import ipaddress
import logging
import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path

import aiohttp
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey
from cryptography.x509 import Certificate
from cryptography.x509.oid import NameOID

logger = logging.getLogger(__name__)


async def get_docker_container_ip(container_name: str) -> str:
    process = await asyncio.create_subprocess_exec(
        "docker",
        "inspect",
        container_name,
        "-f",
        "{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
        stdout=asyncio.subprocess.PIPE,
    )
    stdout, _ = await process.communicate()
    return stdout.decode("utf-8").strip()


async def check_endpoint(url, timeout) -> bool:
    """
    Pings endpoint every second until it returns 200 or timeout is reached.
    """
    async with aiohttp.ClientSession() as session:
        for _ in range(timeout):
            try:
                response = await session.get(url)
                if response.status == 200:
                    return True
            except aiohttp.ClientError as e:
                logger.debug(f"Failed to ping {url}: {e}")
            await asyncio.sleep(1)
    return False


async def start_nginx(
    nginx_conf: str,
    port: int,
    dir_path: Path,
    container_name: str = "job-nginx",
    timeout: int = 10,
):
    nginx_conf_file = dir_path / "nginx.conf"
    nginx_conf_file.write_text(nginx_conf)

    process = await asyncio.create_subprocess_exec(
        "docker",
        "run",
        "--detach",
        "--rm",
        "--name",
        container_name,
        "-p",
        f"{port}:443",
        "-v",
        f"{dir_path}:/etc/nginx/",
        "nginx:1.26-alpine",
    )
    _stdout, _stderr = await process.communicate()
    await process.wait()

    # wait for nginx to start
    ip = await get_docker_container_ip(container_name)
    url = f"http://{ip}/ok"
    nginx_started = await check_endpoint(url, timeout)
    if not nginx_started:
        stdout = _stdout.decode() if _stdout else ""
        stderr = _stderr.decode() if _stderr else ""
        raise Exception(f"Failed to ping nginx on {url} - server init {stdout=}, {stderr=}")


def generate_certificate(alternative_name: str) -> tuple[Certificate, RSAPrivateKey]:
    """Generate a certificate and private key"""
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)

    builder = x509.CertificateBuilder()
    builder = builder.subject_name(
        x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "localhost")])
    )
    builder = builder.issuer_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "localhost")]))
    builder = builder.not_valid_before(datetime.now(UTC))
    builder = builder.not_valid_after(datetime.now(UTC) + timedelta(days=365))
    builder = builder.serial_number(x509.random_serial_number())
    builder = builder.public_key(private_key.public_key())
    builder = builder.add_extension(
        x509.BasicConstraints(ca=False, path_length=None), critical=True
    )

    alt_name: x509.GeneralName
    try:
        _ip = ipaddress.ip_address(alternative_name)
    except ValueError:
        alt_name = x509.DNSName(alternative_name)
    else:
        alt_name = x509.IPAddress(_ip)

    builder = builder.add_extension(
        x509.SubjectAlternativeName([x509.DNSName("localhost"), alt_name]), critical=False
    )

    certificate = builder.sign(private_key=private_key, algorithm=hashes.SHA256())

    return certificate, private_key


def serialize_certificate(certificate: Certificate) -> bytes:
    return certificate.public_bytes(encoding=serialization.Encoding.PEM)


def serialize_private_key(private_key: RSAPrivateKey) -> bytes:
    return private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def write_certificate(certificate: Certificate, path: Path) -> None:
    path.write_bytes(serialize_certificate(certificate))


def write_private_key(private_key: RSAPrivateKey, path: Path) -> None:
    path.write_bytes(serialize_private_key(private_key))


def save_public_key(public_key: str, dir_path: Path) -> None:
    certs_dir = dir_path / "ssl"
    client_cert_file = certs_dir / "client.crt"
    client_cert_file.write_text(public_key)


def generate_certificate_at(
    dir_path: Path | None = None, alternative_name: str = "127.0.0.1"
) -> tuple[Path, str, tuple[str, str]]:
    """
    Generate a certificate and private key and save them to a directory.
    Returns the directory path, the public key and the paths to the public and private key files.
    """

    if dir_path is None:
        dir_path = Path(tempfile.mkdtemp())
    certs_dir = dir_path / "ssl"
    certs_dir.mkdir()

    certificate, private_key = generate_certificate(alternative_name)
    public_key_path = certs_dir / "certificate.pem"
    private_key_path = certs_dir / "private_key.pem"
    write_certificate(certificate, public_key_path)
    write_private_key(private_key, private_key_path)

    public_key = serialize_certificate(certificate).decode("utf-8")
    return dir_path, public_key, (str(public_key_path), str(private_key_path))


def read_certificate(cert_path: Path) -> str | None:
    try:
        return cert_path.read_bytes().decode("utf-8")
    except Exception as e:
        logger.error(f"Failed to read executor certificate at {cert_path}: {e}")
        return None
