import ipaddress
import logging
import pathlib
import subprocess
import tempfile
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey
from cryptography.x509 import Certificate
from cryptography.x509.oid import NameOID

logger = logging.getLogger(__name__)


def start_nginx_with_certificates(
    nginx_conf: str,
    public_key: bytes,
    port: int,
    container_name: str = "job-nginx",
    tmp_path: Path | None = None,
):
    if tmp_path is None:
        tmp_path = Path(tempfile.mkdtemp())
    certs_dir = tmp_path / "ssl"
    certs_dir.mkdir()

    generate_certificate_at(certs_dir, "127.0.0.1")

    nginx_conf_file = tmp_path / "nginx.conf"
    nginx_conf_file.write_text(nginx_conf)

    client_cert_file = certs_dir / "client.crt"
    client_cert_file.write_bytes(public_key)

    subprocess.run(
        [
            "docker",
            "run",
            "--detach",
            "--rm",
            "--name",
            container_name,
            "-p",
            f"{port}:443",
            "-v",
            f"{tmp_path}:/etc/nginx/",
            "nginx:1.26-alpine",
        ]
    )

    # wait for nginx to start
    time.sleep(1)

    return container_name, certs_dir


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


def generate_certificate_at(dir_path: Path, alternative_name: str) -> None:
    """Generates and saves a certificate and private key at `dir_path`"""
    assert dir_path.is_dir()

    certificate, private_key = generate_certificate(alternative_name)
    write_certificate(certificate, dir_path / "certificate.pem")
    write_private_key(private_key, dir_path / "private_key.pem")


def read_certificate(certs_dir: pathlib.Path) -> str | None:
    try:
        return Path(certs_dir / "certificate.pem").read_bytes().decode("utf-8")
    except Exception as e:
        logger.error(f"Failed to read executor certificate.pem: {e}")
        return None
