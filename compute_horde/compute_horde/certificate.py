import ipaddress
from datetime import UTC, datetime, timedelta
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey
from cryptography.x509 import Certificate
from cryptography.x509.oid import NameOID

CA_COMMON_NAME = "Custom CA"
COMMON_NAME = "dummy.computehorde.io"
COUNTRY_NAME = "US"
ORGANIZATION_NAME = "ComputeHorde"


def generate_certificate_authority() -> tuple[Certificate, RSAPrivateKey]:
    # Generate CA private key
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)

    # Generate CA certificate
    builder = x509.CertificateBuilder()
    builder = builder.subject_name(
        x509.Name(
            [
                x509.NameAttribute(NameOID.COMMON_NAME, CA_COMMON_NAME),
                x509.NameAttribute(NameOID.ORGANIZATION_NAME, ORGANIZATION_NAME),
                x509.NameAttribute(NameOID.COUNTRY_NAME, COUNTRY_NAME),
            ]
        )
    )
    builder = builder.issuer_name(
        x509.Name(
            [
                x509.NameAttribute(NameOID.COMMON_NAME, CA_COMMON_NAME),
                x509.NameAttribute(NameOID.ORGANIZATION_NAME, ORGANIZATION_NAME),
                x509.NameAttribute(NameOID.COUNTRY_NAME, COUNTRY_NAME),
            ]
        )
    )
    builder = builder.not_valid_before(datetime.now(UTC))
    builder = builder.not_valid_after(datetime.now(UTC) + timedelta(days=365))
    builder = builder.serial_number(x509.random_serial_number())
    builder = builder.public_key(private_key.public_key())
    builder = builder.add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)

    # Sign CA certificate with its own private key
    certificate = builder.sign(private_key=private_key, algorithm=hashes.SHA256())

    return certificate, private_key


def generate_certificate(
    certificate_authority: tuple[Certificate, RSAPrivateKey] | None = None,
    public_ip: str | None = None,
) -> tuple[Certificate, RSAPrivateKey]:
    """
    Generate a certificate and public key.

    :param certificate_authority: if given, the CA will be used to generate the new certificate.
    :param public_ip: if given, adds the ip as an alternative name to the certificate
    :return:
    """
    if certificate_authority is None:
        ca_cert, ca_key = generate_certificate_authority()
    else:
        ca_cert, ca_key = certificate_authority

    # Generate private key
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)

    # Generate certificate
    builder = x509.CertificateBuilder()
    builder = builder.subject_name(
        x509.Name(
            [
                x509.NameAttribute(NameOID.COMMON_NAME, COMMON_NAME),
                x509.NameAttribute(NameOID.ORGANIZATION_NAME, ORGANIZATION_NAME),
                x509.NameAttribute(NameOID.COUNTRY_NAME, COUNTRY_NAME),
            ]
        )
    )
    builder = builder.issuer_name(ca_cert.subject)
    builder = builder.not_valid_before(datetime.now(UTC))
    builder = builder.not_valid_after(datetime.now(UTC) + timedelta(days=365))
    builder = builder.serial_number(x509.random_serial_number())
    builder = builder.public_key(private_key.public_key())

    # Add extensions
    builder = builder.add_extension(
        x509.BasicConstraints(ca=False, path_length=None), critical=True
    )
    alternative_names = [
        x509.DNSName(COMMON_NAME),
        x509.DNSName("localhost"),
        x509.IPAddress(ipaddress.ip_address("127.0.0.1")),
    ]
    if public_ip:
        alternative_names.append(x509.IPAddress(ipaddress.ip_address(public_ip)))
    builder = builder.add_extension(x509.SubjectAlternativeName(alternative_names), critical=False)

    # Sign certificate with CA private key
    certificate = builder.sign(private_key=ca_key, algorithm=hashes.SHA256())

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


def generate_ca_and_server_certs(dir_path: Path, public_ip: str | None = None) -> None:
    """
    Generates certificate authority and server certificates.

    :param dir_path: directory where the generated certificates and public keys will be saved
    :param public_ip: if given, adds the ip as an alternative name to the server certificate
    :return:
    """
    assert dir_path.is_dir()

    ca_cert, ca_key = generate_certificate_authority()

    server_cert, server_key = generate_certificate(
        certificate_authority=(ca_cert, ca_key),
        public_ip=public_ip,
    )

    # Save the certificates and keys
    write_certificate(ca_cert, dir_path / "ca.crt")
    write_private_key(ca_key, dir_path / "ca.key")

    write_certificate(server_cert, dir_path / "server.crt")
    write_private_key(server_key, dir_path / "server.key")
