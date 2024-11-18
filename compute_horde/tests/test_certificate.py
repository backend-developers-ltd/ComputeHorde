import os

from cryptography import x509
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey

from compute_horde.certificate import generate_certificate_at


def test_generate_ca_and_server_certs(tmp_path):
    generate_certificate_at(tmp_path, "blabla")

    assert set(os.listdir(tmp_path)) == {"certificate.pem", "private_key.pem"}

    # check the cert is valid
    x509.load_pem_x509_certificate((tmp_path / "certificate.pem").read_bytes())

    # check the public key is valid
    key = serialization.load_pem_private_key((tmp_path / "private_key.pem").read_bytes(), None)
    assert isinstance(key, RSAPrivateKey)
