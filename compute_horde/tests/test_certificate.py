import os

from cryptography import x509
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey

from compute_horde.certificate import generate_ca_and_server_certs


def test_generate_ca_and_server_certs(tmp_path):
    generate_ca_and_server_certs(tmp_path)

    cert_files = ["ca.crt", "server.crt"]
    key_files = ["ca.key", "server.key"]
    expected_files = cert_files + key_files

    assert set(os.listdir(tmp_path)) == set(expected_files)

    # check certs are valid
    for cert_file in cert_files:
        x509.load_pem_x509_certificate((tmp_path / cert_file).read_bytes())

    # check public keys are valid
    for key_file in key_files:
        key = serialization.load_pem_private_key((tmp_path / key_file).read_bytes(), None)
        assert isinstance(key, RSAPrivateKey)
