import os
import subprocess

import pytest
import requests
from cryptography import x509
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey

from compute_horde.certificate import (
    generate_certificate,
    generate_certificate_at,
    start_nginx_with_certificates,
    write_certificate,
)

NGINX_CONF = """
http {
    server {
        listen 443 ssl;
        server_name example.com;

        ssl_certificate /etc/nginx/ssl/certificate.pem;
        ssl_certificate_key /etc/nginx/ssl/private_key.pem;

        ssl_client_certificate /etc/nginx/ssl/client.crt;
        ssl_verify_client on;

        location / {
            if ($ssl_client_verify != SUCCESS) { return 403; }
            return 200 'Hello World!';
            add_header Content-Type text/plain;
        }
    }
}

events {
    worker_connections 1024;
}
"""


def test_generate_ca_and_server_certs(tmp_path):
    generate_certificate_at(tmp_path, "blabla")

    assert set(os.listdir(tmp_path)) == {"certificate.pem", "private_key.pem"}

    # check the cert is valid
    x509.load_pem_x509_certificate((tmp_path / "certificate.pem").read_bytes())

    # check the public key is valid
    key = serialization.load_pem_private_key((tmp_path / "private_key.pem").read_bytes(), None)
    assert isinstance(key, RSAPrivateKey)


def make_cert(tmp_path):
    client_tmp_path = tmp_path / "client"
    client_tmp_path.mkdir()

    generate_certificate_at(client_tmp_path, "blabla")
    public_key = (client_tmp_path / "certificate.pem").read_bytes()

    return public_key, (
        str(client_tmp_path / "certificate.pem"),
        str(client_tmp_path / "private_key.pem"),
    )


def test_certificates_with_nginx__success(tmp_path, docker_name_prefix):
    public_key, cert = make_cert(tmp_path)
    container_name, certs_dir = start_nginx_with_certificates(
        NGINX_CONF, public_key, docker_name_prefix, tmp_path
    )

    resp = requests.get(
        "https://localhost:8443", verify=str(certs_dir / "certificate.pem"), cert=cert
    )
    assert resp.status_code == 200
    assert resp.text.strip() == "Hello World!"

    subprocess.run(["docker", "kill", container_name])


def test_certificates_with_nginx__no_cert(tmp_path, docker_name_prefix):
    public_key, _ = make_cert(tmp_path)
    container_name, certs_dir = start_nginx_with_certificates(
        NGINX_CONF, public_key, docker_name_prefix, tmp_path
    )

    resp = requests.get("https://localhost:8443", verify=str(certs_dir / "certificate.pem"))

    # 400 no cert, 403 wrong cert
    assert resp.status_code != 200

    subprocess.run(["docker", "kill", container_name])


def test_certificates_with_nginx__fail(tmp_path, docker_name_prefix):
    public_key, _ = make_cert(tmp_path)
    container_name, _ = start_nginx_with_certificates(
        NGINX_CONF, public_key, docker_name_prefix, tmp_path
    )

    cert_path = tmp_path / "wrong_certificate.pem"
    certificate, _ = generate_certificate("127.0.0.1")
    write_certificate(certificate, cert_path)

    with pytest.raises(requests.exceptions.SSLError):
        requests.get("https://localhost:8443", verify=str(cert_path))

    subprocess.run(["docker", "kill", container_name])
