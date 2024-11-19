import os
import subprocess
import time
import uuid

import pytest
import requests
from cryptography import x509
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey

from compute_horde.certificate import (
    generate_certificate,
    generate_certificate_at,
    write_certificate,
)


def test_generate_ca_and_server_certs(tmp_path):
    generate_certificate_at(tmp_path, "blabla")

    assert set(os.listdir(tmp_path)) == {"certificate.pem", "private_key.pem"}

    # check the cert is valid
    x509.load_pem_x509_certificate((tmp_path / "certificate.pem").read_bytes())

    # check the public key is valid
    key = serialization.load_pem_private_key((tmp_path / "private_key.pem").read_bytes(), None)
    assert isinstance(key, RSAPrivateKey)


NGINX_CONF = """
http {
    server {
        listen 443 ssl;
        server_name example.com;

        ssl_certificate /etc/nginx/ssl/certificate.pem;
        ssl_certificate_key /etc/nginx/ssl/private_key.pem;

        location / {
            return 200 'Hello World!';
            add_header Content-Type text/plain;
        }
    }
}

events {
    worker_connections 1024;
}
"""


def certificates_with_nginx_helper(tmp_path, docker_name_prefix):
    certs_dir = tmp_path / "ssl"
    certs_dir.mkdir()
    generate_certificate_at(certs_dir, "127.0.0.1")
    nginx_conf_file = tmp_path / "nginx.conf"
    nginx_conf_file.write_text(NGINX_CONF)

    container_name = f"{docker_name_prefix}_{uuid.uuid4()}"
    subprocess.run(
        [
            "docker",
            "run",
            "--detach",
            "--rm",
            "--name",
            container_name,
            "-p",
            "8080:80",
            "-p",
            "8443:443",
            "-v",
            f"{nginx_conf_file}:/etc/nginx/nginx.conf",
            "-v",
            f"{certs_dir}:/etc/nginx/ssl",
            "nginx:1.26-alpine",
        ]
    )

    # wait for nginx to start
    time.sleep(1)

    return container_name, certs_dir


def test_certificates_with_nginx_success(tmp_path, docker_name_prefix):
    container_name, certs_dir = certificates_with_nginx_helper(tmp_path, docker_name_prefix)

    resp = requests.get("https://localhost:8443", verify=str(certs_dir / "certificate.pem"))
    assert resp.status_code == 200
    assert resp.text.strip() == "Hello World!"

    subprocess.run(["docker", "kill", container_name])


def test_certificates_with_nginx_fail(tmp_path, docker_name_prefix):
    container_name, _ = certificates_with_nginx_helper(tmp_path, docker_name_prefix)

    cert_path = tmp_path / "wrong_certificate.pem"
    certificate, _ = generate_certificate("127.0.0.1")
    write_certificate(certificate, cert_path)

    with pytest.raises(requests.exceptions.SSLError):
        requests.get("https://localhost:8443", verify=str(cert_path))

    subprocess.run(["docker", "kill", container_name])
