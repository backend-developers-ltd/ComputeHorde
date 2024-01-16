#!/bin/sh -eux
# Copyright 2020, Reef Technologies (reef.pl), All rights reserved.

DOCKER_BIN="$(command -v docker || true)"
DOCKER_COMPOSE_BIN="$(command -v docker-compose || true)"
SENTRY_CLI="$(command -v sentry-cli || true)"
B2_CLI="$(command -v b2 || true)"
AWS_CLI="$(command -v aws || true)"
JQ_BIN="$(command -v jq || true)"

if [ -x "${DOCKER_BIN}" ] && [ -x "${DOCKER_COMPOSE_BIN}" ] && [ -x "${SENTRY_CLI}" ] && [ -x "${B2_CLI}" ] && [ -x "${AWS_CLI}" ] && [ -x "${JQ_BIN}" ]; then
    echo "\e[31mEverything required is already installed!\e[0m";
    exit 0;
fi

PLATFORM="$(uname -i)"
if [ "${PLATFORM}" != "x86_64" ] && [ "${PLATFORM}" != "aarch64" ]; then
  echo "Unsupported hardware platform: ${PLATFORM}"
  exit 1
fi

WORK_DIR="$(mktemp -d)"
if [ ! "${WORK_DIR}" ] || [ ! -d "${WORK_DIR}" ]; then
  echo "Could not create temp dir"
  exit 1
fi
cd "${WORK_DIR}"
cleanup() {
  rm -rf "${WORK_DIR}"
}
trap cleanup EXIT

DEBIAN_FRONTEND=noninteractive

apt-get update
apt-get install -y apt-transport-https ca-certificates curl software-properties-common python3-pip rng-tools

if [ ! -x "${SENTRY_CLI}" ]; then
  curl -sL https://sentry.io/get-cli/ | bash
fi

if [ ! -x "${B2_CLI}" ]; then
  curl -s --output /usr/local/bin/b2 -L https://github.com/Backblaze/B2_Command_Line_Tool/releases/latest/download/b2-linux
  chmod a+x /usr/local/bin/b2
fi

if [ ! -x "${DOCKER_BIN}" ]; then
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | apt-key add -
    add-apt-repository -y "deb [arch=amd64] https://download.docker.com/linux/ubuntu focal stable"
    apt-get -y install docker-ce
    usermod -aG docker "$USER"
fi

if [ ! -x "${DOCKER_COMPOSE_BIN}" ]; then
    apt-get -y install docker-ce docker-compose
fi

if [ ! -x "${AWS_CLI}" ]; then
  apt-get -y install gpg unzip
  curl "https://awscli.amazonaws.com/awscli-exe-linux-${PLATFORM}.zip" -o "awscliv2.zip"
  curl "https://awscli.amazonaws.com/awscli-exe-linux-${PLATFORM}.zip.sig" -o "awscliv2.sig"
  gpg --import <<EOF
-----BEGIN PGP PUBLIC KEY BLOCK-----

mQINBF2Cr7UBEADJZHcgusOJl7ENSyumXh85z0TRV0xJorM2B/JL0kHOyigQluUG
ZMLhENaG0bYatdrKP+3H91lvK050pXwnO/R7fB/FSTouki4ciIx5OuLlnJZIxSzx
PqGl0mkxImLNbGWoi6Lto0LYxqHN2iQtzlwTVmq9733zd3XfcXrZ3+LblHAgEt5G
TfNxEKJ8soPLyWmwDH6HWCnjZ/aIQRBTIQ05uVeEoYxSh6wOai7ss/KveoSNBbYz
gbdzoqI2Y8cgH2nbfgp3DSasaLZEdCSsIsK1u05CinE7k2qZ7KgKAUIcT/cR/grk
C6VwsnDU0OUCideXcQ8WeHutqvgZH1JgKDbznoIzeQHJD238GEu+eKhRHcz8/jeG
94zkcgJOz3KbZGYMiTh277Fvj9zzvZsbMBCedV1BTg3TqgvdX4bdkhf5cH+7NtWO
lrFj6UwAsGukBTAOxC0l/dnSmZhJ7Z1KmEWilro/gOrjtOxqRQutlIqG22TaqoPG
fYVN+en3Zwbt97kcgZDwqbuykNt64oZWc4XKCa3mprEGC3IbJTBFqglXmZ7l9ywG
EEUJYOlb2XrSuPWml39beWdKM8kzr1OjnlOm6+lpTRCBfo0wa9F8YZRhHPAkwKkX
XDeOGpWRj4ohOx0d2GWkyV5xyN14p2tQOCdOODmz80yUTgRpPVQUtOEhXQARAQAB
tCFBV1MgQ0xJIFRlYW0gPGF3cy1jbGlAYW1hem9uLmNvbT6JAlQEEwEIAD4WIQT7
Xbd/1cEYuAURraimMQrMRnJHXAUCXYKvtQIbAwUJB4TOAAULCQgHAgYVCgkICwIE
FgIDAQIeAQIXgAAKCRCmMQrMRnJHXJIXEAChLUIkg80uPUkGjE3jejvQSA1aWuAM
yzy6fdpdlRUz6M6nmsUhOExjVIvibEJpzK5mhuSZ4lb0vJ2ZUPgCv4zs2nBd7BGJ
MxKiWgBReGvTdqZ0SzyYH4PYCJSE732x/Fw9hfnh1dMTXNcrQXzwOmmFNNegG0Ox
au+VnpcR5Kz3smiTrIwZbRudo1ijhCYPQ7t5CMp9kjC6bObvy1hSIg2xNbMAN/Do
ikebAl36uA6Y/Uczjj3GxZW4ZWeFirMidKbtqvUz2y0UFszobjiBSqZZHCreC34B
hw9bFNpuWC/0SrXgohdsc6vK50pDGdV5kM2qo9tMQ/izsAwTh/d/GzZv8H4lV9eO
tEis+EpR497PaxKKh9tJf0N6Q1YLRHof5xePZtOIlS3gfvsH5hXA3HJ9yIxb8T0H
QYmVr3aIUes20i6meI3fuV36VFupwfrTKaL7VXnsrK2fq5cRvyJLNzXucg0WAjPF
RrAGLzY7nP1xeg1a0aeP+pdsqjqlPJom8OCWc1+6DWbg0jsC74WoesAqgBItODMB
rsal1y/q+bPzpsnWjzHV8+1/EtZmSc8ZUGSJOPkfC7hObnfkl18h+1QtKTjZme4d
H17gsBJr+opwJw/Zio2LMjQBOqlm3K1A4zFTh7wBC7He6KPQea1p2XAMgtvATtNe
YLZATHZKTJyiqA==
=vYOk
-----END PGP PUBLIC KEY BLOCK-----
EOF
  gpg --verify awscliv2.sig awscliv2.zip
  unzip awscliv2.zip
  ./aws/install
fi

if [ ! -x "${JQ_BIN}" ]; then
  apt-get -y install jq
fi
