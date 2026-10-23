FROM docker:29.9.2-cli@sha256:5e6baf67325a79f64ed3bed6863954d5e05d0f3d4e57cbdb14809346814bbfdb
WORKDIR /root/validator
COPY data/docker-compose.yml docker-compose.yml
COPY entrypoint.sh /entrypoint.sh

ARG VALIDATOR_RUNNER_VERSION=notset
ENV VALIDATOR_RUNNER_VERSION=$VALIDATOR_RUNNER_VERSION

ARG VALIDATOR_IMAGE_REPO=compute-horde-validator
ENV VALIDATOR_IMAGE_REPO=$VALIDATOR_IMAGE_REPO

RUN chmod u+x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
