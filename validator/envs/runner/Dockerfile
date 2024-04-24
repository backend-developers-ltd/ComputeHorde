FROM docker:26-cli
WORKDIR /root/validator
COPY data/docker-compose.yml docker-compose.yml
COPY entrypoint.sh /entrypoint.sh

ARG VALIDATOR_RUNNER_VERSION=notset
ENV VALIDATOR_RUNNER_VERSION=$VALIDATOR_RUNNER_VERSION

ARG VALIDATOR_IMAGE_REPO=compute-horde-validator
ENV VALIDATOR_IMAGE_REPO=$VALIDATOR_IMAGE_REPO

RUN chmod u+x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
