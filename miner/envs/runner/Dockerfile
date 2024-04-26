FROM docker:26-cli
WORKDIR /root/miner
COPY data/docker-compose.yml docker-compose.yml
COPY entrypoint.sh /entrypoint.sh

ARG MINER_RUNNER_VERSION=notset
ENV MINER_RUNNER_VERSION=$MINER_RUNNER_VERSION

ARG MINER_IMAGE_REPO=compute-horde-miner
ENV MINER_IMAGE_REPO=$MINER_IMAGE_REPO

RUN chmod u+x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
