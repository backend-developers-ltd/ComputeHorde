FROM docker:29.9.2-cli@sha256:5e6baf67325a79f64ed3bed6863954d5e05d0f3d4e57cbdb14809346814bbfdb
WORKDIR /root
COPY data/docker-compose.yml docker-compose.yml
COPY entrypoint.sh /entrypoint.sh

ARG MINER_RUNNER_VERSION=notset
ENV MINER_RUNNER_VERSION=$MINER_RUNNER_VERSION

ARG MINER_IMAGE_REPO=compute-horde-miner
ENV MINER_IMAGE_REPO=$MINER_IMAGE_REPO

ARG MINER_NGINX_IMAGE_REPO=compute-horde-miner-nginx
ENV MINER_NGINX_IMAGE_REPO=$MINER_NGINX_IMAGE_REPO

RUN chmod u+x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
