FROM ubuntu:22.04
EXPOSE 80
RUN apt-get update && apt-get install -y docker.io docker-compose
WORKDIR /root
COPY data/docker-compose.yml docker-compose.yml
COPY entrypoint.sh /entrypoint.sh
RUN chmod u+x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]