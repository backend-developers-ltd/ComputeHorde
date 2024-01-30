FROM ubuntu:22.04
EXPOSE 80
RUN apt-get update && apt-get install -y docker.io docker-compose
WORKDIR /root
COPY data/nginx nginx
COPY data/docker-compose.yml docker-compose.yml
ENTRYPOINT ["docker-compose", "up"]