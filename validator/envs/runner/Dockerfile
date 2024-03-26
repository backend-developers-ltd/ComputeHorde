FROM docker:26-cli
WORKDIR /root/validator
COPY data/docker-compose.yml docker-compose.yml
COPY entrypoint.sh /entrypoint.sh
RUN chmod u+x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
