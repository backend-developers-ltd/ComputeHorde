FROM alpine:latest
RUN apk add --no-cache git bash

COPY dct_pull.sh /
RUN chmod +x /dct_pull.sh

ENTRYPOINT "/dct_pull.sh"
