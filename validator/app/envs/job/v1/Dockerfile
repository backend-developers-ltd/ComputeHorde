FROM ubuntu:24.04 AS base
FROM base as job
RUN apt-get update && apt-get install --no-install-recommends -y hashcat hashcat-data nvidia-cuda-toolkit python3 python3-cryptography time
ENTRYPOINT ["/usr/bin/python3"]
