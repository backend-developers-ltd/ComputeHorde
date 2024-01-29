FROM ubuntu:22.04 AS base
FROM base as job
RUN apt-get update && apt-get install -y hashcat hashcat-data nvidia-cuda-toolkit
ENTRYPOINT ["hashcat"]