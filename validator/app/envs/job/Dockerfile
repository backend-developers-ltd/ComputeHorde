FROM ubuntu:22.04 AS base
FROM base as job
RUN apt-get update && apt-get install -y hashcat hashcat-data nvidia-cuda-toolkit
ENTRYPOINT ["hashcat"]
CMD [ \
    "--runtime", "600", \
    "--restore-disable", \
    "--attack-mode", "3", \
    "--workload-profile", "3", \
    "--optimized-kernel-enable", \
    "--hash-type", "1410", \
    "--hex-salt", \
    "-1", "?l?d?u", \
    "--quiet", \
    "--outfile", "/volume/output.txt", \
    "--outfile-format", "2", \
    "/volume/payload.txt", \
    "?1?1?1?1?1?1" \
]