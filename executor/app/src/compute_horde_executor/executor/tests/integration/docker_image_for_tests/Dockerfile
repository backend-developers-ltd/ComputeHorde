FROM alpine

CMD cat /volume/payload.txt && \
    echo '4 // chosen by fair dice roll. guaranteed to be random :D' > /output/file1.txt && \
    echo '5 // chosen by fair dice roll. guaranteed to be random :D' > /output/file2.txt && \
    mkdir -p /artifacts && \
    touch /artifacts/empty.bin && \
    echo 'I am LLM, your AI assistant' > /artifacts/text.txt && \
    printf '%b' $(seq 0 255 | awk '{printf "\\x%02X", $1}') > /artifacts/All-BYTES.bin && \
    dd if=/dev/zero bs=1 count=100000 of='/artifacts/100k zeros' 2>/dev/null
