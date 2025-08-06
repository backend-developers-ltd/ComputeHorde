FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    VOLUME_MANAGER_CACHE_DIR=/tmp/volume_manager/cache

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install uv
RUN curl -LsSf https://astral.sh/uv/install.sh | sh && \
    mv /root/.local/bin/uv /usr/local/bin/uv && \
    rm -rf /root/.local

# Set up volume manager cache directory with proper permissions
RUN mkdir -p /tmp/volume_manager/cache && \
    chmod -R 777 /tmp/volume_manager

COPY compute_horde_sdk /app/compute_horde_sdk
COPY examples/volume_manager /app/volume_manager

WORKDIR /app/volume_manager

RUN uv sync

EXPOSE 8001

CMD ["uv", "run", "volume_manager"] 