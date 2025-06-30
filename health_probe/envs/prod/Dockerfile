ARG BASE_IMAGE=python:3.11-slim


FROM $BASE_IMAGE AS base-image
LABEL builder=true

RUN apt-get update && apt-get install -y git
COPY --from=ghcr.io/astral-sh/uv:0.5 /uv /uvx /bin/

COPY --from=compute-horde-sdk . /root/compute_horde_sdk

WORKDIR /root/src/
COPY pyproject.toml uv.lock main.py ./

# Enable bytecode compilation
ENV UV_COMPILE_BYTECODE=1
# Copy from the cache instead of linking since it's a mounted volume
ENV UV_LINK_MODE=copy

RUN --mount=type=cache,target=/root/.cache/uv \
	PDM_BUILD_SCM_VERSION=0 uv sync --frozen --no-install-project --no-dev --no-editable

COPY ./health_probe /root/src/health_probe

RUN --mount=type=cache,target=/root/.cache/uv \
	uv sync --frozen --no-dev --no-editable


FROM $BASE_IMAGE AS secondary-image
LABEL builder=false

RUN apt-get update \
	&& apt-get install -y wget \
	&& rm -rf /var/lib/apt/lists/*

WORKDIR /root/src/
ENV PYTHONUNBUFFERED=1
ENV PATH="/root/src/.venv/bin:$PATH"

COPY --from=base-image /root/src/ /root/src/

ENTRYPOINT ["python", "main.py"]
